from __future__ import annotations

import json
import hashlib
import errno
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

REQUIRED_FILES = ["data.jsonl", "metadata.json", "run.log"]

# macOS Docker bind-mounts can intermittently raise EDEADLK (Errno 35)
_RETRY_ERRNOS = {getattr(errno, "EDEADLK", 35), 35}


def _with_retries(
    func,
    *,
    attempts: int = 10,
    base_sleep: float = 0.05,
    max_sleep: float = 0.5,
    max_elapsed: float = 5.0,
):
    last_exc: Exception | None = None
    start = time.monotonic()
    for i in range(attempts):
        try:
            return func()
        except OSError as e:
            last_exc = e
            if getattr(e, "errno", None) in _RETRY_ERRNOS:
                if time.monotonic() - start >= max_elapsed:
                    break
                time.sleep(min(base_sleep * (2**i), max_sleep))
                continue
            raise
    if last_exc is not None:
        if isinstance(last_exc, OSError) and getattr(last_exc, "errno", None) in _RETRY_ERRNOS:
            raise TimeoutError("I/O retry deadline exceeded")
        raise last_exc


def _read_text_retry(p: Path, *, encoding: str = "utf-8", errors: str = "strict") -> str:
    return _with_retries(lambda: p.read_text(encoding=encoding, errors=errors))


@dataclass
class ScoreResult:
    total: float
    breakdown: Dict[str, float]
    errors: List[str]
    details: Dict[str, Any]


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()

    def _read_all():
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)

    _with_retries(_read_all)
    return h.hexdigest()


def _load_json(p: Path) -> Dict[str, Any]:
    return json.loads(_read_text_retry(p, encoding="utf-8", errors="strict"))


def _count_jsonl_rows(p: Path) -> int:
    def _count():
        n = 0
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    n += 1
        return n

    return _with_retries(_count)


def _dedup_check_jsonl(p: Path, key_fields: List[str]) -> Tuple[int, int]:
    def _check():
        seen = set()
        total = 0
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total += 1
                obj = json.loads(line)
                k = tuple(obj.get(kf) for kf in key_fields)
                seen.add(k)
        return total, len(seen)

    return _with_retries(_check)


def score_output(output_dir: Path, task_expected: Dict[str, Any]) -> ScoreResult:
    """
    Enhanced scoring with multiple dimensions:
    - correctness (40 points): Data accuracy with gradient scoring
    - completeness (20 points): Required files and fields
    - robustness (20 points): Error handling capability  
    - efficiency (20 points): Request count and execution time
    
    Total: 100 points with gradient scoring (not just 0 or 100)
    """
    errors: List[str] = []
    breakdown: Dict[str, float] = {
        "correctness": 0.0, 
        "completeness": 0.0,
        "robustness": 0.0, 
        "efficiency": 0.0
    }
    details: Dict[str, Any] = {}

    if not output_dir.exists():
        return ScoreResult(0.0, breakdown, [f"Missing output dir: {output_dir}"], details)

    # Check required files
    missing_files = []
    for fn in REQUIRED_FILES:
        if not (output_dir / fn).exists():
            missing_files.append(fn)
            errors.append(f"Missing required file: {fn}")

    if missing_files:
        # Partial credit for partial files
        files_present = len(REQUIRED_FILES) - len(missing_files)
        breakdown["completeness"] = (files_present / len(REQUIRED_FILES)) * 10.0
        return ScoreResult(breakdown["completeness"], breakdown, errors, details)

    data_path = output_dir / "data.jsonl"
    meta_path = output_dir / "metadata.json"
    log_path = output_dir / "run.log"

    try:
        meta = _load_json(meta_path)
    except json.JSONDecodeError as e:
        return ScoreResult(0.0, breakdown, [f"metadata.json is not valid JSON: {e}"], details)
    except OSError as e:
        return ScoreResult(0.0, breakdown, [f"metadata.json could not be read: {e}"], details)

    # ============ COMPLETENESS (20 points) ============
    needed_meta_fields = ["task_id", "query", "row_count", "schema", "dedup_key"]
    fields_present = sum(1 for f in needed_meta_fields if f in meta)
    breakdown["completeness"] = (fields_present / len(needed_meta_fields)) * 20.0
    
    for f in needed_meta_fields:
        if f not in meta:
            errors.append(f"metadata.json missing field: {f}")

    row_count_actual = _count_jsonl_rows(data_path)
    details["row_count_actual"] = row_count_actual
    details["row_count_declared"] = meta.get("row_count")

    # ============ CORRECTNESS (40 points) - Gradient scoring ============
    correctness = 0.0
    
    # Row count accuracy (15 points) - gradient based on accuracy
    expected_rows = task_expected.get("constraints", {}).get("total_rows", 0)
    declared_rows = meta.get("row_count", 0)
    
    if expected_rows > 0:
        # Calculate accuracy percentage
        row_accuracy = 1.0 - abs(row_count_actual - expected_rows) / max(expected_rows, 1)
        row_accuracy = max(0.0, min(1.0, row_accuracy))
        correctness += row_accuracy * 15.0
        details["row_accuracy_pct"] = round(row_accuracy * 100, 1)
        
        if row_count_actual != expected_rows:
            errors.append(f"Row count: got {row_count_actual}, expected {expected_rows} (accuracy: {row_accuracy*100:.1f}%)")
    elif meta.get("row_count") == row_count_actual:
        correctness += 15.0
    else:
        errors.append(f"Row count mismatch: declared={meta.get('row_count')} actual={row_count_actual}")

    # Schema validation (5 points)
    schema = meta.get("schema") or []
    if isinstance(schema, list):
        expected_schema_len = 9  # Full schema has 9 columns
        schema_completeness = min(len(schema) / expected_schema_len, 1.0)
        correctness += schema_completeness * 5.0
        details["schema_completeness_pct"] = round(schema_completeness * 100, 1)
        if len(schema) < 5:
            errors.append(f"Schema incomplete: {len(schema)} columns, expected >= 5")
    else:
        errors.append("Schema must be a list")

    # Query matching (5 points)
    expected_query = task_expected.get("query", {})
    got_query = meta.get("query", {})
    query_fields = ["reporter", "partner", "flow", "hs", "year"]
    query_matches = sum(1 for k in query_fields 
                       if expected_query.get(k) is None or got_query.get(k) == expected_query.get(k))
    correctness += (query_matches / len(query_fields)) * 5.0
    if query_matches < len(query_fields):
        errors.append(f"Query mismatch: {query_matches}/{len(query_fields)} fields correct")

    # Deduplication check (10 points) - gradient based on duplicate rate
    dedup_key = meta.get("dedup_key") or []
    if isinstance(dedup_key, list) and len(dedup_key) >= 3:
        total_rows, unique_rows = _dedup_check_jsonl(data_path, dedup_key)
        details["dedup_total_rows"] = total_rows
        details["dedup_unique_rows"] = unique_rows
        
        if total_rows > 0:
            dedup_quality = unique_rows / total_rows
            correctness += dedup_quality * 10.0
            details["dedup_quality_pct"] = round(dedup_quality * 100, 1)
            if unique_rows < total_rows:
                dup_count = total_rows - unique_rows
                errors.append(f"Found {dup_count} duplicates ({(1-dedup_quality)*100:.1f}% duplicate rate)")
        else:
            correctness += 10.0
    else:
        errors.append("dedup_key invalid; expect list with >= 3 fields.")

    # Data content validation (5 points) - check data integrity
    try:
        data_valid = _validate_data_content(data_path, meta.get("schema", []))
        correctness += data_valid * 5.0
        details["data_integrity_pct"] = round(data_valid * 100, 1)
    except Exception as e:
        errors.append(f"Data validation error: {e}")

    # Totals handling check for T7_totals_trap
    if task_expected.get("fault_injection", {}).get("mode") == "totals_trap":
        totals_handling = meta.get("totals_handling", {})
        if not totals_handling.get("enabled"):
            errors.append("T7_totals_trap requires totals_handling.enabled=true")
            correctness -= 5.0
        elif totals_handling.get("rows_dropped", 0) < 1:
            errors.append("T7_totals_trap: no rows dropped")
            correctness -= 3.0

    breakdown["correctness"] = max(0.0, min(40.0, correctness))

    # ============ ROBUSTNESS (20 points) ============
    log_text = _read_text_retry(log_path, encoding="utf-8", errors="ignore").lower()
    robustness = 0.0
    mode = task_expected.get("fault_injection", {}).get("mode")

    if mode == "rate_limit":
        # Check for 429 handling with gradient scoring
        has_429 = "429" in log_text
        has_retry = "retry" in log_text or "backoff" in log_text
        has_exponential = "exponential" in log_text or "backoff" in log_text
        
        if has_429 and has_retry:
            robustness = 15.0
            if has_exponential:
                robustness = 20.0  # Bonus for proper exponential backoff
        elif has_retry:
            robustness = 10.0
        else:
            errors.append("No evidence of 429 handling in run.log")
            
    elif mode == "server_error":
        has_500 = "500" in log_text
        has_retry = "retry" in log_text
        has_limit = "max" in log_text or "limit" in log_text
        
        if has_500 and has_retry:
            robustness = 15.0
            if has_limit:
                robustness = 20.0  # Bonus for retry limits
        elif has_retry:
            robustness = 10.0
        else:
            errors.append("No evidence of 500 handling in run.log")
    else:
        # For other modes, check log quality
        log_lines = log_text.strip().split('\n')
        if len(log_lines) >= 5:
            robustness = 20.0
        elif len(log_lines) >= 3:
            robustness = 15.0
        elif len(log_text.strip()) > 10:
            robustness = 10.0

    breakdown["robustness"] = robustness

    # ============ EFFICIENCY (20 points) ============
    efficiency = 0.0
    
    # Parse execution metrics from metadata or log
    exec_time = meta.get("execution_time_seconds", 0)
    request_count = meta.get("request_count", 0)
    
    # If not in metadata, try to extract from log
    if request_count == 0:
        request_count = log_text.count("request") + log_text.count("fetch") + log_text.count("page")
    
    constraints = task_expected.get("constraints", {})
    max_requests = constraints.get("max_requests", 100)
    
    # Request efficiency (10 points)
    if request_count > 0 and max_requests > 0:
        # Lower request count = better efficiency
        request_efficiency = 1.0 - min(request_count / max_requests, 1.0)
        # But must complete the task, so penalize if too few requests for multi-page
        total_rows = constraints.get("total_rows", 0)
        page_size = constraints.get("page_size", 100)
        min_requests_needed = max(1, total_rows // page_size)
        
        if request_count >= min_requests_needed:
            efficiency += (0.5 + request_efficiency * 0.5) * 10.0
        else:
            efficiency += request_efficiency * 5.0
        
        details["request_count"] = request_count
        details["request_efficiency_pct"] = round(request_efficiency * 100, 1)
    else:
        efficiency += 10.0  # Default if not measurable
    
    # Time efficiency (10 points) - if available
    if exec_time > 0:
        # Assume 60 seconds is baseline, faster = better
        time_efficiency = max(0, 1.0 - exec_time / 60.0)
        efficiency += time_efficiency * 10.0
        details["execution_time_seconds"] = exec_time
        details["time_efficiency_pct"] = round(time_efficiency * 100, 1)
    else:
        efficiency += 10.0  # Default if not measurable

    breakdown["efficiency"] = min(20.0, efficiency)

    details["data_sha256"] = _sha256_file(data_path)
    details["metadata_sha256"] = _sha256_file(meta_path)

    total = sum(breakdown.values())
    return ScoreResult(total=round(total, 1), breakdown=breakdown, errors=errors, details=details)


def _validate_data_content(data_path: Path, schema: List[str]) -> float:
    """Validate data content quality. Returns score 0.0 - 1.0"""
    def _validate():
        valid_rows = 0
        total_rows = 0
        
        with data_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_rows += 1
                try:
                    obj = json.loads(line)
                    # Check if row has expected fields
                    if isinstance(obj, dict):
                        # At least 50% of schema fields should be present
                        if schema:
                            fields_present = sum(1 for s in schema if s in obj)
                            if fields_present >= len(schema) * 0.5:
                                valid_rows += 1
                        else:
                            valid_rows += 1
                except json.JSONDecodeError:
                    pass
        
        return valid_rows / total_rows if total_rows > 0 else 0.0
    
    return _with_retries(_validate)
