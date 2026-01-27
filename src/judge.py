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

# ============ GOVERNANCE CONSTANTS ============
# These thresholds enforce scoring fairness and anti-gaming rules.
# See README.md "Scoring Philosophy / Governance" for rationale.

# Threshold: correctness must reach 70% for full efficiency/observability credit
CORRECTNESS_GATE_THRESHOLD = 0.70  # 70% of max 30 points = 21 points
CORRECTNESS_GATE_PENALTY = 0.50    # efficiency/observability capped at 50% if below threshold

# Completeness must be 100% to earn any efficiency points
COMPLETENESS_GATE_FULL = 1.0  # Must be fully complete

# Time penalty: only penalize if execution exceeds this threshold (seconds)
# Avoids penalizing normal variance; only flags truly slow runs
EXECUTION_TIME_PENALTY_THRESHOLD = 45.0  # seconds
EXECUTION_TIME_PENALTY_POINTS = 3.0      # max points lost for slow execution

# Per-task efficiency baselines (expected request counts for fair comparison)
# Prevents pagination-heavy tasks from being unfairly penalized
TASK_EFFICIENCY_BASELINES: Dict[str, int] = {
    "T1_single_page": 1,       # Single page, 1 request expected
    "T2_multi_page": 5,        # Multi-page, ~5 requests expected
    "T3_duplicates": 3,        # Duplicates task, ~3 requests
    "T4_rate_limit_429": 4,    # Rate limit with retries, ~4 requests
    "T5_server_error_500": 4,  # Server error with retries, ~4 requests
    "T6_page_drift": 3,        # Page drift, ~3 requests
    "T7_totals_trap": 8,       # Totals trap, more pages, ~8 requests
}

# Observability: required traceable fields (must be present in log or metadata)
REQUIRED_OBSERVABILITY_FIELDS = [
    "task_id",          # Which task was executed
    "page",             # Pagination tracking (page/offset/cursor)
    "request",          # Request tracking
    "complete",         # Stop reason / completion indicator
]


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
    Comprehensive scoring with 6 dimensions:
    
    - correctness (30 points): Data accuracy with gradient scoring
    - completeness (15 points): Required files and fields
    - robustness (15 points): Error handling capability  
    - efficiency (15 points): Request count (stable metrics only)
    - data_quality (15 points): Content validation, type checking
    - observability (10 points): Traceable fields presence (not log length)
    
    Total: 100 points with gradient scoring
    
    GOVERNANCE RULES (anti-gaming, fairness):
    - If correctness < 70%, efficiency and observability are capped at 50%
    - If completeness < 100%, efficiency is 0 (no gaming incomplete outputs)
    - Efficiency uses task-specific baselines for cross-task fairness
    - Time scoring uses threshold penalty, not continuous (reproducibility)
    - Observability checks required fields, not log verbosity
    """
    errors: List[str] = []
    breakdown: Dict[str, float] = {
        "correctness": 0.0, 
        "completeness": 0.0,
        "robustness": 0.0, 
        "efficiency": 0.0,
        "data_quality": 0.0,
        "observability": 0.0
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

    # ============ COMPLETENESS (15 points) ============
    needed_meta_fields = ["task_id", "query", "row_count", "schema", "dedup_key"]
    fields_present = sum(1 for f in needed_meta_fields if f in meta)
    breakdown["completeness"] = (fields_present / len(needed_meta_fields)) * 15.0
    
    for f in needed_meta_fields:
        if f not in meta:
            errors.append(f"metadata.json missing field: {f}")

    row_count_actual = _count_jsonl_rows(data_path)
    details["row_count_actual"] = row_count_actual
    details["row_count_declared"] = meta.get("row_count")

    # ============ CORRECTNESS (30 points) - Gradient scoring ============
    correctness = 0.0
    
    # Row count accuracy (12 points) - gradient based on accuracy
    expected_rows = task_expected.get("constraints", {}).get("total_rows", 0)
    declared_rows = meta.get("row_count", 0)
    
    if expected_rows > 0:
        # Calculate accuracy percentage
        row_accuracy = 1.0 - abs(row_count_actual - expected_rows) / max(expected_rows, 1)
        row_accuracy = max(0.0, min(1.0, row_accuracy))
        correctness += row_accuracy * 12.0
        details["row_accuracy_pct"] = round(row_accuracy * 100, 1)
        
        if row_count_actual != expected_rows:
            errors.append(f"Row count: got {row_count_actual}, expected {expected_rows} (accuracy: {row_accuracy*100:.1f}%)")
    elif meta.get("row_count") == row_count_actual:
        correctness += 12.0
    else:
        errors.append(f"Row count mismatch: declared={meta.get('row_count')} actual={row_count_actual}")

    # Schema validation (4 points)
    schema = meta.get("schema") or []
    if isinstance(schema, list):
        expected_schema_len = 9  # Full schema has 9 columns
        schema_completeness = min(len(schema) / expected_schema_len, 1.0)
        correctness += schema_completeness * 4.0
        details["schema_completeness_pct"] = round(schema_completeness * 100, 1)
        if len(schema) < 5:
            errors.append(f"Schema incomplete: {len(schema)} columns, expected >= 5")
    else:
        errors.append("Schema must be a list")

    # Query matching (4 points)
    expected_query = task_expected.get("query", {})
    got_query = meta.get("query", {})
    query_fields = ["reporter", "partner", "flow", "hs", "year"]
    query_matches = sum(1 for k in query_fields 
                       if expected_query.get(k) is None or got_query.get(k) == expected_query.get(k))
    correctness += (query_matches / len(query_fields)) * 4.0
    if query_matches < len(query_fields):
        errors.append(f"Query mismatch: {query_matches}/{len(query_fields)} fields correct")

    # Deduplication check (6 points) - gradient based on duplicate rate
    dedup_key = meta.get("dedup_key") or []
    if isinstance(dedup_key, list) and len(dedup_key) >= 3:
        total_rows, unique_rows = _dedup_check_jsonl(data_path, dedup_key)
        details["dedup_total_rows"] = total_rows
        details["dedup_unique_rows"] = unique_rows
        
        if total_rows > 0:
            dedup_quality = unique_rows / total_rows
            correctness += dedup_quality * 6.0
            details["dedup_quality_pct"] = round(dedup_quality * 100, 1)
            if unique_rows < total_rows:
                dup_count = total_rows - unique_rows
                errors.append(f"Found {dup_count} duplicates ({(1-dedup_quality)*100:.1f}% duplicate rate)")
        else:
            correctness += 6.0
    else:
        errors.append("dedup_key invalid; expect list with >= 3 fields.")

    # Declared vs actual consistency (4 points)
    if meta.get("row_count") == row_count_actual:
        correctness += 4.0
    else:
        errors.append(f"Declared row_count ({meta.get('row_count')}) != actual ({row_count_actual})")

    # Totals handling check for T7_totals_trap
    if task_expected.get("fault_injection", {}).get("mode") == "totals_trap":
        totals_handling = meta.get("totals_handling", {})
        if not totals_handling.get("enabled"):
            errors.append("T7_totals_trap requires totals_handling.enabled=true")
            correctness -= 4.0
        elif totals_handling.get("rows_dropped", 0) < 1:
            errors.append("T7_totals_trap: no rows dropped")
            correctness -= 2.0

    breakdown["correctness"] = max(0.0, min(30.0, correctness))

    # ============ ROBUSTNESS (15 points) ============
    log_text = _read_text_retry(log_path, encoding="utf-8", errors="ignore").lower()
    robustness = 0.0
    mode = task_expected.get("fault_injection", {}).get("mode")

    if mode == "rate_limit":
        # Check for 429 handling with gradient scoring
        has_429 = "429" in log_text
        has_retry = "retry" in log_text or "backoff" in log_text
        has_exponential = "exponential" in log_text or "backoff" in log_text
        
        if has_429 and has_retry:
            robustness = 12.0
            if has_exponential:
                robustness = 15.0  # Bonus for proper exponential backoff
        elif has_retry:
            robustness = 8.0
        else:
            errors.append("No evidence of 429 handling in run.log")
            
    elif mode == "server_error":
        has_500 = "500" in log_text
        has_retry = "retry" in log_text
        has_limit = "max" in log_text or "limit" in log_text
        
        if has_500 and has_retry:
            robustness = 12.0
            if has_limit:
                robustness = 15.0  # Bonus for retry limits
        elif has_retry:
            robustness = 8.0
        else:
            errors.append("No evidence of 500 handling in run.log")
    else:
        # For other modes, check log quality
        log_lines = log_text.strip().split('\n')
        if len(log_lines) >= 5:
            robustness = 15.0
        elif len(log_lines) >= 3:
            robustness = 12.0
        elif len(log_text.strip()) > 10:
            robustness = 8.0

    breakdown["robustness"] = robustness

    # ============ EFFICIENCY (15 points) ============
    # GOVERNANCE: Uses stable metrics only (request count, retry count)
    # Time uses threshold penalty, not continuous scoring (reproducibility)
    efficiency = 0.0
    
    # Parse execution metrics from metadata
    exec_time = meta.get("execution_time_seconds", 0)
    request_count = meta.get("request_count", 0)
    retry_count = meta.get("request_stats", {}).get("retries_total", 0)
    
    # Get task-specific baseline for fair cross-task comparison
    task_id = meta.get("task_id", "")
    baseline_requests = TASK_EFFICIENCY_BASELINES.get(task_id, 5)
    
    details["efficiency_baseline_requests"] = baseline_requests
    details["request_count"] = request_count
    
    # Request efficiency (12 points) - relative to task-specific baseline
    if request_count > 0:
        # Score based on how close to baseline (not penalizing pagination-heavy tasks)
        # Perfect = at or below baseline; penalty for excessive requests
        if request_count <= baseline_requests:
            request_score = 12.0  # At or below baseline = full points
        else:
            # Graceful degradation: lose points proportionally to overage
            overage_ratio = (request_count - baseline_requests) / max(baseline_requests, 1)
            request_score = max(0.0, 12.0 * (1.0 - min(overage_ratio, 1.0)))
        
        efficiency += request_score
        details["request_efficiency_pct"] = round((request_score / 12.0) * 100, 1)
    else:
        efficiency += 12.0  # Default if not measurable (benefit of doubt)
    
    # Time penalty (3 points) - threshold-based, not continuous (reproducibility)
    # Only penalize if execution exceeds threshold; avoids wall-clock variance issues
    if exec_time > 0:
        details["execution_time_seconds"] = exec_time
        if exec_time <= EXECUTION_TIME_PENALTY_THRESHOLD:
            efficiency += 3.0  # Within threshold = full points
            details["time_penalty_applied"] = False
        else:
            # Exceeded threshold: apply penalty (not gradient, just penalty)
            efficiency += 0.0
            details["time_penalty_applied"] = True
            errors.append(f"Execution time {exec_time:.1f}s exceeded threshold {EXECUTION_TIME_PENALTY_THRESHOLD}s")
    else:
        efficiency += 3.0  # Default if not measurable

    breakdown["efficiency"] = min(15.0, efficiency)

    # ============ DATA QUALITY (15 points) - NEW ============
    data_quality = 0.0
    
    # Content validation (5 points) - check data integrity
    try:
        data_valid = _validate_data_content(data_path, meta.get("schema", []))
        data_quality += data_valid * 5.0
        details["data_integrity_pct"] = round(data_valid * 100, 1)
    except Exception as e:
        errors.append(f"Data validation error: {e}")
    
    # Type consistency (5 points) - check if values have consistent types
    try:
        type_score = _check_type_consistency(data_path)
        data_quality += type_score * 5.0
        details["type_consistency_pct"] = round(type_score * 100, 1)
    except Exception as e:
        errors.append(f"Type check error: {e}")
    
    # Value range validation (5 points) - check if numeric values are reasonable
    try:
        range_score = _check_value_ranges(data_path, task_expected)
        data_quality += range_score * 5.0
        details["value_range_pct"] = round(range_score * 100, 1)
    except Exception as e:
        errors.append(f"Value range check error: {e}")
    
    breakdown["data_quality"] = min(15.0, data_quality)

    # ============ OBSERVABILITY (10 points) ============
    # GOVERNANCE: Checks required traceable fields, NOT log verbosity
    # This prevents gaming by log spam while rewarding actual traceability
    observability = 0.0
    log_lines = log_text.strip().split('\n')
    details["log_line_count"] = len(log_lines)
    
    # Required traceable fields check (6 points) - 1.5 points per field
    # These fields enable post-mortem debugging and audit trails
    traceable_fields_found = []
    traceable_fields_missing = []
    
    for field in REQUIRED_OBSERVABILITY_FIELDS:
        # Check both log and metadata for presence
        field_in_log = field.lower() in log_text
        field_in_meta = field in str(meta).lower()
        if field_in_log or field_in_meta:
            traceable_fields_found.append(field)
        else:
            traceable_fields_missing.append(field)
    
    traceable_score = (len(traceable_fields_found) / len(REQUIRED_OBSERVABILITY_FIELDS)) * 6.0
    observability += traceable_score
    details["traceable_fields_found"] = traceable_fields_found
    details["traceable_fields_missing"] = traceable_fields_missing
    details["traceability_pct"] = round((len(traceable_fields_found) / len(REQUIRED_OBSERVABILITY_FIELDS)) * 100, 1)
    
    if traceable_fields_missing:
        errors.append(f"Missing traceable fields: {traceable_fields_missing}")
    
    # Structured logging (2 points) - has INFO/WARN/ERROR levels
    # Indicates proper log level discipline
    has_info = "info" in log_text
    has_warn = "warn" in log_text
    has_error = "error" in log_text
    log_levels = sum([has_info, has_warn, has_error])
    observability += min(2.0, log_levels * 0.67)  # Cap at 2 points
    details["log_levels_used"] = log_levels
    
    # Stop reason / completion indicator (2 points)
    # Must be able to determine why the task stopped
    has_stop_reason = any(kw in log_text for kw in ["complete", "finish", "done", "success", "fail", "error", "stop"])
    has_stop_in_meta = "stop_reason" in meta or "pagination_stats" in meta
    if has_stop_reason or has_stop_in_meta:
        observability += 2.0
        details["has_stop_reason"] = True
    else:
        details["has_stop_reason"] = False
        errors.append("No stop reason indicator found in log or metadata")
    
    breakdown["observability"] = min(10.0, observability)

    details["data_sha256"] = _sha256_file(data_path)
    details["metadata_sha256"] = _sha256_file(meta_path)

    # ============ GOVERNANCE GATES ============
    # Apply threshold-based caps to prevent gaming
    
    governance_applied = []
    
    # Gate 1: Completeness gate - if not 100% complete, efficiency = 0
    # Rationale: Can't claim efficiency credit for incomplete work
    completeness_ratio = breakdown["completeness"] / 15.0
    if completeness_ratio < COMPLETENESS_GATE_FULL:
        original_efficiency = breakdown["efficiency"]
        breakdown["efficiency"] = 0.0
        governance_applied.append(f"completeness_gate: efficiency {original_efficiency:.1f} → 0 (completeness {completeness_ratio*100:.0f}% < 100%)")
    
    # Gate 2: Correctness gate - if < 70%, cap efficiency and observability at 50%
    # Rationale: Can't claim quality signals if core task is largely wrong
    correctness_ratio = breakdown["correctness"] / 30.0
    if correctness_ratio < CORRECTNESS_GATE_THRESHOLD:
        # Apply 50% cap to efficiency (if not already zeroed by completeness gate)
        if breakdown["efficiency"] > 0:
            original_efficiency = breakdown["efficiency"]
            capped_efficiency = original_efficiency * CORRECTNESS_GATE_PENALTY
            breakdown["efficiency"] = capped_efficiency
            governance_applied.append(f"correctness_gate: efficiency {original_efficiency:.1f} → {capped_efficiency:.1f} (correctness {correctness_ratio*100:.0f}% < 70%)")
        
        # Apply 50% cap to observability
        original_observability = breakdown["observability"]
        capped_observability = original_observability * CORRECTNESS_GATE_PENALTY
        breakdown["observability"] = capped_observability
        governance_applied.append(f"correctness_gate: observability {original_observability:.1f} → {capped_observability:.1f} (correctness {correctness_ratio*100:.0f}% < 70%)")
    
    if governance_applied:
        details["governance_rules_applied"] = governance_applied
    
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


def _check_type_consistency(data_path: Path) -> float:
    """Check if fields have consistent types across rows. Returns score 0.0 - 1.0"""
    def _check():
        field_types: Dict[str, set] = {}
        total_rows = 0
        
        with data_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_rows += 1
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k not in field_types:
                                field_types[k] = set()
                            # Track type (allow None as compatible with any type)
                            if v is not None:
                                field_types[k].add(type(v).__name__)
                except json.JSONDecodeError:
                    pass
        
        if not field_types:
            return 0.0
        
        # Score based on type consistency (1 type per field is best)
        consistent_fields = sum(1 for types in field_types.values() if len(types) <= 1)
        return consistent_fields / len(field_types)
    
    return _with_retries(_check)


def _check_value_ranges(data_path: Path, task_expected: Dict[str, Any]) -> float:
    """Check if numeric values are within reasonable ranges. Returns score 0.0 - 1.0"""
    def _check():
        total_checks = 0
        valid_checks = 0
        
        # Expected year range
        expected_year = task_expected.get("query", {}).get("year")
        
        with data_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        continue
                    
                    # Check year field if present
                    if "year" in obj:
                        total_checks += 1
                        year_val = obj["year"]
                        if isinstance(year_val, (int, float)):
                            # Year should be reasonable (1900-2100)
                            if 1900 <= year_val <= 2100:
                                valid_checks += 1
                                # Bonus if matches expected year
                                if expected_year and year_val == expected_year:
                                    valid_checks += 0.5
                                    total_checks += 0.5
                    
                    # Check trade value if present (should be non-negative)
                    for val_field in ["value", "trade_value", "tradeValue", "primaryValue"]:
                        if val_field in obj:
                            total_checks += 1
                            val = obj[val_field]
                            if isinstance(val, (int, float)) and val >= 0:
                                valid_checks += 1
                    
                    # Check quantity if present (should be non-negative)
                    for qty_field in ["qty", "quantity", "netWgt"]:
                        if qty_field in obj:
                            total_checks += 1
                            qty = obj[qty_field]
                            if qty is None or (isinstance(qty, (int, float)) and qty >= 0):
                                valid_checks += 1
                                
                except json.JSONDecodeError:
                    pass
        
        return valid_checks / total_checks if total_checks > 0 else 1.0
    
    return _with_retries(_check)
