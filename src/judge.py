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
    errors: List[str] = []
    breakdown: Dict[str, float] = {"correctness": 0.0, "robustness": 0.0, "completeness": 0.0}
    details: Dict[str, Any] = {}

    if not output_dir.exists():
        return ScoreResult(0.0, breakdown, [f"Missing output dir: {output_dir}"], details)

    for fn in REQUIRED_FILES:
        if not (output_dir / fn).exists():
            errors.append(f"Missing required file: {fn}")

    if errors:
        return ScoreResult(0.0, breakdown, errors, details)

    data_path = output_dir / "data.jsonl"
    meta_path = output_dir / "metadata.json"
    log_path = output_dir / "run.log"

    try:
        meta = _load_json(meta_path)
    except json.JSONDecodeError as e:
        return ScoreResult(0.0, breakdown, [f"metadata.json is not valid JSON: {e}"], details)
    except OSError as e:
        return ScoreResult(0.0, breakdown, [f"metadata.json could not be read: {e}"], details)

    needed_meta_fields = ["task_id", "query", "row_count", "schema", "dedup_key"]
    for f in needed_meta_fields:
        if f not in meta:
            errors.append(f"metadata.json missing field: {f}")

    row_count_actual = _count_jsonl_rows(data_path)
    details["row_count_actual"] = row_count_actual
    details["row_count_declared"] = meta.get("row_count")

    if not errors:
        breakdown["completeness"] = 30.0

    correctness = 0.0
    if meta.get("row_count") == row_count_actual:
        correctness += 20.0
    else:
        errors.append(f"Row count mismatch: declared={meta.get('row_count')} actual={row_count_actual}")

    schema = meta.get("schema") or []
    if isinstance(schema, list) and len(schema) >= 5:
        correctness += 10.0
    else:
        errors.append("Schema too small or invalid; expect list with >= 5 columns.")

    expected_query = task_expected.get("query", {})
    got_query = meta.get("query", {})
    q_ok = True
    for k in ["reporter", "partner", "flow", "hs", "year"]:
        if expected_query.get(k) is not None and got_query.get(k) != expected_query.get(k):
            q_ok = False
    if q_ok:
        correctness += 10.0
    else:
        errors.append("Query in metadata.json does not match task query keys (reporter/partner/flow/hs/year).")

    dedup_key = meta.get("dedup_key") or []
    if isinstance(dedup_key, list) and len(dedup_key) >= 3:
        total_rows, unique_rows = _dedup_check_jsonl(data_path, dedup_key)
        details["dedup_total_rows"] = total_rows
        details["dedup_unique_rows"] = unique_rows
        if unique_rows == total_rows:
            correctness += 10.0
        else:
            errors.append(f"Data appears to contain duplicates under dedup_key: total={total_rows} unique={unique_rows}")
    else:
        errors.append("dedup_key invalid; expect list with >= 3 fields.")

    # Totals handling check for T7_totals_trap
    if task_expected.get("fault_injection", {}).get("mode") == "totals_trap":
        totals_handling = meta.get("totals_handling", {})
        if not totals_handling.get("enabled"):
            errors.append("T7_totals_trap requires totals_handling.enabled=true in metadata.json")
            correctness -= 10.0
        elif totals_handling.get("rows_dropped", 0) < 1:
            errors.append("T7_totals_trap: totals_handling.enabled=true but rows_dropped < 1")
            correctness -= 10.0

    breakdown["correctness"] = min(50.0, correctness)

    log_text = _read_text_retry(log_path, encoding="utf-8", errors="ignore").lower()
    robustness = 0.0
    mode = task_expected.get("fault_injection", {}).get("mode")

    if mode == "rate_limit":
        if "429" in log_text and ("retry" in log_text or "backoff" in log_text):
            robustness = 20.0
        else:
            errors.append(
                "Robustness check failed: expected evidence of 429 handling (retry/backoff) in run.log."
            )
    elif mode == "server_error":
        if "500" in log_text and "retry" in log_text:
            robustness = 20.0
        else:
            errors.append(
                "Robustness check failed: expected evidence of 500 handling (retry) in run.log."
            )
    else:
        if len(log_text.strip()) > 10:
            robustness = 20.0

    breakdown["robustness"] = robustness

    details["data_sha256"] = _sha256_file(data_path)
    details["metadata_sha256"] = _sha256_file(meta_path)

    total = breakdown["completeness"] + breakdown["correctness"] + breakdown["robustness"]
    return ScoreResult(total=total, breakdown=breakdown, errors=errors, details=details)
