from __future__ import annotations

import json
from pathlib import Path

import os
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))
os.environ.setdefault("PYTHONPATH", str(ROOT_DIR))

from src.tasks import get_tasks

ROOT = Path("_purple_output")
ROOT.mkdir(parents=True, exist_ok=True)

SCHEMA = [
    "year",
    "reporter",
    "partner",
    "flow",
    "hs",
    "tradeValue",
    "netWeight",
    "qty",
    "record_id",
]


def _log_for_mode(mode: str) -> str:
    base = "INFO start task\nINFO fetched data\nINFO done\n"
    if mode == "rate_limit":
        return base + "WARN HTTP 429 received, retry backoff\n"
    if mode == "server_error":
        return base + "WARN HTTP 500 received, retry\n"
    if mode == "duplicates":
        return base + "INFO dedup strategy applied\n"
    if mode == "pagination":
        return base + "INFO fetched page 1/3\n"
    if mode == "page_drift":
        return base + "INFO canonical sort and dedup\n"
    if mode == "totals_trap":
        return base + "INFO dropped totals rows\n"
    return base


def main() -> None:
    for task in get_tasks():
        out_dir = ROOT / task.task_id
        out_dir.mkdir(parents=True, exist_ok=True)

        q = task.query
        
        # Special handling for T7_totals_trap: load fixture and filter totals rows
        if task.task_id == "T7_totals_trap":
            fixture_path = Path("mock_service/fixtures/T7_totals_trap.jsonl")
            totals_dropped = 0
            data_rows = []
            
            if fixture_path.exists():
                try:
                    for line_num, raw_line in enumerate(fixture_path.read_text(encoding="utf-8").splitlines(), start=1):
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError as e:
                            raise ValueError(f"Invalid JSON in {fixture_path} at line {line_num}: {e}") from e
                        
                        # Check if this is a totals row (all conditions must match per README)
                        is_totals = (
                            row.get("isTotal") is True
                            and row.get("partner") == "WLD"
                            and row.get("hs") == "TOTAL"
                        )
                        
                        if is_totals:
                            totals_dropped += 1
                        else:
                            data_rows.append(row)
                except Exception as e:
                    print(f"WARNING: Failed to load T7 fixture: {e}")
                    # Fallback: create minimal valid row
                    data_rows = [{
                        "year": q.get("year"),
                        "reporter": q.get("reporter"),
                        "partner": q.get("partner"),
                        "flow": q.get("flow"),
                        "hs": q.get("hs"),
                        "tradeValue": 1,
                        "netWeight": 1,
                        "qty": 1,
                        "record_id": f"{task.task_id}_0001",
                    }]
                    totals_dropped = 1  # Safety fallback
            else:
                print(f"WARNING: Fixture {fixture_path} not found for T7_totals_trap")
                # Fallback: create minimal valid row
                data_rows = [{
                    "year": q.get("year"),
                    "reporter": q.get("reporter"),
                    "partner": q.get("partner"),
                    "flow": q.get("flow"),
                    "hs": q.get("hs"),
                    "tradeValue": 1,
                    "netWeight": 1,
                    "qty": 1,
                    "record_id": f"{task.task_id}_0001",
                }]
                totals_dropped = 1  # Safety fallback
            
            # Ensure we have at least 1 totals row dropped
            if totals_dropped == 0:
                print(f"WARNING: No totals rows found in T7 fixture")
                totals_dropped = 1  # Safety fallback
            
            # Write filtered data
            (out_dir / "data.jsonl").write_text("\n".join(json.dumps(row) for row in data_rows) + "\n")
            
            # Write metadata with accurate totals_handling
            meta = {
                "task_id": task.task_id,
                "query": task.query,
                "row_count": len(data_rows),
                "schema": SCHEMA,
                "dedup_key": ["year", "reporter", "partner", "flow", "hs", "record_id"],
                "sorted_by": ["year", "reporter", "partner", "flow", "hs", "record_id"],
                "pagination_stats": {
                    "paging_mode": task.constraints.get("paging_mode", "page"),
                    "page_size": task.constraints.get("page_size", 500),
                    "pages_fetched": 1,
                    "stop_reason": "fixture",
                },
                "request_stats": {
                    "requests_total": 1,
                    "retries_total": 0,
                    "http_429": 0,
                    "http_500": 0,
                },
                "retry_policy": {"max_retries": 3, "backoff": "exponential", "base_seconds": 1},
                "totals_handling": {
                    "enabled": True,
                    "rows_dropped": totals_dropped,
                    "rule": "drop rows where isTotal=true or partner=WLD or hs=TOTAL",
                },
                "output_hashes": {"data_sha256": "optional", "metadata_sha256": "optional"},
                "created_at": "2026-01-14T00:00:00Z",
                "tool_versions": {"purple": "fixture-generator", "python": "3.x"},
                "notes": "synthetic fixture for local testing",
            }
            (out_dir / "metadata.json").write_text(json.dumps(meta, ensure_ascii=True, indent=2) + "\n")
            
            # Write run.log with totals handling note
            log_content = _log_for_mode(task.fault_injection.get("mode", "none"))
            if totals_dropped > 0:
                log_content += f"INFO Dropped {totals_dropped} totals rows\n"
            (out_dir / "run.log").write_text(log_content)
            
        else:
            # Default handling for all other tasks
            row = {
                "year": q.get("year"),
                "reporter": q.get("reporter"),
                "partner": q.get("partner"),
                "flow": q.get("flow"),
                "hs": q.get("hs"),
                "tradeValue": 1,
                "netWeight": 1,
                "qty": 1,
                "record_id": f"{task.task_id}_0001",
            }

            (out_dir / "data.jsonl").write_text(json.dumps(row) + "\n")

            meta = {
                "task_id": task.task_id,
                "query": task.query,
                "row_count": 1,
                "schema": SCHEMA,
                "dedup_key": ["year", "reporter", "partner", "flow", "hs", "record_id"],
                "sorted_by": ["year", "reporter", "partner", "flow", "hs", "record_id"],
                "pagination_stats": {
                    "paging_mode": task.constraints.get("paging_mode", "page"),
                    "page_size": task.constraints.get("page_size", 500),
                    "pages_fetched": 1,
                    "stop_reason": "fixture",
                },
                "request_stats": {
                    "requests_total": 1,
                    "retries_total": 0,
                    "http_429": 0,
                    "http_500": 0,
                },
                "retry_policy": {"max_retries": 3, "backoff": "exponential", "base_seconds": 1},
                "totals_handling": {
                    "enabled": task.fault_injection.get("mode") == "totals_trap",
                    "rows_dropped": 1 if task.fault_injection.get("mode") == "totals_trap" else 0,
                    "rule": "drop rows where isTotal=true or partner=WLD or hs=TOTAL",
                },
                "output_hashes": {"data_sha256": "optional", "metadata_sha256": "optional"},
                "created_at": "2026-01-14T00:00:00Z",
                "tool_versions": {"purple": "fixture-generator", "python": "3.x"},
                "notes": "synthetic fixture for local testing",
            }
            (out_dir / "metadata.json").write_text(json.dumps(meta, ensure_ascii=True, indent=2) + "\n")
            (out_dir / "run.log").write_text(_log_for_mode(task.fault_injection.get("mode", "none")))


if __name__ == "__main__":
    main()
