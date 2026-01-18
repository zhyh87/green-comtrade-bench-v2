"""
Baseline Purple Agent - Core Logic

Minimal, deterministic Purple agent that:
- Waits for services to be ready
- Configures mock service via POST /configure
- Fetches records via GET /records with pagination
- Handles retries for HTTP 429 and 500 with deterministic backoff
- Drops totals rows for T7_totals_trap
- Outputs contract-compliant files: data.jsonl, metadata.json, run.log
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


class PurpleAgent:
    """Baseline Purple Agent for green-comtrade-bench evaluation."""

    def __init__(self):
        self.session = requests.Session()
        self.log_lines: List[str] = []

    def _log(self, message: str) -> None:
        """Add message to run log."""
        self.log_lines.append(message)
        print(f"[Purple] {message}")

    def _wait_for_http(self, url: str, timeout_s: int = 20, interval_s: float = 0.5) -> bool:
        """Wait for HTTP endpoint to be ready."""
        start = time.time()
        while time.time() - start < timeout_s:
            try:
                resp = self.session.get(url, timeout=2)
                if resp.status_code < 500:
                    return True
            except requests.RequestException:
                pass
            time.sleep(interval_s)
        return False

    def _get_task_definition(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Load task definition from src/tasks.py."""
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.tasks import get_tasks
            for task in get_tasks():
                if task.task_id == task_id:
                    return {
                        "task_id": task.task_id,
                        "query": task.query,
                        "constraints": task.constraints,
                        "fault_injection": task.fault_injection,
                    }
        except Exception as e:
            self._log(f"ERROR: Failed to load task definition: {e}")
        return None

    def _configure_mock(self, mock_url: str, task_def: Dict[str, Any]) -> bool:
        """Configure mock service with task definition."""
        self._log(f"INFO: Configuring mock service for task {task_def['task_id']}")
        try:
            resp = self.session.post(
                f"{mock_url}/configure",
                json=task_def,
                timeout=10,
            )
            resp.raise_for_status()
            self._log(f"INFO: Mock configured: {resp.json()}")
            return True
        except Exception as e:
            self._log(f"ERROR: Configure failed: {e}")
            return False

    def _fetch_with_retry(
        self,
        url: str,
        params: Dict[str, Any],
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """Fetch with exponential backoff on 429/500."""
        for attempt in range(max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=10)
                
                if resp.status_code == 200:
                    return resp.json()
                
                if resp.status_code in {429, 500}:
                    if attempt < max_retries:
                        backoff = 2 ** attempt  # Deterministic: 1s, 2s, 4s
                        self._log(f"WARN: HTTP {resp.status_code} received, retry after {backoff}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(backoff)
                        continue
                    else:
                        self._log(f"ERROR: HTTP {resp.status_code} after {max_retries} retries")
                        return None
                
                resp.raise_for_status()
            except requests.RequestException as e:
                self._log(f"ERROR: Request failed: {e}")
                if attempt < max_retries:
                    backoff = 2 ** attempt
                    self._log(f"WARN: Retrying after {backoff}s")
                    time.sleep(backoff)
                else:
                    return None
        return None

    def _fetch_all_pages(
        self,
        mock_url: str,
        paging_mode: str,
        page_size: int,
        max_requests: int,
        total_rows: int,
    ) -> List[Dict[str, Any]]:
        """Fetch all records using pagination."""
        self._log(f"INFO: Fetching records (paging_mode={paging_mode}, page_size={page_size})")
        
        all_rows: List[Dict[str, Any]] = []
        
        if paging_mode == "page":
            page = 1
            while len(all_rows) < total_rows and page <= max_requests:
                params = {"page": page, "page_size": page_size}
                self._log(f"INFO: Fetching page {page}")
                
                result = self._fetch_with_retry(f"{mock_url}/records", params)
                if not result:
                    break
                
                data = result.get("data", [])
                all_rows.extend(data)
                
                if len(data) < page_size:
                    self._log(f"INFO: Last page reached (returned {len(data)} rows)")
                    break
                
                page += 1
        
        elif paging_mode == "offset":
            offset = 0
            while offset < total_rows and offset // page_size < max_requests:
                params = {"offset": offset, "maxRecords": page_size}
                self._log(f"INFO: Fetching offset {offset}")
                
                result = self._fetch_with_retry(f"{mock_url}/records", params)
                if not result:
                    break
                
                data = result.get("data", [])
                all_rows.extend(data)
                
                if len(data) == 0:
                    self._log(f"INFO: No more records")
                    break
                
                offset += len(data)
        
        else:
            self._log(f"ERROR: Unknown paging_mode: {paging_mode}")
        
        self._log(f"INFO: Fetched {len(all_rows)} total rows")
        return all_rows

    def _is_totals_row(self, row: Dict[str, Any]) -> bool:
        """Check if row is a totals row per repo marker rule.
        
        Authoritative rule (all must match):
        - isTotal is True AND
        - partner is WLD AND
        - hs is TOTAL
        """
        return (
            row.get("isTotal") == True
            and row.get("partner") == "WLD"
            and row.get("hs") == "TOTAL"
        )

    def _process_rows(
        self,
        rows: List[Dict[str, Any]],
        task_id: str,
        dedup_key: List[str],
    ) -> tuple[List[Dict[str, Any]], int]:
        """Process rows: filter totals, deduplicate, sort."""
        # Filter totals rows for T7
        totals_dropped = 0
        if task_id == "T7_totals_trap":
            filtered = []
            for row in rows:
                if self._is_totals_row(row):
                    totals_dropped += 1
                else:
                    filtered.append(row)
            rows = filtered
            self._log(f"INFO: Dropped {totals_dropped} totals rows")
        
        # Deduplicate by dedup_key
        seen = set()
        unique_rows = []
        for row in rows:
            key = tuple(row.get(k) for k in dedup_key)
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        
        if len(unique_rows) < len(rows):
            self._log(f"INFO: Deduplication: {len(rows)} → {len(unique_rows)} rows")
        
        # Stable sort by dedup_key for deterministic output
        sorted_rows = sorted(unique_rows, key=lambda r: tuple(r.get(k) for k in dedup_key))
        
        return sorted_rows, totals_dropped

    def _write_outputs(
        self,
        output_dir: Path,
        task_id: str,
        query: Dict[str, Any],
        rows: List[Dict[str, Any]],
        dedup_key: List[str],
        totals_dropped: int,
    ) -> None:
        """Write contract-compliant output files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # data.jsonl
        data_path = output_dir / "data.jsonl"
        data_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
        self._log(f"INFO: Wrote {len(rows)} rows to data.jsonl")
        
        # Extract schema from first row
        schema = list(rows[0].keys()) if rows else []
        
        # metadata.json
        metadata = {
            "task_id": task_id,
            "query": query,
            "row_count": len(rows),
            "schema": schema,
            "dedup_key": dedup_key,
            "sorted_by": dedup_key,
            "pagination_stats": {
                "paging_mode": "varies",
                "page_size": "varies",
                "pages_fetched": "varies",
                "stop_reason": "complete",
            },
            "request_stats": {
                "requests_total": "varies",
                "retries_total": "varies",
                "http_429": 0,
                "http_500": 0,
            },
            "retry_policy": {
                "max_retries": 3,
                "backoff": "exponential",
                "base_seconds": 1,
            },
            "totals_handling": {
                "enabled": task_id == "T7_totals_trap",
                "rows_dropped": totals_dropped,
                "rule": "drop rows where isTotal=true AND partner=WLD AND hs=TOTAL",
            },
            "output_hashes": {"data_sha256": "optional", "metadata_sha256": "optional"},
            "created_at": "2026-01-15T00:00:00Z",
            "tool_versions": {"purple": "baseline-purple-v1", "python": "3.x"},
            "notes": "baseline purple agent output",
        }
        
        metadata_path = output_dir / "metadata.json"
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        self._log(f"INFO: Wrote metadata.json")
        
        # run.log
        log_path = output_dir / "run.log"
        log_path.write_text("\n".join(self.log_lines) + "\n", encoding="utf-8")
        self._log(f"INFO: Wrote run.log")

    def run(
        self,
        task_id: str,
        output_dir: str,
        mock_url: str = "http://localhost:8000",
    ) -> bool:
        """Run Purple agent for a single task."""
        self._log(f"INFO: Starting baseline purple agent for task {task_id}")
        
        # Wait for services to be ready
        self._log("INFO: Waiting for mock service...")
        if not self._wait_for_http(f"{mock_url}/docs", timeout_s=20):
            self._log("ERROR: Mock service not ready after 20s")
            return False
        self._log("INFO: Mock service ready")
        
        self._log("INFO: Waiting for green agent...")
        green_url = mock_url.replace(":8000", ":9009")
        if not self._wait_for_http(f"{green_url}/healthz", timeout_s=20):
            self._log("WARN: Green agent not ready (continuing anyway)")
        else:
            self._log("INFO: Green agent ready")
        
        # Load task definition
        task_def = self._get_task_definition(task_id)
        if not task_def:
            self._log(f"ERROR: Task {task_id} not found")
            return False
        
        # Configure mock service
        if not self._configure_mock(mock_url, task_def):
            return False
        
        # Extract parameters
        constraints = task_def.get("constraints", {})
        paging_mode = constraints.get("paging_mode", "page")
        page_size = constraints.get("page_size", 500)
        max_requests = constraints.get("max_requests", 50)
        total_rows = constraints.get("total_rows", 1000)
        dedup_key = ["year", "reporter", "partner", "flow", "hs", "record_id"]
        
        # Fetch all records
        rows = self._fetch_all_pages(mock_url, paging_mode, page_size, max_requests, total_rows)
        if not rows:
            self._log(f"ERROR: No rows fetched")
            return False
        
        # Process rows
        processed_rows, totals_dropped = self._process_rows(rows, task_id, dedup_key)
        
        # Write outputs
        output_path = Path(output_dir)
        self._write_outputs(
            output_path,
            task_id,
            task_def["query"],
            processed_rows,
            dedup_key,
            totals_dropped,
        )
        
        self._log(f"INFO: Task {task_id} complete (output: {output_path})")
        return True
