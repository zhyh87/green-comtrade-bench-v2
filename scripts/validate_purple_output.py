#!/usr/bin/env python3
"""
Purple Output Validator for Green Comtrade Bench

Validates purple agent output against Evaluation Contract v1.0.0 (2026-01-14).
Implements strict JSON/JSONL parsing, type checking, and constraint validation.

Error Codes (Appendix B):
  E001: Missing output directory
  E002: Missing required file
  E003: Invalid JSON in metadata
  E004: Row count mismatch
  E005: Schema too small
  E006: Query mismatch
  E007: Duplicates found
  E008: No retry evidence
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

# Mandatory fields for data.jsonl records (§4.1.1)
MANDATORY_FIELDS = {
    "year": int,
    "reporter": str,
    "partner": str,
    "flow": str,
    "hs": str,
    "tradeValue": int,
    "netWeight": int,
    "qty": int,
    "record_id": str,
}

# Minimum required fields in dedup_key (§2.2)
REQUIRED_DEDUP_FIELDS = {"year", "reporter", "partner", "flow", "hs", "record_id"}

# Log evidence patterns by fault mode (§9.3)
LOG_EVIDENCE = {
    "rate_limit": [("429", "retry or backoff")],
    "server_error": [("500", "retry")],
    "totals_trap": [("totals", None)],
}

# Totals marker definition (§5.5)
TOTALS_MARKERS = {"isTotal": True, "partner": "WLD", "hs": "TOTAL"}


def error_exit(code: str, message: str) -> None:
    """Print error with code and exit non-zero."""
    print(f"[{code}] {message}", file=sys.stderr)
    sys.exit(1)


def validate_directory_exists(output_dir: Path, task_id: str) -> None:
    """E001: Validate output directory exists."""
    if not output_dir.exists():
        error_exit("E001", f"Missing output directory: {output_dir}")
    if not output_dir.is_dir():
        error_exit("E001", f"Path is not a directory: {output_dir}")


def validate_required_files(output_dir: Path) -> Tuple[Path, Path, Path]:
    """E002: Validate required files present."""
    data_file = output_dir / "data.jsonl"
    metadata_file = output_dir / "metadata.json"
    log_file = output_dir / "run.log"
    
    if not data_file.exists():
        error_exit("E002", f"Missing required file: {data_file}")
    if not metadata_file.exists():
        error_exit("E002", f"Missing required file: {metadata_file}")
    if not log_file.exists():
        error_exit("E002", f"Missing required file: {log_file}")
    
    return data_file, metadata_file, log_file


def load_metadata(metadata_file: Path) -> Dict[str, Any]:
    """E003: Load and validate metadata.json."""
    try:
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        return metadata
    except json.JSONDecodeError as e:
        error_exit("E003", f"Invalid JSON in metadata: {e}")
    except UnicodeDecodeError:
        error_exit("E003", f"Metadata is not UTF-8: {metadata_file}")


def load_jsonl_records(data_file: Path) -> List[Dict[str, Any]]:
    """Load JSONL records, ignoring blank lines, strict UTF-8."""
    records = []
    try:
        with open(data_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:  # Ignore blank lines (§4.1.4)
                    continue
                try:
                    record = json.loads(line)
                    records.append(record)
                except json.JSONDecodeError as e:
                    error_exit("E003", f"Invalid JSON in {data_file}:{line_num}: {e}")
        return records
    except UnicodeDecodeError:
        error_exit("E003", f"Data file is not UTF-8: {data_file}")


def validate_record_fields(record: Dict[str, Any], record_idx: int) -> None:
    """Validate mandatory fields and types for a single record."""
    for field, expected_type in MANDATORY_FIELDS.items():
        if field not in record:
            error_exit(
                "E003",
                f"Record {record_idx}: missing mandatory field '{field}'",
            )
        value = record[field]
        if not isinstance(value, expected_type):
            error_exit(
                "E003",
                f"Record {record_idx}: field '{field}' must be {expected_type.__name__}, got {type(value).__name__}",
            )
        
        # Non-negative constraints (§4.1.1)
        if field in {"tradeValue", "netWeight", "qty"} and value < 0:
            error_exit(
                "E003",
                f"Record {record_idx}: field '{field}' must be non-negative, got {value}",
            )
        
        # Flow constraint
        if field == "flow" and value not in ("M", "X"):
            error_exit(
                "E003",
                f"Record {record_idx}: field 'flow' must be 'M' or 'X', got '{value}'",
            )


def validate_row_count(metadata: Dict[str, Any], actual_count: int) -> None:
    """E004: Validate row_count matches actual non-empty lines."""
    declared_count = metadata.get("row_count")
    if declared_count is None:
        error_exit("E004", "Metadata missing required field 'row_count'")
    if not isinstance(declared_count, int):
        error_exit("E004", f"row_count must be integer, got {type(declared_count).__name__}")
    if declared_count != actual_count:
        error_exit(
            "E004",
            f"Row count mismatch: metadata declares {declared_count}, actual {actual_count}",
        )


def validate_schema(metadata: Dict[str, Any]) -> None:
    """E005: Validate schema field."""
    schema = metadata.get("schema")
    if schema is None:
        error_exit("E005", "Metadata missing required field 'schema'")
    if not isinstance(schema, list):
        error_exit("E005", f"schema must be array, got {type(schema).__name__}")
    if len(schema) < 5:
        error_exit("E005", f"Schema too small: has {len(schema)} fields, need >= 5")
    
    # Validate schema includes all mandatory fields
    schema_set = set(schema)
    missing = set(MANDATORY_FIELDS.keys()) - schema_set
    if missing:
        error_exit("E005", f"Schema missing mandatory fields: {sorted(missing)}")


def validate_query(metadata: Dict[str, Any], task_query: Dict[str, Any]) -> None:
    """E006: Validate query matches task query (type-aware)."""
    query = metadata.get("query")
    if query is None:
        error_exit("E006", "Metadata missing required field 'query'")
    if not isinstance(query, dict):
        error_exit("E006", f"query must be object, got {type(query).__name__}")
    
    # Check all expected keys present and match (§4.2.4)
    required_keys = {"reporter", "partner", "flow", "hs", "year"}
    missing_keys = required_keys - set(query.keys())
    if missing_keys:
        error_exit("E006", f"Query missing required keys: {sorted(missing_keys)}")
    
    # Type-aware comparison (§4.2.4)
    for key in required_keys:
        expected = task_query[key]
        actual = query[key]
        if actual != expected or type(actual) != type(expected):
            error_exit(
                "E006",
                f"Query mismatch on '{key}': expected {expected!r} ({type(expected).__name__}), "
                f"got {actual!r} ({type(actual).__name__})",
            )


def validate_dedup_key(metadata: Dict[str, Any]) -> List[str]:
    """Validate dedup_key field."""
    dedup_key = metadata.get("dedup_key")
    if dedup_key is None:
        error_exit("E007", "Metadata missing required field 'dedup_key'")
    if not isinstance(dedup_key, list):
        error_exit("E007", f"dedup_key must be array, got {type(dedup_key).__name__}")
    if len(dedup_key) < 3:
        error_exit("E007", f"dedup_key too small: has {len(dedup_key)} fields, need >= 3")
    
    # Validate includes required primary key fields (§2.2)
    dedup_set = set(dedup_key)
    missing = REQUIRED_DEDUP_FIELDS - dedup_set
    if missing:
        error_exit("E007", f"dedup_key missing required fields: {sorted(missing)}")
    
    return dedup_key


def validate_no_duplicates(records: List[Dict[str, Any]], dedup_key: List[str]) -> None:
    """E007: Validate no duplicate rows by dedup_key."""
    seen: Set[Tuple] = set()
    for idx, record in enumerate(records):
        # Extract dedup tuple
        try:
            dedup_tuple = tuple(record.get(field) for field in dedup_key)
        except KeyError as e:
            error_exit("E007", f"Record {idx}: missing dedup_key field {e}")
        
        if dedup_tuple in seen:
            error_exit(
                "E007",
                f"Duplicate found at record {idx}: {dict(zip(dedup_key, dedup_tuple))}",
            )
        seen.add(dedup_tuple)


def validate_log_evidence(log_file: Path, fault_mode: str) -> None:
    """E008: Validate log contains required evidence for fault mode."""
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            log_content = f.read().lower()  # Case-insensitive (§9.3)
    except UnicodeDecodeError:
        error_exit("E008", f"Log file is not UTF-8: {log_file}")
    
    # Minimum log content (§4.3.1)
    if len(log_content.strip()) < 10:
        error_exit("E008", f"Log file too short: need >= 10 non-whitespace chars")
    
    # Fault-specific evidence (§4.3.2)
    if fault_mode in LOG_EVIDENCE:
        for required, optional in LOG_EVIDENCE[fault_mode]:
            if required.lower() not in log_content:
                error_exit(
                    "E008",
                    f"No {fault_mode} evidence: log missing '{required}' pattern",
                )
            if optional and not any(
                pattern.lower() in log_content
                for pattern in optional.split(" or ")
            ):
                error_exit(
                    "E008",
                    f"No {fault_mode} evidence: log missing '{optional}' pattern",
                )


def validate_no_totals_rows(records: List[Dict[str, Any]]) -> None:
    """Validate totals rows have been dropped (§5.5)."""
    for idx, record in enumerate(records):
        is_total = record.get("isTotal")
        partner = record.get("partner")
        hs = record.get("hs")
        
        # Check if matches totals marker
        if (
            is_total == TOTALS_MARKERS["isTotal"]
            and partner == TOTALS_MARKERS["partner"]
            and hs == TOTALS_MARKERS["hs"]
        ):
            error_exit(
                "E007",
                f"Totals row not dropped at record {idx}: isTotal={is_total}, partner={partner}, hs={hs}",
            )


def main():
    parser = argparse.ArgumentParser(
        description="Validate purple agent output against Evaluation Contract v1.0.0"
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Path to purple output directory (e.g. _purple_output/T1_single_page)",
    )
    parser.add_argument(
        "--task-query",
        type=str,
        help="JSON string of expected task query (for E006 validation)",
    )
    parser.add_argument(
        "--fault-mode",
        type=str,
        choices=["none", "rate_limit", "server_error", "duplicates", "page_drift", "totals_trap", "pagination"],
        default="none",
        help="Fault injection mode for log evidence validation",
    )
    args = parser.parse_args()
    
    output_dir = args.output_dir
    task_id = output_dir.name
    
    # E001: Validate directory exists
    validate_directory_exists(output_dir, task_id)
    
    # E002: Validate required files
    data_file, metadata_file, log_file = validate_required_files(output_dir)
    
    # E003: Load metadata
    metadata = load_metadata(metadata_file)
    
    # Validate task_id matches directory (§4.2.1)
    metadata_task_id = metadata.get("task_id")
    if metadata_task_id != task_id:
        error_exit(
            "E003",
            f"task_id mismatch: directory is '{task_id}', metadata declares '{metadata_task_id}'",
        )
    
    # Load JSONL records
    records = load_jsonl_records(data_file)
    
    # Validate all records
    for idx, record in enumerate(records):
        validate_record_fields(record, idx)
    
    # E004: Row count match
    validate_row_count(metadata, len(records))
    
    # E005: Schema validation
    validate_schema(metadata)
    
    # E006: Query validation (if provided)
    if args.task_query:
        try:
            task_query = json.loads(args.task_query)
            validate_query(metadata, task_query)
        except json.JSONDecodeError as e:
            error_exit("E006", f"Invalid --task-query JSON: {e}")
    
    # E007: Dedup validation
    dedup_key = validate_dedup_key(metadata)
    validate_no_duplicates(records, dedup_key)
    
    # Totals validation for totals_trap mode
    if args.fault_mode == "totals_trap":
        validate_no_totals_rows(records)
    
    # E008: Log evidence
    validate_log_evidence(log_file, args.fault_mode)
    
    print(f"✓ {task_id}: All validations passed")
    print(f"  - {len(records)} records")
    print(f"  - dedup_key: {dedup_key}")
    print(f"  - fault_mode: {args.fault_mode}")
    sys.exit(0)


if __name__ == "__main__":
    main()
