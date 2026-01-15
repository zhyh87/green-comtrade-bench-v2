# Baseline Purple Agent

Minimal, deterministic Purple agent implementation for validating the green-comtrade-bench evaluation contract.

## Features

- **No LLM**: Pure HTTP client implementation
- **Deterministic**: Stable sorting, fixed retry schedule (no random jitter)
- **Contract-compliant**: Outputs match EVALUATION_CONTRACT.md schema
- **Handles all fault modes**: Pagination, duplicates, 429, 500, page drift, totals trap

## Usage

### Run Single Task

```bash
python3 -m baseline_purple.run --task-id T1_single_page
```

### Run with Custom Output Directory

```bash
python3 -m baseline_purple.run --task-id T6_page_drift --output-dir /tmp/purple_out/T6
```

### Run with Custom Mock URL (Docker)

```bash
python3 -m baseline_purple.run --task-id T7_totals_trap --mock-url http://mock-comtrade:8000
```

## Makefile Targets

```bash
# Run single task (default: T1_single_page)
make purple-one TASK=T6_page_drift

# Run all tasks T1-T7
make purple-all

# Validate outputs with Green bench
make purple-all && make test
```

## Implementation Notes

- **Configure step**: Calls `POST /configure` with task definition from `src/tasks.py`
- **Fetch**: Uses `GET /records` with pagination (page or offset mode)
- **Retry logic**: Exponential backoff (1s, 2s, 4s) for HTTP 429 and 500
- **Totals handling**: For T7, drops rows where `isTotal=true AND partner=WLD AND hs=TOTAL`
- **Deduplication**: By `dedup_key` fields (year, reporter, partner, flow, hs, record_id)
- **Sorting**: Stable sort by dedup_key for deterministic output

## Output Files

For each task, creates:
- `data.jsonl`: Deduplicated, sorted records
- `metadata.json`: Task metadata with query, row_count, schema, totals_handling
- `run.log`: Execution log with retry evidence for fault tasks

## Dependencies

- Python 3.11+
- `requests` library (already in requirements.txt)
