# green-comtrade-bench

[![CI](https://github.com/zhyh87/green-comtrade-bench/actions/workflows/ci.yml/badge.svg)](https://github.com/zhyh87/green-comtrade-bench/actions/workflows/ci.yml)

Deterministic, offline Comtrade-like benchmark (Green agent) with a configurable mock service and a scoring judge. It is designed to evaluate Purple agents on pagination, de-duplication, retries (429/500), page drift, and totals handling.

## Installation

**Recommended (development)**:
```bash
pip install -e .
```

This installs the package in editable mode from `pyproject.toml`, which is the canonical source for all dependencies.

**Why pyproject.toml?** Modern Python packaging standard (PEP 518/517). Ensures identical dependencies across local development, Docker builds, and CI. The `requirements.txt` file is deprecated and kept only for backward compatibility.

## Quickstart

```bash
make clean
make up
make fixtures
make test
```

Run one task:

```bash
make test-one TASK=T6_page_drift
```

Endpoints:
- Mock service (Swagger UI): http://localhost:8000/docs
- Green agent card:         http://localhost:9009/agent-card
- Assess endpoint:          http://localhost:9009/assess

## AgentBeats Submission

This Green bench provides deterministic offline evaluation with a mock Comtrade API and automated scoring. Purple agents submit file-based outputs under `_purple_output/<task_id>/`.

**A2A Endpoints:**
- `GET /.well-known/agent.json` — Agent discovery
- `POST /a2a/rpc` — JSON-RPC 2.0 (methods: tasks/send, tasks/get, tasks/cancel, tasks/sendSubscribe)
- `GET /healthz` — Health check

**AgentBeats Integration:**
- Leaderboard query: `agentbeats_leaderboard.sql`
- Repository: Public with webhook configured
- Docker image: `ghcr.io/zhyh87/green-comtrade-bench:latest`

**CI Reproducibility:**
- Pipeline: `make clean` → `make up` → wait + health checks → `make fixtures` → `make test` → cleanup (always)
- Environment: GitHub Actions ubuntu-latest with Docker Compose v2
- Workflow: `.github/workflows/ci.yml` runs on main branch pushes and PRs
- Status: See badge above for current CI state

**A2A Mapping:** The `tasks/send` method calls the same internal logic as `POST /assess`. Example:

```bash
curl -X POST http://localhost:9009/a2a/rpc -H 'Content-Type: application/json' -d '{
  "jsonrpc": "2.0", "id": "1", "method": "tasks/send",
  "params": {"task": {"input": {"type": "object", "content": {"task_id": "T6_page_drift"}}}}
}'
```

Response includes `result.task.output.content` with `score_total`, `score_breakdown`, `errors`, and `details`.

## Baseline Purple Agent

The `baseline_purple/` module provides a reference Purple agent implementation for validating the benchmark.

**Key Features:**
- Calls POST `/configure` to set up mock service per task
- Fetches records from GET `/records` with pagination
- Implements deterministic retry/backoff for HTTP 429 and 500
- Handles T7 totals rows correctly (drops marker rows)
- Outputs contract-compliant files to `_purple_output/<task_id>/`

**Agent Modes:**

The baseline agent runs in **fixed mode** by default:
- Uses deterministic logic (no LLM)
- Can achieve high scores on many tasks with robust error handling
- Demonstrates proper pagination, retry logic, and data validation

> **Note**: Actual scores are environment-dependent and require validation via CI artifacts. Competitive performance requires robust error handling, retries, deduplication, and careful logging under adversarial conditions.

**Run single task:**
```bash
make purple-one TASK=T1_single_page
```

**Run all tasks:**
```bash
make purple-all
```

**Validate outputs:**
```bash
make purple-all && make test
```

This baseline demonstrates that competitive scores don't require LLMs—just robust HTTP handling and data quality checks.


## Layout

```text
green-comtrade-bench/
  mock_service/           # FastAPI mock Comtrade-like API
  mock_service/fixtures/  # Optional file-backed datasets (*.jsonl)
  src/                    # Green agent + judge
  scripts/                # dev_up/stage_fixtures/run_one/run_all
  _purple_output/         # Fixture source directory staged into /workspace/purple_output
  docker-compose.yml
  Makefile
  EVALUATION_CONTRACT.md
```

## Tasks (T1–T7)

| task_id             | fault mode   | what it tests                         |
|---------------------|--------------|-------------------------------------|
| T1_single_page       | none         | baseline schema/metadata             |
| T2_multi_page        | none         | pagination correctness               |
| T3_duplicates       | duplicates   | de-dup under `dedup_key`             |
| T4_rate_limit_429    | rate_limit   | retry/backoff on 429                 |
| T5_server_error_500  | server_error | retry on 500                        |
| T6_page_drift        | page_drift   | canonical sort + convergence         |
| T7_totals_trap       | totals_trap  | drop totals rows + report totals_handling |

Authoritative task definitions live in `src/tasks.py`.

## How Scoring Works

Each task is evaluated against three criteria:

- **Completeness** (30 points): All required files present and valid
- **Correctness** (50 points): Data matches expected output, proper deduplication
- **Robustness** (20 points): Proper error handling and logging evidence

**Total Score per Task** = Completeness + Correctness + Robustness (max 100 points)

### Baseline Performance

| Agent Mode | Performance | Notes |
|------------|-------------|-------|
| Baseline Purple (fixed) | High scores achievable* | Deterministic, no LLM required |
| Baseline Purple (ground truth) | 100% (validation) | Validates judge correctness |

*Environment-dependent; requires robust error handling, retries, and deduplication. Verify via CI artifacts.

The baseline agent demonstrates that high scores are achievable with deterministic logic, proper pagination, and robust error handling—no LLM required.

**Scoring Implementation**: The green judge scoring logic is implemented in [`src/judge.py:score_output()`](file:///Users/sarah/Desktop/Antigravity/TEST/green-comtrade-bench/src/judge.py#L102-L204). This function evaluates purple agent outputs against the Evaluation Contract v1.0.0, returning a `ScoreResult` with total score, breakdown (completeness/correctness/robustness), errors, and details.

## Evaluation contract

**The Evaluation Contract is normative.** In case of discrepancy, the Green judge implementation ([`src/judge.py:score_output()`](file:///Users/sarah/Desktop/Antigravity/TEST/green-comtrade-bench/src/judge.py#L102-L204)) is authoritative.

The authoritative scoring/output contract is in **EVALUATION_CONTRACT.md**. Purple agents must write:

- `_purple_output/<task_id>/data.jsonl`
- `_purple_output/<task_id>/metadata.json`
- `_purple_output/<task_id>/run.log`

Key requirements (summary):
- Deterministic `data.jsonl` ordering (stable sort) and de-dup under `dedup_key`.
- `metadata.json.query` must match task query keys.
- Fault tasks must show retry/backoff evidence in `run.log`.
- Totals tasks must drop totals rows and report `totals_handling`.

See **EVALUATION_CONTRACT.md** for the full schema, stop reasons, and scoring breakdown.

## Related Repositories

| Repository | Description |
|------------|-------------|
| [Leaderboard](https://github.com/zhyh87/agentbeats-leaderboard) | Submission tracking and automated assessment |
| [Baseline Purple Agent](file:///Users/sarah/Desktop/Antigravity/TEST/green-comtrade-bench/baseline_purple) | Reference implementation (this repo) |

### AgentBeats URLs

- **Green Agent**: https://agentbeats.dev/zhyh87/green-comtrade-bench *(coming soon)*
- **Leaderboard**: https://agentbeats.dev/zhyh87/green-comtrade-bench-leaderboard *(coming soon)*

## Demo Video

*Coming soon - will showcase baseline purple agent running against all T1-T7 tasks*

## Contract Validation (Offline)

The benchmark includes an offline validator to verify purple agent outputs against the [Evaluation Contract v1.0.0](file:///Users/sarah/Desktop/Antigravity/TEST/green-comtrade-bench/EVALUATION_CONTRACT.md).

**Validate a single task:**
```bash
python scripts/validate_purple_output.py _purple_output/T1_single_page \
  --task-query '{"reporter":"840","partner":"156","flow":"M","hs":"85","year":2021}' \
  --fault-mode none
```

**Validate all tasks:**
```bash
for task in _purple_output/T*; do
  python scripts/validate_purple_output.py "$task"
done
```

**What it checks:**
- ✓ Required files present (`data.jsonl`, `metadata.json`, `run.log`)
- ✓ Valid JSON/JSONL with UTF-8 encoding
- ✓ Mandatory fields and type constraints
- ✓ Row count match between metadata and actual lines
- ✓ Schema includes all required fields
- ✓ Query parameters match task (type-aware)
- ✓ No duplicate rows by dedup_key
- ✓ Totals rows dropped (for T7)
- ✓ Log evidence for fault handling

**CI Integration**: This validator can be integrated into CI pipelines to reject invalid Purple outputs before leaderboard ingestion, ensuring only contract-compliant submissions are processed.

**Error codes:** See [EVALUATION_CONTRACT.md Appendix B](file:///Users/sarah/Desktop/Antigravity/TEST/green-comtrade-bench/EVALUATION_CONTRACT.md) for E001-E008 definitions.

**JSON Schemas:** Available in `schemas/` directory for automated validation:
- `metadata.schema.json` — Validates `metadata.json` structure
- `data_record.schema.json` — Validates individual JSONL record format

### Minimal Passing run.log Example (T5_server_error_500)

For tasks with fault injection (T4, T5), `run.log` must contain evidence of retry logic:

```text
WARN HTTP 500 received, retrying request
INFO Retry successful, continuing
```

**Required patterns**:
- T4 (rate_limit): Must include "429" AND ("retry" OR "backoff")
- T5 (server_error): Must include "500" AND "retry"

## Troubleshooting

- **Check staged outputs:** `docker compose exec -T green-agent ls -lah /workspace/purple_output`
- **macOS Docker file sharing (Errno 35 / deadlock):** avoid bind-mounting Purple output directories. This repo uses a named Docker volume for `/workspace/purple_output` and stages files via `make stage`.
- **If scores/timeouts look wrong:** run a clean reset:

```bash
make clean
make up
make fixtures
make test
```

- **View logs:**

```bash
make logs
# or
docker compose logs -f --tail=200
```
