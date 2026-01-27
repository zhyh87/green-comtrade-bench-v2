# green-comtrade-bench

[![CI](https://github.com/yonghongzhang-io/green-comtrade-bench-v2/actions/workflows/ci.yml/badge.svg)](https://github.com/yonghongzhang-io/green-comtrade-bench-v2/actions/workflows/ci.yml)

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
- Docker image: `ghcr.io/yonghongzhang-io/green-comtrade-bench-v2:latest`

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

Each task is evaluated against **6 dimensions** (100 points total per task, 700 total for 7 tasks):

| Dimension | Points | What it Measures |
|-----------|--------|------------------|
| **correctness** | 30 | Data accuracy: row count, schema, query match, deduplication |
| **completeness** | 15 | Required files present, metadata fields complete |
| **robustness** | 15 | Error handling: 429/500 retry logic with backoff |
| **efficiency** | 15 | Request count relative to task baseline (stable metrics) |
| **data_quality** | 15 | Type consistency, value ranges, data integrity |
| **observability** | 10 | Traceable fields for debugging and audit trails |

**Scoring Implementation**: [`src/judge.py:score_output()`](src/judge.py)

---

## Scoring Philosophy / Governance

This benchmark is designed for **reproducibility**, **fairness**, and **anti-gaming**. The following governance rules are enforced:

### Dimension Purposes

| Dimension | Engineering Signal | Why It Matters |
|-----------|-------------------|----------------|
| correctness | Core task success | Did the agent solve the actual problem? |
| completeness | Contract compliance | Are all required outputs present? |
| robustness | Fault tolerance | Can the agent handle real-world failures? |
| efficiency | Resource discipline | Does the agent avoid unnecessary work? |
| data_quality | Output reliability | Is the data trustworthy and well-formed? |
| observability | Debuggability | Can we trace what happened if something fails? |

### Governance Rules (Anti-Gaming)

**1. Completeness Gate**
- If `completeness < 100%`, then `efficiency = 0`
- *Rationale*: You cannot claim efficiency credit for incomplete work

**2. Correctness Gate**
- If `correctness < 70%` (< 21/30 points), then:
  - `efficiency` capped at 50%
  - `observability` capped at 50%
- *Rationale*: Quality signals are meaningless if the core task is largely wrong

**3. Efficiency Stability**
- Request count is scored against **task-specific baselines** (not absolute counts)
- Execution time uses **threshold penalty** (> 45s), not continuous gradient
- *Rationale*: Pagination-heavy tasks shouldn't be penalized; wall-clock variance shouldn't affect scores

**4. Observability = Traceability, Not Verbosity**
- Points awarded for **required traceable fields** (task_id, page, request, complete)
- Log length does NOT affect score
- *Rationale*: Spamming logs is not observability; being able to debug is

### Thresholds Reference

| Threshold | Value | Effect |
|-----------|-------|--------|
| Correctness gate | 70% (21/30 pts) | Below → efficiency/observability capped at 50% |
| Completeness gate | 100% (15/15 pts) | Below → efficiency = 0 |
| Time penalty threshold | 45 seconds | Above → lose 3 efficiency points |

### Task Efficiency Baselines

| Task | Expected Requests | Notes |
|------|-------------------|-------|
| T1_single_page | 1 | Single page, minimal requests |
| T2_multi_page | 5 | Multi-page pagination |
| T3_duplicates | 3 | Deduplication task |
| T4_rate_limit_429 | 4 | Includes retry overhead |
| T5_server_error_500 | 4 | Includes retry overhead |
| T6_page_drift | 3 | Page drift handling |
| T7_totals_trap | 8 | Larger dataset with totals |

---

## Anti-patterns (Will Score Poorly)

The following behaviors are explicitly penalized or not rewarded:

### ❌ Log Spam Without Traceability
```text
# BAD: Verbose but useless
Processing... Processing... Processing... Done!

# GOOD: Traceable fields
[task_id=T1] page=1 request=GET /records complete=true
```
*Log length doesn't increase score. Missing traceable fields (task_id, page, request, complete) loses points.*

### ❌ Sacrificing Correctness for Speed
```text
# BAD: Fast but wrong
Requests: 1, Rows: 50 (expected: 800)
```
*Correctness < 70% caps efficiency at 50%. You can't "win" by being fast and wrong.*

### ❌ Incomplete Outputs Claiming Efficiency
```text
# BAD: Missing metadata.json but low request count
Files: [data.jsonl, run.log]  # metadata.json missing
```
*Completeness < 100% → efficiency = 0. Incomplete work gets no efficiency credit.*

### ❌ Hardcoded / Fixture Outputs
```text
# BAD: Same output regardless of task
data.jsonl: [same 100 rows for all tasks]
```
*Query matching and row count checks will fail. Correctness will be near 0.*

### ❌ Ignoring Error Handling
```text
# BAD: No retry evidence for T4/T5
run.log: "Got 429, giving up"
```
*Robustness score = 0 for fault tasks without proper retry/backoff evidence.*

---

### Baseline Performance

| Agent Mode | Performance | Notes |
|------------|-------------|-------|
| Baseline Purple (fixed) | ~85-95%* | Deterministic, no LLM required |
| Baseline Purple (ground truth) | 100% (validation) | Validates judge correctness |

*Environment-dependent; requires robust error handling, retries, and deduplication. Verify via CI artifacts.

The baseline agent demonstrates that high scores are achievable with deterministic logic, proper pagination, and robust error handling—no LLM required.

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
| [Leaderboard](https://github.com/yonghongzhang-io/agentbeats-leaderboard-v2) | Submission tracking and automated assessment |
| [Baseline Purple Agent](file:///Users/sarah/Desktop/Antigravity/TEST/green-comtrade-bench/baseline_purple) | Reference implementation (this repo) |

### AgentBeats URLs

- **Green Agent**: https://agentbeats.dev/yonghongzhang-io/green-comtrade-bench-v2 *(coming soon)*
- **Leaderboard**: https://agentbeats.dev/yonghongzhang-io/agentbeats-leaderboard-v2 *(coming soon)*

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
