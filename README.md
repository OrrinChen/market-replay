# Market Replay Service

Market Replay Service is a pure Go backend for historical market-data replay, validation, asynchronous job control, data-quality reporting, observability, and bounded benchmark reporting. The replay core stays intentionally narrow: parse market-data files, replay events in timestamp order, validate sequence integrity, record replay manifests, and measure replay performance with reproducible local workloads.

This repository is not a live trading system and not an automatic trading engine. The service layers API metadata, Redis control-plane jobs, observability, Kafka-compatible data-plane replay, and a minimal server-rendered dashboard around the pure Go replay core while keeping all claims bounded to historical replay diagnostics.

## Current Status

The full local roadmap demo is implemented and smoke-tested with Docker Compose:

- Pure Go CLI parser/replayer/validator and deterministic workload generator.
- Gin API with Postgres-backed metadata/results and sqlc-backed repository queries.
- Redis/Asynq control-plane queue with worker retry, idempotency, cancellation, checkpoint/resume, and worker metrics.
- Redpanda/Kafka-compatible data-plane replay producer/consumer benchmark path.
- Prometheus and Grafana observability with API and worker scrape targets.
- Minimal server-rendered dashboard mounted by the Go API server.
- Data-governance slice for dataset lineage, replay job manifests, validation-error summaries, and JSON/CSV quality report exports.

Recent verification on 2026-05-21:

```bash
CGO_ENABLED=1 .tools/go/bin/go test -ldflags=-linkmode=external ./...
make GO=.tools/go/bin/go test-kafka
DATABASE_URL='postgres://market:market@127.0.0.1:5432/market_replay?sslmode=disable' \
REDIS_ADDR='127.0.0.1:6379' \
make GO=.tools/go/bin/go test-integration
KAFKA_BROKERS='127.0.0.1:9092' \
make GO=.tools/go/bin/go test-integration-kafka
```

## Benchmark Snapshot

Resume-safe local evidence is captured in [docs/BENCHMARK.md](docs/BENCHMARK.md). Highlights from the latest run:

| Surface | Workload | Result |
| --- | --- | --- |
| Streaming replay | 1GB generated BTCUSDT JSONL, 4,819,950 events | 153,566 events/sec, 15.166 us p95 handler latency, 7.34MB peak Go heap |
| Postgres writes | 10,000 rows | row INSERT 3,022 rows/sec vs COPY 418,109 rows/sec, 138.33x speedup |
| Postgres lookup | 50,000 rows, 1,000 lookups | idempotency lookup p95 5.711ms without index vs 0.569ms indexed, 10.04x p95 improvement |
| Redis/Asynq queue | 1,000 isolated enqueue operations | 125,880 jobs/min, 0.902ms enqueue p95, duplicate task rejected |
| Kafka/Redpanda | 10MB generated BTCUSDT JSONL, 47,922 events | producer 2,810 events/sec, consumer 414,607 events/sec, end-to-end 17.172s |

## Grafana Dashboard

![Grafana dashboard](docs/assets/grafana-dashboard.png)

## Phase 0-1 Scope

- Pure Go streaming parsers for JSONL depth events and CSV aggregate trades.
- Deterministic local replay from files under `testdata/`.
- Validation for malformed rows, missing required fields, timestamp ordering, and update sequence gaps.
- Benchmark harness for local replay throughput, latency, allocation, and memory measurements.
- Small fixtures that can be parsed with standard library JSON and CSV readers.

Out of scope for Phase 0-1:

- Live exchange connectivity.
- Order placement, execution, or trading advice.
- Kafka, Redis, database, object-store, or message-bus integration.
- Python notebooks, frontend dashboards, or multi-service deployment.
- Claims about production trading readiness.

## Quick Start

Requires Go 1.22 or newer.

```bash
make GO=.tools/go/bin/go test
make GO=.tools/go/bin/go validate-fixtures
make GO=.tools/go/bin/go bench
make GO=.tools/go/bin/go build
bin/market-replay replay --file testdata/btcusdt_depth.jsonl --symbol BTCUSDT --speed max
```

If Go is not on `PATH`, pass an explicit binary:

```bash
make GO=/path/to/go test
```

On macOS 26 with the bundled Go 1.22.5 runtime, test binaries may require the external linker:

```bash
CGO_ENABLED=1 .tools/go/bin/go test -ldflags=-linkmode=external ./...
```

## Phase 7-8 Operations Slice

The dashboard is a small Go `net/http` package, not a React application. It accepts `store.Repository` and renders:

- Datasets
- Replay Jobs
- Job Detail
- Validation Errors
- Dataset Lineage
- Quality Report Export
- Metrics Summary
- Benchmark Report

Mounting example:

```go
handler := dashboard.New(repository)
http.Handle("/dashboard/", handler)
```

`cmd/server` mounts the dashboard under `/dashboard`, Prometheus metrics under `/metrics`, pprof under `/debug/pprof`, and enqueues replay jobs through Redis/Asynq when `REDIS_ADDR` is set.

Local infrastructure scaffolding is available for the service topology:

```bash
docker compose up postgres redis redpanda
docker compose --profile app --profile observability up api worker prometheus grafana
docker compose --profile app up api worker
```

In this topology, Postgres is metadata/results storage, Redis is the control-plane queue, and Redpanda provides a Kafka-compatible broker for the data-plane replay CLI. Prometheus scrapes API metrics from `api:8080` and worker metrics from `worker:9090`; Grafana provisions the bundled dashboard. The Docker image builds pure Go binaries for the CLI, API server, worker, and Kafka replay tool.

Governance endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /datasets/:id/lineage` | Dataset to event-file to replay-job lineage with metric/error counts. |
| `GET /replay-jobs/:id/report` | Replay quality report JSON with dataset, event file, manifest, metrics, errors, and error summary. |
| `GET /replay-jobs/:id/report.csv` | CSV export of row-level validation errors for a replay job. |
| `GET /validation-errors/summary` | Validation-error distribution by type, file, symbol, and day. |

## Documentation

- [Benchmark report](docs/BENCHMARK.md)
- [Operations runbook](docs/OPERATIONS.md)
- [Failure modes](docs/FAILURE_MODES.md)
- [Resume notes](docs/RESUME.md)
- [Testing strategy](docs/TESTING.md)

## Event Schemas

### DepthEvent

Represents one order-book depth update from a JSONL source. Each line is one JSON object.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `event_type` | string | yes | Must be `depth`. |
| `symbol` | string | yes | Uppercase market symbol, for example `BTCUSDT`. |
| `event_time` | int64 | yes | Exchange event timestamp in Unix milliseconds. |
| `first_update_id` | int64 | yes | First update id included in this depth event. |
| `final_update_id` | int64 | yes | Final update id included in this depth event. |
| `bids` | array | yes | Array of `[price, quantity]` string pairs. |
| `asks` | array | yes | Array of `[price, quantity]` string pairs. |

Depth sequence validation checks that each event for a symbol starts at the prior event's `final_update_id + 1`, unless it is the first event for that symbol.

### TradeEvent

Represents one aggregate trade from a CSV source.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `event_type` | string | yes | Must be `aggTrade`. |
| `symbol` | string | yes | Uppercase market symbol, for example `SOLUSDT`. |
| `trade_id` | int64 | yes | Aggregate trade id. |
| `price` | string | yes | Decimal string to avoid float drift in parsing tests. |
| `quantity` | string | yes | Decimal string to avoid float drift in parsing tests. |
| `trade_time` | int64 | yes | Exchange trade timestamp in Unix milliseconds. |
| `is_buyer_maker` | bool | yes | Maker side flag from the source row. |

### ReplayJob

Defines one local replay run.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `job_id` | string | yes | Stable caller-provided id for logs and metrics. |
| `dataset_id` | string | yes | Dataset identifier for API/PostgreSQL metadata. |
| `symbol` | string | no | Optional symbol filter. Empty means all symbols. |
| `file_path` | string | yes | Local file to parse and replay. |
| `speed` | string | yes | `max`, `10x`, or `1x`. |
| `status` | string | yes | `pending`, `running`, `completed`, or `failed`. |
| `submitted_at` | timestamp | yes | Job submission time. |
| `started_at` | timestamp | no | Replay start time. |
| `completed_at` | timestamp | no | Replay completion time. |

### ValidationResult

Reports validation output for one parser or replay run.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `rows` | int64 | yes | Input rows processed, including malformed rows. |
| `events` | int64 | yes | Parsed valid events after optional symbol filtering. |
| `malformed_events` | int64 | yes | Rows that could not be parsed or failed required-field validation. |
| `sequence_gaps` | int64 | yes | Detected depth update-id gaps or trade-id gaps. |
| `ordering_failures` | int64 | yes | Per-symbol timestamp or id ordering failures. |
| `symbol_event_counts` | map[string]int64 | yes | Valid event count by symbol. |
| `failures` | array | no | Row-level malformed, sequence, or ordering failures. |
| `started_at` | timestamp | yes | Validation start time. |
| `completed_at` | timestamp | yes | Validation completion time. |

### ReplayMetric

Captures benchmark and replay counters.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `rows` | int64 | yes | Input rows processed, including malformed rows. |
| `events` | int64 | yes | Valid events emitted by the parser. |
| `malformed_events` | int64 | yes | Malformed rows observed during the run. |
| `sequence_gaps` | int64 | yes | Detected sequence gaps. |
| `duration` | duration | yes | Total benchmark duration. |
| `rows_per_second` | float64 | yes | Raw input rows processed per second. |
| `events_per_second` | float64 | yes | Valid replay events emitted per second. |
| `p95_latency` | duration | yes | P95 sampled event handling latency during replay. |
| `peak_alloc_bytes` | uint64 | yes | Peak observed Go heap allocation during the run. |
| `allocs_per_event` | float64 | yes | Allocation count divided by valid events. |
| `workload_file_path` | string | yes | File used for this benchmark row. |

## Fixtures

- `testdata/btcusdt_depth.jsonl`: valid BTCUSDT depth events with contiguous sequence ids.
- `testdata/ethusdt_depth.jsonl`: valid ETHUSDT depth events with contiguous sequence ids.
- `testdata/solusdt_aggtrade.csv`: valid SOLUSDT aggregate trades.
- `testdata/malformed.jsonl`: intentionally invalid JSONL rows for parser validation.
- `testdata/sequence_gap.jsonl`: valid JSONL syntax with exactly one BTCUSDT sequence gap.

## Expected Phase 1 Behavior

The first Go implementation should be able to stream these files without loading the full input into memory, emit typed events, count malformed rows without panics, and report one sequence gap for `sequence_gap.jsonl`.
