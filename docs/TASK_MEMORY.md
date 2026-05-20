# Task Memory

## Phase 0-1 Status

Phase 0 initializes documentation and deterministic market-data fixtures for a pure Go Market Replay Service V1. Phase 1 now has a first pure Go streaming implementation for parser, validation, replay speed control, CLI smoke commands, and benchmark metrics.

Completed Phase 0 assets:

- `README.md` defines the Phase 0-1 scope, non-goals, event schemas, fixture inventory, and expected Phase 1 behavior.
- `docs/ARCHITECTURE.md` defines the local single-process parser, validator, replayer, and metrics architecture.
- `docs/BENCHMARK_PLAN.md` defines 10MB, 100MB, and later 1GB benchmark workloads and required metrics.
- `testdata/btcusdt_depth.jsonl` contains valid BTCUSDT depth events with contiguous sequence ids.
- `testdata/ethusdt_depth.jsonl` contains valid ETHUSDT depth events with contiguous sequence ids.
- `testdata/solusdt_aggtrade.csv` contains valid SOLUSDT aggregate trade rows.
- `testdata/malformed.jsonl` contains intentionally bad JSONL rows for malformed-row handling.
- `testdata/sequence_gap.jsonl` contains valid JSONL syntax and exactly one expected sequence gap.

Completed Phase 1 assets:

- `cmd/market-replay` provides `validate`, `replay`, and `bench` commands.
- `cmd/market-replay bench-matrix` compares fixture vs generated workloads, JSONL vs CSV, and repeated-run medians without Docker.
- `internal/parser` streams JSONL and CSV with row-level malformed records.
- `internal/validate` counts malformed rows, sequence gaps, ordering failures, and per-symbol events.
- `internal/replay` supports `max`, `10x`, and `1x` speed control.
- `internal/bench` reports rows/sec, events/sec, sampled p95 latency, peak allocation, and allocations/event.
- `internal/bench` provides a deterministic local matrix runner that generates scaled JSONL/CSV workloads under `tmp/bench-matrix/`.
- `cmd/market-replay db-bench` provides a Compose-backed Postgres benchmark for row insert vs `COPY` insert and idempotency-key lookup before/after indexing.
- `cmd/market-replay queue-bench` provides a Redis/Asynq enqueue benchmark with duplicate task rejection verification.
- `cmd/kafka-replay bench` provides a Redpanda-backed producer/consumer baseline using isolated benchmark topics.
- `Makefile` provides `test`, `validate-fixtures`, `bench`, `bench-matrix`, and `build` targets.

## Scope Guardrails

Keep Phase 1 implementation bounded to:

- Pure Go parsers.
- Local file replay.
- Validation results.
- Replay metrics.
- Benchmark harnesses.

Do not claim Phase 1 alone includes:

- Live trading or automatic trading.
- Kafka, Redis, database, Python, or frontend systems.
- Production exchange connectivity.
- Production-grade distributed replay.
- Database batch-size, database index, Redis queue, or Kafka broker optimization benchmark evidence; those are tracked as later service-layer evidence.

Phase 7-8 extends the repo with a dashboard and operations/documentation slice. It still must not be described as live trading, automatic trading, production exchange connectivity, or production-grade distributed replay.

Completed Phase 7-8 assets:

- `internal/dashboard` provides minimal server-rendered `net/http` pages for datasets, replay jobs, job detail, validation errors, metrics summary, and benchmark report.
- `web/templates` contains plain Go HTML templates; no React or large frontend bundle is introduced.
- `Dockerfile` builds pure Go binaries for `cmd/market-replay`, `cmd/server`, and `cmd/worker` into a small runtime image.
- `docker-compose.yml` documents the target local topology: API and worker roles, Postgres metadata/results, Redis control-plane queue, Redpanda/Kafka-compatible data-plane broker, Prometheus, and Grafana.
- `docs/BENCHMARK.md`, `docs/OPERATIONS.md`, `docs/FAILURE_MODES.md`, `docs/RESUME.md`, and `docs/TESTING.md` define the benchmark command, runbook, failure modes, bounded resume bullets, and verification strategy.

Supervisor integration updates:

- `cmd/server` now mounts `internal/dashboard` at `/dashboard`, Prometheus metrics at `/metrics`, and pprof at `/debug/pprof`.
- `cmd/server` enqueues replay jobs through Redis/Asynq when `REDIS_ADDR` is set; without Redis it falls back to metadata-only job creation.
- `cmd/server` and `cmd/worker` run an idempotent local schema bootstrap so `docker compose --profile app up` can start against a fresh Postgres volume.
- `cmd/worker` exposes Prometheus worker metrics when `WORKER_METRICS_ADDR` is set; compose enables it internally for Prometheus scraping.
- `internal/store/PostgresRepository` uses sqlc-generated query methods for live Postgres operations while preserving the project repository interface.
- `docker-compose.yml` now mounts Prometheus and Grafana provisioning files.
- Prometheus scrapes the API at `api:8080` and the worker at `worker:9090`; Grafana provisions dashboards from `/var/lib/grafana/dashboards`.
- Redpanda exposes separate Kafka listeners for compose-internal clients (`redpanda:9092`) and host-side CLI clients (`localhost:9092`).
- Worker retry now performs line-based logical resume: checkpointed rows prime validator state, while metrics and validation errors are emitted only for rows after `checkpoint_line`.

## Expected Fixture Results

| Fixture | Expected valid events | Expected malformed rows | Expected sequence gaps |
| --- | ---: | ---: | ---: |
| `testdata/btcusdt_depth.jsonl` | 4 | 0 | 0 |
| `testdata/ethusdt_depth.jsonl` | 4 | 0 | 0 |
| `testdata/solusdt_aggtrade.csv` | 5 | 0 | n/a |
| `testdata/malformed.jsonl` | 1 | 4 | n/a |
| `testdata/sequence_gap.jsonl` | 4 | 0 | 1 |

For `sequence_gap.jsonl`, the single gap is between `final_update_id = 3002` and the next `first_update_id = 3005`.

## Verification Snapshot

Executed locally with a project-local Go 1.22.5 runtime under `.tools/go`:

- `.tools/go/bin/go test ./...`
- `PATH="$PWD/.tools/go/bin:$PATH" make validate-fixtures`
- `PATH="$PWD/.tools/go/bin:$PATH" make bench`
- `PATH="$PWD/.tools/go/bin:$PATH" go run ./cmd/market-replay replay --file testdata/btcusdt_depth.jsonl --symbol BTCUSDT --speed max`

Benchmark refresh executed locally on 2026-05-20 with `CGO_ENABLED=0`, `go version go1.22.5 darwin/arm64`, and `Darwin arm64`:

- `mkdir -p tmp`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay generate-workload --source testdata/btcusdt_depth.jsonl --output tmp/btcusdt_10mb.jsonl --bytes 10485760`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay generate-workload --source testdata/btcusdt_depth.jsonl --output tmp/btcusdt_100mb.jsonl --bytes 104857600`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/btcusdt_depth.jsonl --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/ethusdt_depth.jsonl --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/sequence_gap.jsonl --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/solusdt_aggtrade.csv --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/malformed.jsonl --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file tmp/btcusdt_10mb.jsonl --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file tmp/btcusdt_100mb.jsonl --json`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench-matrix --runs 3 --bytes 65536 --output-dir tmp/bench-matrix`
- `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench-matrix --runs 3 --bytes 65536 --output-dir tmp/bench-matrix --json`

Captured local benchmark results are in `docs/BENCHMARK.md`. The generated 10MB, 100MB, 1GB, and matrix workloads stayed under `tmp/`; they are deterministic local artifacts, not committed fixtures, and not production-scale or live exchange evidence.

Runtime smoke on 2026-05-20:

- `docker compose config` passed with Docker CLI/Compose under `/opt/homebrew/bin`.
- Full Compose and Docker image build initially hit Docker Hub/GCR EOF while pulling larger images.
- The missing images were later pulled successfully through retries/fallback tags: `redpandadata/redpanda:v24.2.7` was tagged as `docker.redpanda.com/redpandadata/redpanda:v24.2.7`, `quay.io/prometheus/prometheus:v2.54.1` was tagged as `prom/prometheus:v2.54.1`, and `redis:7` was tagged as `redis:7-alpine` for local Compose compatibility.
- `market-replay-service:local` built successfully after Dockerfile support for `TARGETOS`/`TARGETARCH` and configurable `GOPROXY`/`GOSUMDB`.
- A local fallback harness using the real API router and real worker handler with memory storage created a dataset, event file, replay job, completed the job, returned metrics with `rows=4`, `events=4`, `sequence_gaps=1`, returned one `sequence_gap` error, and returned the same job id for duplicate idempotency-key submission.
- Real local Postgres/Redis integration passed using Homebrew services on temporary ports: `DATABASE_URL='postgres://market@127.0.0.1:55432/market_replay?sslmode=disable' REDIS_ADDR='127.0.0.1:6380' make GO=.tools/go/bin/go test-integration`.
- Full Compose smoke passed with API, worker, Postgres, Redis, Redpanda, Prometheus, and Grafana running.
- Compose-backed integration passed: `DATABASE_URL='postgres://market:market@127.0.0.1:5432/market_replay?sslmode=disable' REDIS_ADDR='127.0.0.1:6379' make GO=.tools/go/bin/go test-integration`.
- Redpanda-backed Kafka integration passed: `KAFKA_BROKERS='127.0.0.1:9092' make GO=.tools/go/bin/go test-integration-kafka`.
- 1GB streaming replay benchmark passed: `tmp/btcusdt_1gb.jsonl` had 1,073,741,853 bytes, 4,819,950 events, 153,566 events/sec, 15.166 us p95 handler latency, and 7.34MB peak Go heap.
- Compose-backed Postgres benchmark passed: row insert 3,022 rows/sec vs `COPY` 418,109 rows/sec; idempotency-key lookup p95 improved from 5.711ms without an index to 0.569ms with an index.
- Redis/Asynq queue benchmark passed: 1,000 isolated enqueue operations at 125,880 jobs/min, 0.902ms enqueue p95, and duplicate task rejection count of 1.
- Redpanda-backed Kafka benchmark passed on `tmp/btcusdt_10mb.jsonl`: 47,922 events produced and consumed; producer 2,810 events/sec, consumer 414,607 events/sec, end-to-end 17.172s.
- Grafana dashboard screenshot captured at `docs/assets/grafana-dashboard.png` after a real replay job generated worker/API metrics.

Final verification after supervisor integration:

- `make GO=.tools/go/bin/go test`
- `make GO=.tools/go/bin/go test-kafka`
- `make GO=.tools/go/bin/go build`
- `make GO=.tools/go/bin/go build-kafka`
- `make GO=.tools/go/bin/go validate-fixtures`
- `make GO=.tools/go/bin/go bench`
- `make GO=.tools/go/bin/go bench-matrix`
- `.tools/bin/sqlc generate`
- `.tools/bin/goose -dir db/migrations validate`
- `PATH="/opt/homebrew/bin:$PATH" docker compose config`
- `DATABASE_URL='postgres://market@127.0.0.1:55432/market_replay?sslmode=disable' REDIS_ADDR='127.0.0.1:6380' make GO=.tools/go/bin/go test-integration`
- `python3 -m json.tool deploy/grafana/dashboards/market-replay-overview.json`
- `ruby -e 'require "yaml"; ...'` over compose, Prometheus, and Grafana provisioning YAML.
- `git diff --check`

## Next Phase Candidate

Next work should harden the completed roadmap into release artifacts once Docker Hub pulls are stable:

- Re-run generated workload benchmarks after material parser/replay changes and update `docs/BENCHMARK.md` with the new environment and commands.
