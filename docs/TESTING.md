# Testing Strategy

## Test Layers

| Layer | Command | Purpose |
| --- | --- | --- |
| Unit tests | `make GO=.tools/go/bin/go test` | Parser, validator, replay, benchmark, store, API, worker, Kafka interfaces, observability, governance reports, and dashboard package behavior. |
| Dashboard rendering | `CGO_ENABLED=0 .tools/go/bin/go test ./internal/dashboard` | HTML handlers render repository-backed pages, lineage, manifests, error summaries, and report links using the memory store. |
| Fixture validation smoke | `make GO=.tools/go/bin/go validate-fixtures` | Confirms committed fixtures retain expected malformed/gap behavior. |
| Benchmark smoke | `make GO=.tools/go/bin/go bench` | Confirms benchmark command emits required metrics for representative fixtures. |
| Postgres + Redis integration | `make GO=.tools/go/bin/go test-integration` | Runs env-gated integration scaffolding against real Postgres and Redis/Asynq when `DATABASE_URL` and/or `REDIS_ADDR` are set. |
| Kafka/Redpanda integration | `make GO=.tools/go/bin/go test-integration-kafka` | Runs env-gated `integration,kafka` tests against a real Kafka-compatible broker when `KAFKA_BROKERS` is set. |
| Docker build | `docker build -t market-replay-service:local .` | Confirms the pure Go CLI image builds. |
| Compose infrastructure | `docker compose up postgres redis redpanda` | Confirms local metadata, control-plane, and Kafka-compatible data-plane dependencies start. |
| Full demo smoke | `docker compose --profile app --profile observability up api worker prometheus grafana` | Confirms API, worker, dependencies, Prometheus, and Grafana can run together when image pulls are available. |
| API/worker fallback smoke | `go test ./internal/api ./internal/worker` plus a local harness or `httptest` flow | Confirms dataset/job/metrics/errors/idempotency behavior without Docker. |

## Dashboard Test Expectations

The dashboard package should remain server-rendered and repository-driven. Tests should seed `store.NewMemoryRepository()` with datasets, event files, jobs, manifests, metrics, and validation errors, then assert visible HTML content through `httptest`.

## Integration Test Scaffolding

Ordinary `make GO=.tools/go/bin/go test` must not require Docker, image pulls, or live services. Phase 8 service-dependency checks are isolated behind Go build tags and environment variables:

- Postgres: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/market_replay?sslmode=disable make GO=.tools/go/bin/go test-integration`
- Redis/Asynq: `REDIS_ADDR=localhost:6379 make GO=.tools/go/bin/go test-integration`
- Postgres + Redis together: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/market_replay?sslmode=disable REDIS_ADDR=localhost:6379 make GO=.tools/go/bin/go test-integration`
- Kafka/Redpanda: `KAFKA_BROKERS=localhost:9092 make GO=.tools/go/bin/go test-integration-kafka`

The Postgres test creates and drops an isolated temporary schema, calls `EnsurePostgresSchema`, verifies the expected repository tables exist, and exercises dataset/event-file/replay-job create/list/get plus idempotent replay-job creation, manifest persistence, lineage/report builders, and completion metrics/errors.

The Redis/Asynq test enqueues a replay task into a unique queue with deterministic TaskID/Unique options and verifies the second enqueue is rejected by Asynq duplicate protection. It does not require a worker process.

The Kafka test is built only with `-tags "integration kafka"` and verifies the `KgoProducer` publish path against the configured broker. If the relevant env var is unset, each integration test skips with a clear message.

On 2026-05-20, real Postgres and Redis integration tests were run against temporary Homebrew-backed local services:

```bash
DATABASE_URL='postgres://market@127.0.0.1:55432/market_replay?sslmode=disable' \
REDIS_ADDR='127.0.0.1:6380' \
make GO=.tools/go/bin/go test-integration
```

Later on 2026-05-20, the full Compose topology was started successfully after pulling/tagging local image fallbacks:

```bash
docker compose --profile app --profile observability up -d
DATABASE_URL='postgres://market:market@127.0.0.1:5432/market_replay?sslmode=disable' \
REDIS_ADDR='127.0.0.1:6379' \
make GO=.tools/go/bin/go test-integration
KAFKA_BROKERS='127.0.0.1:9092' \
make GO=.tools/go/bin/go test-integration-kafka
```

The Compose-backed Postgres/Redis integration and Redpanda/Kafka integration passed. The Kafka integration test explicitly creates the required topic before producing a record to avoid broker auto-create races.

## Verification Rules

- Use `CGO_ENABLED=0` or the Makefile defaults for ordinary Go test commands. On macOS 26 with the bundled Go 1.22.5 runtime, use `CGO_ENABLED=1 .tools/go/bin/go test -ldflags=-linkmode=external ./...` to avoid local `dyld` test-binary loading failures.
- Keep generated workloads and benchmark outputs out of git unless a later phase explicitly adds reproducible artifacts.
- Do not treat benchmark timing as a fixed pass/fail threshold until a stable machine baseline is documented.
- Keep live trading, automatic trading, and exchange connectivity out of test names and assertions.
- If Docker Hub pulls fail, record that as an environment limit and keep local Go tests, Compose config validation, and API/worker fallback smoke separate from full Compose readiness.
