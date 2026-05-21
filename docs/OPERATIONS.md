# Operations Runbook

This runbook covers local service scaffolding for replay metadata, Redis control-plane queues, Kafka-compatible data-plane replay, data-quality reports, dashboard pages, and observability. The executable surface includes the pure Go CLI, API server, worker, Kafka replay CLI, and dashboard mounted by `cmd/server`.

## One-Command Startup

Start the infrastructure dependencies:

```bash
docker compose up postgres redis redpanda
```

Start the full demo topology, including API, worker, metrics scraping, and dashboards:

```bash
docker compose --profile app --profile observability up api worker prometheus grafana
```

Build the Go image and start only the API/worker profile:

```bash
docker compose --profile app up api worker
```

## Architecture

```mermaid
flowchart LR
    Files["Historical fixture files"] --> Parser["Go parser and validator"]
    Parser --> Replay["Replay loop"]
    Replay --> Metrics["Replay metrics"]
    API["API service (metadata/results)"] --> Postgres[("Postgres metadata and results")]
    API --> Redis[("Redis control-plane queue")]
    Worker["Worker service"] --> Redis
    Worker --> Postgres
    Worker --> Manifest["Manifest: hash, rows, checkpoint"]
    Manifest --> Postgres
    KafkaReplay["Kafka replay CLI"] --> Kafka["Redpanda/Kafka broker"]
    Metrics --> Prometheus["Prometheus"]
    Prometheus --> Grafana["Grafana"]
    API --> Dashboard["Server-rendered dashboard"]
```

## Service Roles

| Service | Role | Persistence |
| --- | --- | --- |
| `api` | HTTP API service, dashboard mount, metrics, and pprof endpoint. | Postgres for metadata/results. |
| `worker` | Replay worker consuming Redis/Asynq control-plane jobs. | Postgres for results and Redis for control state. |
| `postgres` | Durable dataset, event-file, replay-job, validation-error, metric, file-stat, and manifest metadata. | `postgres-data` volume. |
| `redis` | Control-plane queue and job coordination. | `redis-data` volume with append-only file enabled. |
| `redpanda` | Kafka-compatible broker for market event data-plane replay. | `redpanda-data` volume. |
| `prometheus` | Metrics scraping and local retention. | `prometheus-data` volume. |
| `grafana` | Local dashboarding against Prometheus. | `grafana-data` volume. |

Redpanda exposes two Kafka listeners:

| Listener | Intended caller | Address |
| --- | --- | --- |
| `internal` | Compose services such as API/worker. | `redpanda:9092` |
| `external` | Host-side CLI commands. | `localhost:9092` |

## Dashboard Mounting

The dashboard package is intentionally small and server-rendered. `cmd/server` mounts it at `/dashboard`:

```go
handler := dashboard.New(repository)
http.Handle("/dashboard/", handler)
```

It accepts `store.Repository`, so tests can use `store.NewMemoryRepository()` and production wiring can use the durable repository implementation when mounted by the API service.

Runtime endpoints:

| Endpoint | Purpose |
| --- | --- |
| `/healthz` | API health check. |
| `/dashboard` | Server-rendered dashboard entrypoint. |
| `/datasets/:id/lineage` | Dataset lineage JSON for dataset, event files, replay jobs, metrics, and errors. |
| `/replay-jobs/:id/report` | Replay quality report JSON with manifest, metrics, errors, and summary. |
| `/replay-jobs/:id/report.csv` | CSV export of validation errors for a replay job. |
| `/validation-errors/summary` | Validation error distribution grouped by type, file, symbol, and day. |
| `/metrics` | API Prometheus metrics on the API service; worker metrics are exposed when `WORKER_METRICS_ADDR` is set. |
| `/debug/pprof/` | Go pprof handlers. |

## Routine Checks

| Check | Command |
| --- | --- |
| Unit tests | `make GO=.tools/go/bin/go test` |
| Fixture validation | `make GO=.tools/go/bin/go validate-fixtures` |
| Benchmark smoke | `make GO=.tools/go/bin/go bench` |
| Postgres benchmark | `DATABASE_URL='postgres://market:market@127.0.0.1:5432/market_replay?sslmode=disable' bin/market-replay db-bench --rows 50000 --lookups 1000 --insert-rows 10000` |
| Redis queue benchmark | `REDIS_ADDR='127.0.0.1:6379' bin/market-replay queue-bench --jobs 1000` |
| Docker image build | `docker build -t market-replay-service:local .` |
| Infrastructure health | `docker compose ps` |
| API/worker startup | `docker compose --profile app up api worker` |
| Kafka replay build | `.tools/go/bin/go build -tags kafka ./cmd/kafka-replay` |
| Kafka host CLI smoke | `./bin/kafka-replay produce --brokers localhost:9092 --file testdata/btcusdt_depth.jsonl` |
| Kafka benchmark | `./bin/kafka-replay bench --brokers localhost:9092 --file tmp/btcusdt_10mb.jsonl` |

## Recovery

- If Postgres is unhealthy, inspect `docker compose logs postgres`, confirm credentials match `DATABASE_URL`, and recreate only the Postgres volume when data loss is acceptable.
- If Redis is unhealthy, inspect `docker compose logs redis`; replay job dispatch should pause rather than duplicate work until control-plane state is understood.
- If Redpanda is unhealthy, treat Kafka-backed data-plane replay as unavailable. Historical file replay and metadata operations can remain available.
- If dashboard pages error, verify the mounted repository can list datasets/jobs and that `web/templates` is present in the runtime working tree.

Replay checkpoint resume is line-based. On retry, the worker scans checkpointed rows to prime per-symbol validator state, then records metrics and validation errors only for rows after `checkpoint_line`. It does not currently persist byte offsets, so very large files still pay scan cost before the logical resume point.

Replay manifests are written after successful file processing and before completion metadata is returned. They record input hash, rows, bytes, resume line, final checkpoint, duration, malformed count, sequence-gap count, and validation-error count so a quality report can be tied back to the exact replay input.
