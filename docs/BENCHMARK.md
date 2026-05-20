# Benchmark Report

This report is the resume-facing benchmark surface for the local replay core. It is bounded to deterministic historical file replay and does not measure live exchange connectivity, order execution, live trading, or automatic trading.

## Local Results Snapshot

Captured on 2026-05-20 with `CGO_ENABLED=0`, project-local Go, and deterministic local files only.

| Field | Value |
| --- | --- |
| Go version | `go version go1.22.5 darwin/arm64` |
| OS/arch | `Darwin arm64` |
| Workload location | Generated files under `tmp/`; not committed fixtures |
| Boundary | Local historical file parser/replay benchmark; not production scale, not exchange latency, not live trading readiness |

Generated workload commands:

```bash
mkdir -p tmp
CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay generate-workload \
  --source testdata/btcusdt_depth.jsonl \
  --output tmp/btcusdt_10mb.jsonl \
  --bytes 10485760
CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay generate-workload \
  --source testdata/btcusdt_depth.jsonl \
  --output tmp/btcusdt_100mb.jsonl \
  --bytes 104857600
```

Benchmark commands used the same shape for every row:

```bash
CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file <path> --json
```

| Input | Size bytes | Rows | Events | Malformed | Gaps | Rows/sec | Events/sec | p95 latency | Peak alloc MB | Allocs/event | Command |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `testdata/btcusdt_depth.jsonl` | 868 | 4 | 4 | 0 | 0 | 6,549 | 6,549 | 15.291 us | 0.18 | 106.50 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/btcusdt_depth.jsonl --json` |
| `testdata/ethusdt_depth.jsonl` | 852 | 4 | 4 | 0 | 0 | 4,577 | 4,577 | 15.625 us | 0.18 | 106.50 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/ethusdt_depth.jsonl --json` |
| `testdata/sequence_gap.jsonl` | 700 | 4 | 4 | 0 | 1 | 2,913 | 2,913 | 13.459 us | 0.18 | 90.75 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/sequence_gap.jsonl --json` |
| `testdata/solusdt_aggtrade.csv` | 347 | 5 | 5 | 0 | 0 | 47,431 | 47,431 | 3.667 us | 0.11 | 5.60 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/solusdt_aggtrade.csv --json` |
| `testdata/malformed.jsonl` | 681 | 5 | 1 | 4 | 0 | 7,417 | 1,483 | 557.125 us | 0.18 | 369.00 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file testdata/malformed.jsonl --json` |
| `tmp/btcusdt_10mb.jsonl` | 10,485,919 | 47,922 | 47,922 | 0 | 0 | 61,693 | 61,693 | 14.542 us | 3.59 | 45.01 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file tmp/btcusdt_10mb.jsonl --json` |
| `tmp/btcusdt_100mb.jsonl` | 104,857,720 | 474,958 | 474,958 | 0 | 0 | 69,238 | 69,238 | 17.167 us | 3.88 | 45.00 | `CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench --file tmp/btcusdt_100mb.jsonl --json` |
| `tmp/btcusdt_1gb.jsonl` | 1,073,741,853 | 4,819,950 | 4,819,950 | 0 | 0 | 153,566 | 153,566 | 15.166 us | 7.34 | 45.00 | `/usr/bin/time -l bin/market-replay bench --file tmp/btcusdt_1gb.jsonl --json` |

Notes:

- The 10MB generated workload wrote 10,485,919 bytes and 47,922 rows.
- The 100MB generated workload wrote 104,857,720 bytes and 474,958 rows after the 10MB run completed cleanly.
- The 1GB generated workload wrote 1,073,741,853 bytes and 4,819,950 rows. It completed in 31.386s with a 7.34MB peak Go heap and a 14,189,048-byte peak memory footprint from `/usr/bin/time -l`.
- Fixture rows are intentionally tiny and should be read as validation smoke checks, not throughput comparisons.
- Peak alloc MB is `peak_alloc_bytes / 1,048,576`; p95 latency is reported from benchmark nanoseconds as microseconds.

## Service Benchmark Snapshot

These measurements were captured on 2026-05-20 with full Docker Compose running locally:

```bash
docker compose --profile app --profile observability up -d
DATABASE_URL='postgres://market:market@127.0.0.1:5432/market_replay?sslmode=disable' \
  bin/market-replay db-bench --rows 50000 --lookups 1000 --insert-rows 10000 --json
REDIS_ADDR='127.0.0.1:6379' \
  bin/market-replay queue-bench --jobs 1000 --json
bin/kafka-replay bench --brokers 127.0.0.1:9092 --file tmp/btcusdt_10mb.jsonl --json --timeout 5m
```

### Postgres Insert And Lookup

The DB benchmark uses temporary benchmark tables in the configured Postgres database. It compares row-by-row insert against `COPY`, then compares idempotency-key lookup before and after creating a B-tree index.

| Rows | Lookups | Insert rows | Row insert rows/sec | COPY rows/sec | COPY speedup | No-index lookup p95 | Indexed lookup p95 | P95 improvement |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50,000 | 1,000 | 10,000 | 3,022 | 418,109 | 138.33x | 5.711 ms | 0.569 ms | 10.04x |

### Redis/Asynq Queue Control Plane

The queue benchmark enqueues replay tasks into an isolated temporary Asynq queue, measures enqueue latency, then submits the same task twice with a deterministic `TaskID`/`Unique` option to verify duplicate rejection.

| Jobs | Jobs/min | Enqueue p50 | Enqueue p95 | Duplicate rejected |
| ---: | ---: | ---: | ---: | --- |
| 1,000 | 125,880 | 0.306 ms | 0.902 ms | yes |

### Kafka/Redpanda Produce And Consume

The Kafka benchmark creates isolated one-partition topics, publishes a deterministic historical file, then consumes the produced event count with a unique consumer group. The producer path is synchronous per event, so this is a conservative data-plane baseline rather than a batched producer ceiling.

| File | Events | Producer events/sec | Consumer events/sec | End-to-end duration | Observed consumer lag p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `tmp/btcusdt_10mb.jsonl` | 47,922 | 2,810 | 414,607 | 17.172 s | 45,524 events |

## One Command

```bash
.tools/go/bin/go run ./cmd/market-replay bench --file testdata/btcusdt_depth.jsonl
```

JSON output for automation:

```bash
.tools/go/bin/go run ./cmd/market-replay bench --file testdata/btcusdt_depth.jsonl --json
```

Generate deterministic local workloads without committing large files:

```bash
mkdir -p tmp
.tools/go/bin/go run ./cmd/market-replay generate-workload \
  --source testdata/btcusdt_depth.jsonl \
  --output tmp/btcusdt_10mb.jsonl \
  --bytes 10485760
.tools/go/bin/go run ./cmd/market-replay bench --file tmp/btcusdt_10mb.jsonl --json
```

The workload generator advances depth update ids, aggregate trade ids, and timestamps across repeated fixture cycles so valid source fixtures remain valid after expansion.

## Local Benchmark Matrix

Phase 5 benchmark reporting now has a local deterministic matrix command. It compares only surfaces implemented by the current CLI: committed fixtures versus generated workloads, JSONL versus CSV parsers, and median throughput across repeated local runs. Generated matrix files are written under `tmp/bench-matrix/`, which is ignored by git.

```bash
CGO_ENABLED=0 .tools/go/bin/go run ./cmd/market-replay bench-matrix \
  --runs 3 \
  --bytes 65536 \
  --output-dir tmp/bench-matrix
```

Equivalent Make target:

```bash
make GO=.tools/go/bin/go bench-matrix
```

Captured on 2026-05-20 with `CGO_ENABLED=0`, project-local Go, and generated files under `tmp/bench-matrix/`:

| Workload | Mode | Format | Bytes | Runs | Rows | Events | Median rows/sec | Median events/sec | Median p95 latency | Median peak alloc MB | Malformed | Gaps |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fixture-jsonl-btcusdt-depth` | fixture | JSONL | 868 | 3 | 4 | 4 | 176,468 | 176,468 | 4.000 us | 0.33 | 0 | 0 |
| `fixture-csv-solusdt-aggtrade` | fixture | CSV | 347 | 3 | 5 | 5 | 470,588 | 470,588 | 1.084 us | 0.34 | 0 | 0 |
| `generated-jsonl-btcusdt-depth` | generated | JSONL | 65,751 | 3 | 303 | 303 | 156,125 | 156,125 | 8.291 us | 2.54 | 0 | 0 |
| `generated-csv-solusdt-aggtrade` | generated | CSV | 65,582 | 3 | 1,171 | 1,171 | 1,175,900 | 1,175,900 | 0.917 us | 1.49 | 0 | 0 |

Read the fixture rows as parser/validator smoke checks. The generated rows are still local deterministic replay workloads, not DB throughput, Kafka throughput, production scale, or live exchange latency.

## Manual Benchmark Matrix

| Workload | Command | Expected validation shape |
| --- | --- | --- |
| Valid BTCUSDT depth fixture | `.tools/go/bin/go run ./cmd/market-replay bench --file testdata/btcusdt_depth.jsonl` | zero malformed rows, zero sequence gaps |
| Valid ETHUSDT depth fixture | `.tools/go/bin/go run ./cmd/market-replay bench --file testdata/ethusdt_depth.jsonl` | zero malformed rows, zero sequence gaps |
| Sequence-gap depth fixture | `.tools/go/bin/go run ./cmd/market-replay bench --file testdata/sequence_gap.jsonl` | zero malformed rows, one known sequence gap |
| SOLUSDT aggregate trades | `.tools/go/bin/go run ./cmd/market-replay bench --file testdata/solusdt_aggtrade.csv` | zero malformed rows |
| Generated BTCUSDT depth | `.tools/go/bin/go run ./cmd/market-replay generate-workload --source testdata/btcusdt_depth.jsonl --output tmp/btcusdt_10mb.jsonl --bytes 10485760` then bench output | zero malformed rows, zero sequence gaps |

## Metrics

| Metric | Meaning | Resume-safe interpretation |
| --- | --- | --- |
| `rows` | Input rows processed, including malformed rows. | Fixture and generated-workload scale indicator. |
| `events` | Valid replay events emitted after parsing/filtering. | Historical replay throughput denominator. |
| `malformed_events` | Rows rejected by parser or required-field validation. | Data-quality signal, not a trading signal. |
| `sequence_gaps` | Detected update-id continuity gaps. | Market-data integrity diagnostic. |
| `duration` | End-to-end local replay benchmark duration. | Machine-dependent runtime measurement. |
| `rows_per_second` | Input rows processed per second. | Local Go parser/replay throughput. |
| `events_per_second` | Valid events emitted per second. | Local replay throughput. |
| `p95_latency` | Sampled per-event handling latency. | Local processing latency, not exchange latency. |
| `peak_alloc_bytes` | Peak Go heap allocation observed during the run. | Memory profile for streaming replay. |
| `allocs_per_event` | Allocation count divided by valid emitted events. | Efficiency diagnostic. |

## Reporting Rules

- Report the exact command, Go version, OS, CPU, input file, and file size with every benchmark table.
- Prefer median throughput across at least three runs when comparing implementation changes.
- Do not describe fixture replay as production scale, live trading readiness, or automatic trading capability.
- Keep generated 10MB/100MB/1GB workloads outside git unless a later phase explicitly adds compact reproducible fixtures.
- Report DB, Redis, and Kafka numbers only from the dedicated `db-bench`, `queue-bench`, and `kafka-replay bench` commands, and include the row/job/event counts used for each run.
- Completed service integration tests can be reported separately from benchmark throughput; do not mix Postgres/Redis correctness smoke results with parser/replay throughput metrics.
