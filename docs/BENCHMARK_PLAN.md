# Benchmark Plan

The benchmark plan separates completed local replay evidence from service-dependency benchmark evidence. Current completed measurements cover pure Go file parsing, validation, replay, workload generation, Postgres insert/lookup benchmarks, Redis/Asynq enqueue benchmarks, and Kafka-compatible producer/consumer baseline benchmarks. They do not benchmark Python pipelines, browser dashboards, live exchange feeds, order execution, or trading decisions.

## Objectives

- Measure local file parsing and replay throughput.
- Verify memory remains bounded during streaming reads.
- Quantify validation behavior for malformed rows and sequence gaps.
- Establish a repeatable baseline before adding larger integrations in later phases.

## Workloads

| Workload | Phase | Source | Purpose |
| --- | --- | --- | --- |
| 10MB | Phase 1 | Deterministic expansion of `testdata/` rows | Fast local smoke benchmark for parser, validator, and replay loop. |
| 100MB | Phase 1 | Deterministic expansion of `testdata/` rows | Regression workload for throughput, p95 latency, memory, and allocations. |
| 1GB | Completed stress run | Deterministic expansion of `testdata/` rows | Streaming memory proof for large historical replay. |

Generated benchmark files should preserve known validation properties:

- Valid depth streams have zero expected sequence gaps.
- `sequence_gap.jsonl`-derived streams have a known number of injected gaps.
- Malformed streams have a known malformed-row count.
- Trade CSV streams retain a valid header and deterministic row order.

## Required Metrics

Each benchmark run should report:

| Metric | Description |
| --- | --- |
| `rows_per_sec` | Input rows processed per second, including malformed rows. |
| `events_per_sec` | Valid events emitted per second. |
| `p95_latency_ms` | P95 per-event replay handling latency in milliseconds. |
| `peak_memory_bytes` | Peak memory observed during the run. |
| `gap_detection_accuracy` | Detected expected gaps divided by known expected gaps. Use `1.0` when no expected gaps and no detected gaps. |
| `malformed_count` | Number of malformed rows observed. |
| `allocations_per_event` | Total allocations divided by valid emitted events. |

## Benchmark Matrix

### Completed Local Replay Matrix

The implemented matrix command is:

```bash
.tools/go/bin/go run ./cmd/market-replay bench-matrix --runs 3 --bytes 65536 --output-dir tmp/bench-matrix
```

It currently covers:

| Dimension | Implemented comparison |
| --- | --- |
| Workload source | committed fixture vs deterministic generated workload |
| Parser format | JSONL depth fixture vs CSV aggregate-trade fixture |
| Repetition | median row selected from repeated local runs |
| Artifact location | generated files under `tmp/bench-matrix/`, ignored by git |

### Planned Larger Local File Matrix

| Dataset | Size | Expected malformed | Expected gaps | Required report fields |
| --- | --- | ---: | ---: | --- |
| BTCUSDT depth | 10MB | 0 | 0 | rows/sec, events/sec, p95 latency, peak memory, allocations/event |
| BTCUSDT depth | 100MB | 0 | 0 | rows/sec, events/sec, p95 latency, peak memory, allocations/event |
| ETHUSDT depth | 10MB | 0 | 0 | rows/sec, events/sec, p95 latency, peak memory, allocations/event |
| SOLUSDT aggTrade | 10MB | 0 | n/a | rows/sec, events/sec, p95 latency, peak memory, allocations/event |
| Malformed JSONL | 10MB | known count | n/a | rows/sec, malformed count, peak memory |
| Sequence gap JSONL | 10MB | 0 | known count | rows/sec, events/sec, gap detection accuracy, peak memory |
| Mixed valid depth | 100MB | 0 | 0 | rows/sec, events/sec, p95 latency, peak memory, allocations/event |
| Depth stress | 1GB | 0 | known by generator | rows/sec, events/sec, p95 latency, peak memory, gap detection accuracy, malformed count, allocations/event |

### Completed Service Benchmark Matrix

These Phase 5-adjacent dimensions now have concrete commands and captured results in `docs/BENCHMARK.md`:

| Dimension | Status |
| --- | --- |
| Database batch size | Implemented as row insert vs Postgres `COPY` in `bin/market-replay db-bench`. |
| Database index strategy | Implemented as idempotency-key lookup before/after B-tree index in `bin/market-replay db-bench`. |
| Postgres write/read throughput | Captured in `docs/BENCHMARK.md` from Compose-backed Postgres. |
| Kafka producer/consumer throughput | Implemented in `bin/kafka-replay bench` with isolated Redpanda topics. |
| Redis queue throughput | Implemented as isolated Asynq enqueue and duplicate-rejection benchmark in `bin/market-replay queue-bench`. |
| Docker Compose end-to-end throughput | Covered as a smoke/demo topology, not a throughput benchmark. |

## Acceptance Targets

Phase 1 should define the measurement harness before setting aggressive performance targets. Initial acceptance is functional and reproducible:

- 10MB, 100MB, and 1GB workloads complete without loading the full file into memory.
- Metrics include all required fields.
- Valid fixtures report `malformed_count = 0`.
- `malformed.jsonl` reports the expected malformed count.
- `sequence_gap.jsonl` reports exactly one sequence gap when replayed once.
- Benchmark runs record Go version, OS, CPU model when available, input file size, and command used.

Later phases can add threshold gates after baseline results are collected on a stable machine.

## Reproducibility Notes

- Generate scaled workloads from committed fixtures with a deterministic seed or simple row repetition.
- Store generated 10MB and 100MB files outside git if they are too large for source control.
- Keep generated row counts and expected validation counts in benchmark output.
- Run each benchmark at least three times and report median throughput plus p95 latency from the selected run.
