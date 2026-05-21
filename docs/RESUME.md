# Resume Notes

Bounded bullets that are safe for resumes or project summaries:

- Built a pure Go historical market-data replay core with streaming JSONL/CSV parsers, deterministic fixtures, validation for malformed rows and sequence gaps, and local throughput/memory benchmark reporting.
- Implemented dataset lineage and replay quality reports that link dataset, event file hash/row count, replay job manifest, validation errors, and metrics; exposed JSON/CSV report exports and validation-error summaries by type, file, symbol, and day.
- Added a minimal server-rendered dashboard package over the repository interface for datasets, replay jobs, job detail, validation errors, metrics summaries, and benchmark reports, testable with an in-memory store and mounted by the API service.
- Implemented a Docker Compose operations slice with API/worker roles, Postgres metadata/results, Redis control-plane queueing, Redpanda/Kafka-compatible data-plane replay, Prometheus, and Grafana.
- Captured local benchmark evidence: 1GB streaming replay at 153,566 events/sec with 7.34MB peak Go heap; Postgres `COPY` insert 138.33x faster than row insert on 10,000 rows; indexed idempotency lookup p95 improved 10.04x on a 50,000-row benchmark table; Redis/Asynq enqueue reached 125,880 jobs/min with duplicate rejection; Redpanda baseline produced/consumed 47,922 replay events from a 10MB workload.
- Kept the system explicitly scoped to historical replay diagnostics and benchmark instrumentation; no live trading, order execution, automatic trading, or production exchange-connectivity claims.

Resume wording for the governance slice:

```text
Implemented data lineage and replay quality reporting for historical market-data replay, recording dataset, event file hash/row count, replay job manifest, validation errors, and metric summaries; supported checkpoint resume, idempotent job submission, duplicate rejection, DLQ/archive status tracking, and JSON/CSV quality report exports.
```

Avoid stronger claims unless later phases add evidence:

- Do not claim production trading readiness.
- Do not claim live exchange ingestion.
- Do not claim production Kafka scale; the captured Kafka table is a local Redpanda baseline with synchronous per-event production.
- Do not claim automated trading or decisioning.
