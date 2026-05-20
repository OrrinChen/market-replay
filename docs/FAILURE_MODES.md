# Failure Modes

| Failure mode | Detection | Expected behavior | Operator action |
| --- | --- | --- | --- |
| Malformed JSONL or CSV row | `malformed_events` and validation-error rows increase. | Continue streaming and record row-level error. | Inspect source row, regenerate fixture if needed, keep replay result bounded to valid events. |
| Depth sequence gap | `sequence_gaps` increases; error type `sequence_gap`. | Complete replay with integrity warning unless policy escalates later. | Check source file continuity and upstream archive completeness. |
| Timestamp or id ordering issue | Validation result reports ordering failure. | Record validation failure; do not panic. | Sort or repair historical source file before relying on benchmark numbers. |
| Duplicate idempotency key | Repository returns `ErrAlreadyExists`. | Return existing job metadata instead of creating a duplicate. | Reuse the original job id or provide a new idempotency key. |
| Worker crash during replay | Job remains running or checkpoint stops advancing. | Redis/Asynq retries the control-plane task; the worker primes validator state through checkpointed rows, resumes validation after `checkpoint_line`, and marks the job failed or `dlq` after retry exhaustion. | Inspect worker logs, checkpoint, retry count, and last validation error before resubmitting. |
| Redis unavailable | Control-plane queue unavailable. | API should reject or defer new asynchronous jobs. | Restore Redis before submitting more jobs; avoid manual duplicate dispatch. |
| Postgres unavailable | Metadata and result persistence unavailable. | API and worker should fail closed for durable operations. | Restore Postgres; verify migrations and volume health. |
| Redpanda/Kafka unavailable | Data-plane stream unavailable or producer/consumer lag stops advancing. | Kafka-backed replay commands should pause or fail; local file replay and metadata APIs remain independent. | Restore broker and confirm topic health before resuming stream replay. |
| Prometheus unavailable | Metrics scrape gap. | Replay work can continue, but observability history has holes. | Restore Prometheus and annotate benchmark reports with missing scrape window. |
| Dashboard template missing | Dashboard returns render or startup error. | API should surface dashboard failure without affecting replay core. | Verify `web/templates` is included in the runtime image or working directory. |

These failure modes are operational diagnostics for historical market-data replay. They are not trading risk controls and do not authorize live or automatic trading behavior.
