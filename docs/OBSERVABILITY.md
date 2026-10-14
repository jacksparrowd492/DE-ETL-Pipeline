# Observability: logging, alerts, metrics and lineage

Phase II adds four things to every part of the pipeline: structured logging, Slack/email
alerts for failed tasks, metrics with latency and failure alerts, and data lineage. They
live in two packages, [observability/](../observability/) and [governance/](../governance/),
and every entry point uses them the same way:

| Entry point | Logs (`logs/…`) | Alerts on | Lineage job(s) |
|---|---|---|---|
| `fakestore_pipeline.py` (Airflow) | `airflow_tasks.{log,jsonl}` + Airflow task log | task failed for good (`on_failure_callback`), retry (`on_retry_callback`), SLA breach, data quality | `fakestore_pipeline.*` |
| `run_pipeline_no_airflow.py` | `pipeline.{log,jsonl}` | failed step (exit code 1), SLA breach, data quality | `fakestore_pipeline.*` |
| `python -m kafka1.consumer` | `consumer.{log,jsonl}` | consumer stopped on an error, Kafka → Databricks latency | `kafka_consumer.*` |
| `streaming/spark_delta.py` | `streaming.{log,jsonl}` | query died, p95 ingestion latency, reject ratio, slow trigger | `streaming.kafka_to_delta` |
| `maintenance/dags/lakehouse_maintenance.py` | `airflow_maintenance.{log,jsonl}` | task failed for good / retry | `maintenance.*` |
| `run_local.py` | `run_local.{log,jsonl}` | failed step, SLA breach | – |
| `streamlit_app.py` | `streamlit.{log,jsonl}` | – (shows them: **Monitoring** page) | – |

## Logging

`observability.setup_logging(component)` configures the root logger once per process:

* console in text (or JSON with `LOG_FORMAT=json`, used in the cloud),
* `logs/<component>.log` (text) and `logs/<component>.jsonl` (one JSON object per line),
  each rotated at 10 MB × 5.

Every line carries the **run id** and **task** (`run_context()`): the Airflow `run_id`, or a
generated `20261014T055719Z-bc00b814` style id for the other entry points. The same id tags the
metrics, alerts and OpenLineage events of that run, so one filter pulls everything for one run:

```bash
grep '"run_id": "20261014T055719Z-bc00b814"' logs/*.jsonl logs/lineage/*.jsonl
```

Library modules only call `logging.getLogger(__name__)`; the per-module `logging.basicConfig()`
calls were removed, so handlers are no longer installed as a side effect of an import.

## Alerts

`observability.send_alert(title, message, severity, details, dedup_key)` sends to every
configured channel and **never raises** (a broken Slack webhook must not hide the real error):

| Channel | Configure in `.env` |
|---|---|
| Slack | `SLACK_WEBHOOK_URL` (Slack → Apps → Incoming Webhooks) |
| Email | `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `ALERT_EMAIL_TO` (Gmail: `smtp.gmail.com`, 587, an App Password) |
| Amazon SNS | `ALERT_SNS_TOPIC_ARN` (cloud) |
| File | always: `logs/alerts.jsonl` |

`ALERT_MIN_SEVERITY` (default `warning`) filters what leaves the machine, and a `dedup_key` sends
a repeating alert at most once per `ALERT_COOLDOWN_SECONDS` (300), so a consumer that fails on
every message sends one alert, not thousands.

What raises an alert:

| Alert | Severity | Where |
|---|---|---|
| Task failed (no retries left) | critical | Airflow `on_failure_callback`, or `track_task` outside Airflow |
| Task failed, will retry | warning | Airflow `on_retry_callback` |
| Task slower than its latency SLA (`SLA_SECONDS_<TASK>`) | warning | `track_task` (defaults in `pipeline_tasks.SLA_SECONDS`) |
| Validation dropped > 20 % of products | warning | `pipeline_tasks.validate` |
| Some / all Kafka sends failed | error / task fails | `pipeline_tasks.send_to_kafka` |
| Replay queue not empty after replay | warning | `pipeline_tasks.replay_failed` |
| Kafka → Databricks latency > `LATENCY_ALERT_SECONDS` (300) | warning | `kafka1/consumer.py` |
| Consumer stopped on an error | critical | `kafka1/consumer.py` |
| Stream p95 ingestion latency > 120 s, reject ratio > 25 %, trigger > 60 s | warning | `observability/stream_health.py` |
| Streaming query died | critical | `streaming/spark_delta.py` |

Under Airflow the tasks run with `alert_on_failure=False`, so a failure is alerted once, by the
callback, which knows whether a retry is still coming.

## Metrics

`observability.record(name, value, labels)` appends to `logs/metrics.jsonl` and keeps
`logs/metrics_<component>.prom` in Prometheus text format (point node_exporter's textfile
collector at `logs/`), or pushes to a Pushgateway (`PUSHGATEWAY_URL`). In the cloud
(`METRICS_TO_LOG=true`) each sample is also a JSON log line, which CloudWatch metric filters
turn into metrics and alarms ([deploy/terraform/aws/monitoring.tf](../deploy/terraform/aws/monitoring.tf)).

| Metric (`etl_` prefix in Prometheus) | Labels | From |
|---|---|---|
| `task_duration_seconds`, `task_success`, `task_records`, `task_sla_breach` | task, kind | every pipeline task |
| `pipeline_run_duration_seconds`, `pipeline_run_success` | pipeline | `run_pipeline_no_airflow.py` |
| `validation_reject_ratio`, `replay_queue_size` | task | `pipeline_tasks.py` |
| `kafka_end_to_end_latency_seconds`, `consumer_messages`, `consumer_message_seconds` | topic, status | consumer |
| `stream_batch_valid / rejected / inserted / duplicates`, `stream_reject_ratio`, `stream_ingest_latency_p95_seconds` | table | Spark `foreachBatch` |
| `stream_input_rows(_per_second)`, `stream_processed_rows_per_second`, `stream_trigger_seconds` | query | `StreamingQueryListener` |

## Lineage

See [LINEAGE.md](LINEAGE.md) (generated). Each run emits OpenLineage `START` and
`COMPLETE`/`FAIL` events with schema and column-lineage facets to
`logs/lineage/openlineage.jsonl`, and to `OPENLINEAGE_URL` when set. Marquez's quickstart
uses Docker; without it, build Marquez from source (a Java jar plus a local PostgreSQL), or
point `OPENLINEAGE_URL` at any OpenLineage-compatible backend (DataHub, OpenMetadata, a hosted
Marquez). `python -m governance.lineage emit` pushes the whole static graph at once.

## Try it

```bash
# 1. a run that works, then one that fails (alerts + exit code 1)
python run_pipeline_no_airflow.py
FAKESTORE_API_URL=http://127.0.0.1:1/products python run_pipeline_no_airflow.py; echo "exit $?"

# 2. what was recorded
tail -n 3 logs/alerts.jsonl
grep task_duration logs/metrics.jsonl | tail -n 4
python -m governance.lineage validate

# 3. the dashboard's Monitoring page
streamlit run streamlit_app.py      # -> Monitoring
```
