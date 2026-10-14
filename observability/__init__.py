"""
observability
-------------
Phase II: logging, alerting and metrics shared by every part of the pipeline
(the Airflow DAGs, run_pipeline_no_airflow.py, the Kafka consumer, the Spark
streaming job and the Delta maintenance jobs).

    logging_config  setup_logging(): console + rotating text log + JSON-lines log,
                    every record tagged with the current run_id / task
    alerts          send_alert(): Slack incoming webhook + SMTP email, always
                    also written to logs/alerts.jsonl; Airflow failure/retry callbacks
    metrics         record(): JSON-lines + Prometheus textfile metrics;
                    track_task(): duration, status, latency-SLA and failure alerts

Everything is configured from environment variables (see .env.example), so the
same code runs locally, in CI and on a cloud deployment.
"""

from __future__ import annotations

from observability.alerts import send_alert
from observability.logging_config import get_run_context, run_context, setup_logging
from observability.metrics import record, track_task

__all__ = [
    "get_run_context",
    "record",
    "run_context",
    "send_alert",
    "setup_logging",
    "track_task",
]
