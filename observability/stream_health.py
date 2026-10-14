"""
stream_health.py
----------------
Health checks for the Spark Structured Streaming job (streaming/spark_delta.py),
kept free of any Spark import so they are unit-tested without a JVM.

    observe_batch(...)     called by foreachBatch once per micro-batch
    observe_progress(...)  called by the StreamingQueryListener per trigger
    observe_termination()  called when the query stops

Thresholds (environment)
    STREAM_LATENCY_ALERT_SECONDS   120  p95 of ingested_at - event_time in a batch
    STREAM_REJECT_ALERT_RATIO      0.25 rejected / (valid + rejected) in a batch
    STREAM_REJECT_MIN_MESSAGES     20   ignore the ratio for smaller batches (noise)
    STREAM_TRIGGER_ALERT_SECONDS   60   a trigger slower than this = falling behind
"""

from __future__ import annotations

import os

from observability.alerts import send_alert
from observability.metrics import record


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name) or default)


def observe_batch(
    batch_id: int,
    table: str,
    n_valid: int,
    n_rejected: int,
    inserted: int | None = None,
    latency_p95: float | None = None,
) -> list:
    """Record one micro-batch's metrics; return the titles of the alerts it raised."""

    labels = {"table": table}
    raised = []
    total = n_valid + n_rejected

    record("stream_batch_valid", n_valid, labels)
    record("stream_batch_rejected", n_rejected, labels)
    if inserted is not None:
        record("stream_batch_inserted", inserted, labels)
        record("stream_batch_duplicates", max(n_valid - inserted, 0), labels)

    if total >= _env_float("STREAM_REJECT_MIN_MESSAGES", 20):
        ratio = n_rejected / total
        record("stream_reject_ratio", ratio, labels)
        if ratio > _env_float("STREAM_REJECT_ALERT_RATIO", 0.25):
            title = "Streaming data quality: high reject ratio"
            send_alert(
                title,
                f"Batch {batch_id}: {n_rejected} of {total} messages ({ratio:.0%}) failed "
                "validation and went to the quarantine table.",
                severity="warning",
                details={"batch_id": batch_id, "table": table, "rejected": n_rejected},
                dedup_key="stream:reject_ratio",
            )
            raised.append(title)

    if latency_p95 is not None:
        record("stream_ingest_latency_p95_seconds", latency_p95, labels, unit="s")
        threshold = _env_float("STREAM_LATENCY_ALERT_SECONDS", 120)
        if latency_p95 > threshold:
            title = "Streaming latency: events land late in the lakehouse"
            send_alert(
                title,
                f"Batch {batch_id}: p95 ingestion latency {latency_p95:.0f}s "
                f"(threshold {threshold:.0f}s). The stream is behind Kafka.",
                severity="warning",
                details={"batch_id": batch_id, "table": table, "p95_s": f"{latency_p95:.1f}"},
                dedup_key="stream:latency",
            )
            raised.append(title)

    return raised


def observe_progress(
    query: str,
    batch_id: int,
    num_input_rows: int,
    input_rows_per_second: float | None,
    processed_rows_per_second: float | None,
    trigger_ms: float | None,
) -> list:
    labels = {"query": query}
    raised = []

    record("stream_input_rows", num_input_rows, labels)
    if input_rows_per_second is not None:
        record("stream_input_rows_per_second", input_rows_per_second, labels)
    if processed_rows_per_second is not None:
        record("stream_processed_rows_per_second", processed_rows_per_second, labels)
    if trigger_ms is not None:
        record("stream_trigger_seconds", trigger_ms / 1000, labels, unit="s")
        threshold = _env_float("STREAM_TRIGGER_ALERT_SECONDS", 60)
        if trigger_ms / 1000 > threshold:
            title = "Streaming latency: micro-batch slower than its trigger budget"
            send_alert(
                title,
                f"Batch {batch_id} of {query} took {trigger_ms / 1000:.0f}s "
                f"(threshold {threshold:.0f}s). Input is arriving faster than it is processed.",
                severity="warning",
                details={"query": query, "batch_id": batch_id, "rows": num_input_rows},
                dedup_key="stream:trigger",
            )
            raised.append(title)

    return raised


def observe_termination(query: str, exception: str | None) -> None:
    record("stream_query_up", 0, {"query": query})
    if exception:
        send_alert(
            f"Streaming query {query} stopped on an error",
            exception[:2000],
            severity="critical",
            details={"query": query},
        )
