"""
pipeline_tasks.py
-----------------
The four steps of the FakeStore batch pipeline, instrumented once and shared
by both orchestrators:

    fakestore_pipeline.py        (Airflow DAG)
    run_pipeline_no_airflow.py   (same sequence without Airflow)

Each step wraps the production function (extract_products, validate_products,
send_products, replay_failed_records) with

    observability.track_task   duration / success / record-count metrics and a
                               latency-SLA alert (SLA_SECONDS_<TASK> overrides)
    governance.lineage_run     OpenLineage START + COMPLETE/FAIL events
    data-quality checks        alerts for dropped records and lost Kafka sends

Failure alerts: run_pipeline_no_airflow.py lets track_task alert on a failed
step. Under Airflow, alert_on_failure=False and the DAG's on_failure_callback /
on_retry_callback alert instead, because only Airflow knows whether a retry is
still coming.
"""

import logging
import os

from extract.extractor import extract_products
from governance.lineage import lineage_run
from kafka1.producer import send_products
from observability import record, send_alert, track_task
from replay.replay import replay_failed_records
from validation.validator import validate_products

logger = logging.getLogger(__name__)

# Latency SLA per step, in seconds. Generous for a 20-product batch: a breach
# means something is slow (API, broker, network), not that the data grew.
SLA_SECONDS = {
    "extract": 30,
    "validate": 10,
    "send_to_kafka": 120,
    "replay_failed": 120,
}


def _reject_ratio_threshold() -> float:
    return float(os.environ.get("VALIDATION_REJECT_ALERT_RATIO", "0.2"))


def extract(alert_on_failure: bool = True) -> list:
    with (
        lineage_run("fakestore_pipeline.extract"),
        track_task("extract", SLA_SECONDS["extract"], alert_on_failure) as t,
    ):
        products = extract_products()
        t.count("records_out", len(products))
        if not products:
            raise ValueError("FakeStore API returned no products")
        return products


def validate(products: list, alert_on_failure: bool = True) -> list:
    with (
        lineage_run("fakestore_pipeline.validate"),
        track_task("validate", SLA_SECONDS["validate"], alert_on_failure) as t,
    ):
        validated = validate_products(products)
        rejected = len(products) - len(validated)
        t.count("records_in", len(products))
        t.count("records_out", len(validated))
        t.count("records_rejected", rejected)

        ratio = rejected / len(products) if products else 0.0
        record("validation_reject_ratio", ratio, {"task": "validate"})
        if products and ratio > _reject_ratio_threshold():
            send_alert(
                "Data quality: validation rejected too many products",
                f"{rejected} of {len(products)} products ({ratio:.0%}) failed validation.",
                severity="warning",
                details={"rejected": rejected, "total": len(products)},
                dedup_key="validate:reject_ratio",
            )
        if not validated:
            raise ValueError(f"all {len(products)} products failed validation")
        return validated


def send_to_kafka(validated: list, alert_on_failure: bool = True) -> dict:
    with (
        lineage_run("fakestore_pipeline.send_to_kafka"),
        track_task("send_to_kafka", SLA_SECONDS["send_to_kafka"], alert_on_failure) as t,
    ):
        result = send_products(validated)
        t.count("records_out", result["success"])
        t.count("records_failed", result["failed"])

        if result["failed"] and not result["success"]:
            # nothing reached Kafka: fail the task so Airflow retries it
            raise RuntimeError(f"all {result['failed']} Kafka sends failed")
        if result["failed"]:
            send_alert(
                "Kafka publish: some products were not sent",
                f"{result['failed']} of {len(validated)} products failed to publish to Kafka "
                "after the producer's retries; they are not in the topic.",
                severity="error",
                details=result,
            )
        return result


def replay_failed(alert_on_failure: bool = True) -> dict:
    with (
        lineage_run("fakestore_pipeline.replay_failed"),
        track_task("replay_failed", SLA_SECONDS["replay_failed"], alert_on_failure) as t,
    ):
        result = replay_failed_records()
        t.count("records_out", result["replayed"])
        t.count("records_remaining", result["remaining"])
        record("replay_queue_size", result["remaining"], {"task": "replay_failed"})

        if result["remaining"]:
            send_alert(
                "Replay queue not empty",
                f"{result['remaining']} failed Databricks writes could not be republished; "
                "they stay in failed_records/failed_products.json.",
                severity="warning",
                details=result,
                dedup_key="replay:remaining",
            )
        return result
