"""
consumer.py
-----------
Consumes validated products from Kafka,
transforms them,
loads them into the staging table,
and merges them into the final warehouse.

Observability (Phase II)
    metrics   consumer_messages (status=ok|failed) and
              kafka_end_to_end_latency_seconds = load time - the producer's
              ingestion_time, per message (logs/metrics.jsonl)
    alerts    warning when that latency exceeds LATENCY_ALERT_SECONDS (300),
              at most once per ALERT_COOLDOWN_SECONDS; critical when the
              consumer stops on an error
    lineage   OpenLineage START when consuming starts, COMPLETE on a clean
              stop, FAIL on an error, for both jobs this process runs
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer

from config import kafka_security_kwargs
from governance.lineage import emit, run_event
from load.databricks_loader import create_staging_table, load_to_staging
from observability import record, run_context, send_alert, setup_logging
from observability.cloud_secrets import load_secret_env
from observability.logging_config import new_run_id
from staging.staging_manager import create_products_table, promote_to_warehouse
from transform.transformer import transform_product

# ----------------------------------------------------
# Environment
# ----------------------------------------------------

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
TOPIC = os.getenv("KAFKA_TOPIC")

# ----------------------------------------------------
# Logger
# ----------------------------------------------------

logger = logging.getLogger(__name__)

# ----------------------------------------------------
# Kafka Consumer (lazy)
# ----------------------------------------------------
#
# Built on first use rather than at import time, so importing this module
# (e.g. from tests) never opens a real connection to a broker. Call
# get_consumer() instead of touching a module-level instance.

_consumer = None


def get_consumer() -> KafkaConsumer:
    global _consumer

    if _consumer is None:
        _consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="etl_consumer_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            **kafka_security_kwargs(),
        )

    return _consumer


LINEAGE_JOBS = ("kafka_consumer.load_bronze_silver", "kafka_consumer.promote_to_warehouse")


def latency_alert_seconds() -> float:
    return float(os.environ.get("LATENCY_ALERT_SECONDS", "300"))


def end_to_end_latency(product: dict) -> float | None:
    """Seconds from the producer's ingestion_time (naive UTC ISO string) to now."""
    raw = product.get("ingestion_time")
    if not raw:
        return None
    try:
        sent = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if sent.tzinfo is None:
        sent = sent.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - sent).total_seconds()


def check_latency(product: dict) -> float | None:
    latency = end_to_end_latency(product)
    if latency is None:
        return None
    record("kafka_end_to_end_latency_seconds", latency, {"topic": TOPIC or "-"}, unit="s")
    threshold = latency_alert_seconds()
    if latency > threshold:
        send_alert(
            "Pipeline latency: Kafka -> Databricks is behind",
            f"Product {product.get('id')} was loaded {latency:.0f}s after it was produced "
            f"(threshold {threshold:.0f}s). The consumer is lagging or was down.",
            severity="warning",
            details={
                "product_id": product.get("id"),
                "latency_s": f"{latency:.1f}",
                "topic": TOPIC,
            },
            dedup_key="consumer:latency",
        )
    return latency


# ----------------------------------------------------
# Main Consumer
# ----------------------------------------------------


def consume(consumer=None):
    """
    Runs the consume loop against `consumer` (any iterable of objects with a
    `.value` dict, e.g. real kafka.consumer.fetcher.ConsumerRecord or a
    stand-in used in tests), defaulting to the real Kafka consumer.
    """

    consumer = consumer if consumer is not None else get_consumer()

    with run_context(run_id=new_run_id(), task="consume"):
        logger.info("Preparing Databricks tables...")

        create_staging_table()
        create_products_table()

        for job in LINEAGE_JOBS:
            emit(run_event(job, "START"))

        logger.info("Consumer Started...")
        logger.info("Waiting for Kafka messages...")

        processed = 0
        error = None

        try:
            for message in consumer:
                product = message.value

                logger.info("Received Product ID=%s", product["id"])

                start = time.perf_counter()
                try:
                    transformed = transform_product(product)

                    load_to_staging(transformed)

                    promote_to_warehouse(transformed)
                except Exception:
                    record("consumer_messages", 1, {"status": "failed"})
                    raise

                processed += 1
                record("consumer_messages", 1, {"status": "ok"})
                record("consumer_message_seconds", time.perf_counter() - start, unit="s")
                check_latency(product)

                logger.info("Product %s processed successfully.", transformed["product_id"])

        except KeyboardInterrupt:
            logger.info("Consumer stopped.")

        except Exception as e:
            error = e
            logger.exception(e)
            send_alert(
                "Kafka consumer stopped on an error",
                f"{type(e).__name__}: {e}. The failed product (if it reached the load step) is "
                "in failed_records/ for replay; restart the consumer once the cause is fixed.",
                severity="critical",
                details={"topic": TOPIC, "processed_before_error": processed},
            )

        finally:
            consumer.close()

            for job in LINEAGE_JOBS:
                emit(run_event(job, "FAIL" if error else "COMPLETE", error=error))

            logger.info("Consumer closed. Processed %s message(s).", processed)


if __name__ == "__main__":
    setup_logging("consumer")
    load_secret_env()  # cloud host: Databricks/Slack/SMTP secrets (no-op locally)
    consume()
