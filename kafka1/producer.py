"""
producer.py
-----------
Kafka Producer for sending validated FakeStore products.

Features
--------
✔ JSON serialization
✔ Automatic retries
✔ Acknowledgement from all brokers
✔ Batch ID generation
✔ Processing timestamp
✔ Product validation
✔ Logging
"""

import json
import logging
import os
import uuid
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --------------------------------------------------
# Load Environment Variables
# --------------------------------------------------

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
TOPIC = os.getenv("KAFKA_TOPIC")

# --------------------------------------------------
# Configure Logger
# --------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger("KafkaProducer")

# --------------------------------------------------
# Create Kafka Producer (lazy)
# --------------------------------------------------
#
# Built on first use rather than at import time, so importing this module
# (e.g. from tests, or from replay.py) never opens a real connection to a
# broker. Call get_producer() instead of touching a module-level instance.

_producer = None


def get_producer() -> KafkaProducer:
    global _producer

    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            retries=5,
            acks="all",
        )

    return _producer


# --------------------------------------------------
# Validate Product
# --------------------------------------------------


def validate_product(product: dict) -> bool:
    """
    Performs basic validation before sending to Kafka.
    """

    required_fields = ["id", "title", "price", "category"]

    for field in required_fields:
        if field not in product:
            logger.error("Validation failed. Missing field: %s", field)
            return False

    return True


# --------------------------------------------------
# Add Metadata
# --------------------------------------------------


def enrich_product(product: dict) -> dict:
    """
    Adds ETL metadata to the product.
    """

    enriched = product.copy()

    enriched["batch_id"] = str(uuid.uuid4())

    enriched["ingestion_time"] = datetime.utcnow().isoformat()

    return enriched


# --------------------------------------------------
# Send One Product
# --------------------------------------------------


def send_product(product: dict) -> bool:

    if not validate_product(product):
        return False

    product = enrich_product(product)

    try:
        future = get_producer().send(TOPIC, value=product)

        metadata = future.get(timeout=10)

        logger.info(
            "Sent Product ID=%s | Partition=%s | Offset=%s",
            product["id"],
            metadata.partition,
            metadata.offset,
        )

        return True

    except KafkaError as e:
        logger.error("Kafka Error: %s", e)

        return False

    except Exception:
        logger.exception("Unexpected Error while sending message")

        return False


# --------------------------------------------------
# Send Multiple Products
# --------------------------------------------------


def send_products(products):

    success = 0

    failed = 0

    for product in products:
        if send_product(product):
            success += 1
        else:
            failed += 1

    get_producer().flush()

    logger.info("Batch Completed | Success=%s | Failed=%s", success, failed)


# --------------------------------------------------
# Close Producer
# --------------------------------------------------


def close_producer():

    if _producer is not None:
        _producer.flush()
        _producer.close()

    logger.info("Kafka Producer Closed")


# --------------------------------------------------
# Test
# --------------------------------------------------

if __name__ == "__main__":
    sample = {"id": 1, "title": "Laptop", "price": 599.99, "category": "electronics"}

    send_product(sample)

    close_producer()
