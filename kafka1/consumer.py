"""
consumer.py
-----------
Consumes validated products from Kafka,
transforms them,
loads them into the staging table,
and merges them into the final warehouse.
"""

import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

from transform.transformer import transform_product
from load.databricks_loader import (
    create_staging_table,
    load_to_staging
)
from staging.staging_manager import (
    create_products_table,
    promote_to_warehouse
)

# ----------------------------------------------------
# Environment
# ----------------------------------------------------

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
TOPIC = os.getenv("KAFKA_TOPIC")

# ----------------------------------------------------
# Logger
# ----------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------
# Kafka Consumer
# ----------------------------------------------------

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="etl_consumer_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# ----------------------------------------------------
# Main Consumer
# ----------------------------------------------------

def consume():

    logger.info("Preparing Databricks tables...")

    create_staging_table()
    create_products_table()

    logger.info("Consumer Started...")
    logger.info("Waiting for Kafka messages...")

    try:

        for message in consumer:

            product = message.value

            logger.info(
                "Received Product ID=%s",
                product["id"]
            )

            transformed = transform_product(product)

            load_to_staging(transformed)

            promote_to_warehouse(transformed)

            logger.info(
                "Product %s processed successfully.",
                transformed["product_id"]
            )

    except KeyboardInterrupt:

        logger.info("Consumer stopped.")

    except Exception as e:

        logger.exception(e)

    finally:

        consumer.close()

        logger.info("Consumer closed.")


if __name__ == "__main__":
    consume()