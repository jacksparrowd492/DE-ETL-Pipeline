"""
replay.py
---------
Replays failed records stored locally.

Usage:
    python replay/replay.py
"""

import json
import logging
import os

from kafka1.producer import send_product

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

FAILED_FILE = "failed_records/failed_products.json"


def replay_failed_records():
    """
    Reads failed records and republishes them to Kafka.
    """

    if not os.path.exists(FAILED_FILE):
        logger.info("No failed record file found.")
        return

    with open(FAILED_FILE, "r") as file:
        products = json.load(file)

    if not products:
        logger.info("No failed records to replay.")
        return

    logger.info("Replaying %s failed records...", len(products))

    remaining = []

    for product in products:

        success = send_product(product)

        if not success:
            remaining.append(product)

    with open(FAILED_FILE, "w") as file:
        json.dump(remaining, file, indent=4)

    logger.info(
        "Replay Complete | Remaining Failed Records: %s",
        len(remaining)
    )


if __name__ == "__main__":
    replay_failed_records()