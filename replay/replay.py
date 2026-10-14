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

logger = logging.getLogger(__name__)

FAILED_FILE = "failed_records/failed_products.json"


def replay_failed_records() -> dict:
    """
    Reads failed records and republishes them to Kafka. Returns
    {"replayed": n, "remaining": m}.
    """

    if not os.path.exists(FAILED_FILE):
        logger.info("No failed record file found.")
        return {"replayed": 0, "remaining": 0}

    with open(FAILED_FILE, "r") as file:
        products = json.load(file)

    if not products:
        logger.info("No failed records to replay.")
        return {"replayed": 0, "remaining": 0}

    logger.info("Replaying %s failed records...", len(products))

    remaining = []

    for product in products:
        success = send_product(product)

        if not success:
            remaining.append(product)

    with open(FAILED_FILE, "w") as file:
        json.dump(remaining, file, indent=4)

    logger.info("Replay Complete | Remaining Failed Records: %s", len(remaining))

    return {"replayed": len(products) - len(remaining), "remaining": len(remaining)}


if __name__ == "__main__":
    from observability import setup_logging

    setup_logging("replay")
    replay_failed_records()
