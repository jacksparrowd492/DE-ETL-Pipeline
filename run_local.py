"""
run_local.py
------------
A standalone, offline-friendly run of extract -> validate -> transform that
writes the result to data/output.csv.

This is NOT part of the production DAG (fakestore_pipeline.py), which
streams validated products through Kafka into Databricks and needs those
credentials configured. This script exists purely to demonstrate, in one
command and with no external services besides the public FakeStore API,
that the pipeline logic actually processes real data end-to-end:

    python run_local.py

Prints a summary to stdout and writes every transformed row to
data/output.csv.
"""

import csv
import logging
import os
import sys

from extract.extractor import extract_products
from validation.validator import validate_products
from transform.transformer import transform_product

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "output.csv")

CSV_COLUMNS = (
    "product_id", "product_name", "product_category", "price", "price_category",
    "rating_rate", "rating_count", "product_description", "image_url",
    "data_source", "etl_load_timestamp", "rating_category", "product_value_segment",
)


def run():
    logger.info("Extracting products from FakeStore API...")
    raw_products = extract_products()
    logger.info("Extracted %s raw products.", len(raw_products))

    validated = validate_products(raw_products)
    logger.info("Validated %s products (%s dropped).", len(validated), len(raw_products) - len(validated))

    transformed = [transform_product(p, data_source="Local Run") for p in validated]

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in transformed:
            writer.writerow(row)

    logger.info("Wrote %s transformed rows to %s", len(transformed), OUTPUT_FILE)
    print(f"\nDone. {len(transformed)} rows written to {OUTPUT_FILE}")

    return transformed


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.exception("Local run failed: %s", e)
        sys.exit(1)
