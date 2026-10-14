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
from observability import run_context, setup_logging, track_task
from observability.logging_config import new_run_id
from validation.validator import validate_products
from transform.transformer import transform_product

logger = logging.getLogger(__name__)

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "output.csv")

CSV_COLUMNS = (
    "product_id",
    "product_name",
    "product_category",
    "price",
    "price_category",
    "rating_rate",
    "rating_count",
    "product_description",
    "image_url",
    "data_source",
    "etl_load_timestamp",
    "rating_category",
    "product_value_segment",
)


def run():
    with run_context(run_id=new_run_id()):
        with track_task("extract", sla_seconds=30) as t:
            logger.info("Extracting products from FakeStore API...")
            raw_products = extract_products()
            logger.info("Extracted %s raw products.", len(raw_products))
            t.count("records_out", len(raw_products))

        with track_task("validate", sla_seconds=10) as t:
            validated = validate_products(raw_products)
            logger.info(
                "Validated %s products (%s dropped).",
                len(validated),
                len(raw_products) - len(validated),
            )
            t.count("records_out", len(validated))

        with track_task("transform_to_csv", sla_seconds=10) as t:
            transformed = [transform_product(p, data_source="Local Run") for p in validated]

            os.makedirs(OUTPUT_DIR, exist_ok=True)

            with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                for row in transformed:
                    writer.writerow(row)
            t.count("records_out", len(transformed))

    logger.info("Wrote %s transformed rows to %s", len(transformed), OUTPUT_FILE)
    print(f"\nDone. {len(transformed)} rows written to {OUTPUT_FILE}")

    return transformed


if __name__ == "__main__":
    setup_logging("run_local")
    try:
        run()
    except Exception as e:
        logger.exception("Local run failed: %s", e)
        sys.exit(1)
