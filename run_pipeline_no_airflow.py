"""
run_pipeline_no_airflow.py
---------------------------
Runs the exact same task sequence as the Airflow DAG (fakestore_pipeline.py):

    extract -> validate -> send_to_kafka -> replay_failed

by calling the identical production functions the DAG calls
(extract_products, validate_products, send_products,
replay_failed_records) directly, in order, instead of through Airflow's
scheduler/XCom.

This exists because Airflow can't run on native Windows (its CLI imports
os.register_at_fork, which doesn't exist there - confirmed by trying), and
this machine's WSL2 has no internet access to install Airflow there either
(a network policy blocks WSL2's adapter). This is a faithful stand-in for
demonstrating the pipeline end-to-end against a real running Kafka broker,
not a simulation - every function here is the real one the DAG would call.

    python run_pipeline_no_airflow.py
"""

import logging

from extract.extractor import extract_products
from validation.validator import validate_products
from kafka1.producer import send_products
from replay.replay import replay_failed_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def run():
    logger.info("=== extract ===")
    products = extract_products()

    logger.info("=== validate ===")
    validated = validate_products(products)

    logger.info("=== send_to_kafka ===")
    send_products(validated)

    logger.info("=== replay_failed ===")
    replay_failed_records()

    logger.info("Pipeline run complete.")


if __name__ == "__main__":
    run()
