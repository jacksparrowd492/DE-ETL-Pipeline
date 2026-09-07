"""
databricks_loader.py
--------------------
Loads transformed products into fakestore_catalog.etl_project's bronze and
silver tables.

Features
--------
- Creates bronze/silver tables if they do not exist (idempotent — the real
  tables already exist in Databricks, this is just for portability)
- Inserts transformed products into bronze, then silver
- Saves failed records for replay
- Proper exception handling
- Automatic connection cleanup
"""

import json
import logging
import os
from datetime import datetime, timezone

from Data_Connection import get_connection
from config import table

# --------------------------------------------------------
# Configure Logger
# --------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------
# Failed Records Configuration
# --------------------------------------------------------

FAILED_DIRECTORY = "failed_records"
FAILED_FILE = os.path.join(FAILED_DIRECTORY, "failed_products.json")

BRONZE_TABLE = table("bronze_products")
SILVER_TABLE = table("silver_products")

BRONZE_COLUMNS = (
    "product_id", "product_name", "product_category", "price", "price_category",
    "rating_rate", "rating_count", "product_description", "image_url",
    "data_source", "etl_load_timestamp", "rating_category", "product_value_segment",
)

SILVER_COLUMNS = (
    "product_id", "product_name", "product_category", "price", "price_category",
    "rating_rate", "rating_count", "product_description", "image_url",
    "data_source", "etl_load_timestamp", "product_value_segment", "rating_category",
    "silver_load_timestamp",
)


# --------------------------------------------------------
# Create Bronze / Silver Tables
# --------------------------------------------------------

def create_staging_table():
    """
    Creates the bronze and silver tables in fakestore_catalog.etl_project
    if they do not already exist.
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            product_id INT,
            product_name STRING,
            product_category STRING,
            price DOUBLE,
            price_category STRING,
            rating_rate DOUBLE,
            rating_count INT,
            product_description STRING,
            image_url STRING,
            data_source STRING,
            etl_load_timestamp TIMESTAMP,
            rating_category STRING,
            product_value_segment STRING
        )
        """)

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            product_id INT,
            product_name STRING,
            product_category STRING,
            price DOUBLE,
            price_category STRING,
            rating_rate DOUBLE,
            rating_count INT,
            product_description STRING,
            image_url STRING,
            data_source STRING,
            etl_load_timestamp TIMESTAMP,
            product_value_segment STRING,
            rating_category STRING,
            silver_load_timestamp TIMESTAMP
        )
        """)

        conn.commit()

        logger.info("Bronze/Silver tables are ready.")

    except Exception as e:
        logger.exception("Failed to create bronze/silver tables: %s", e)
        raise

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


# --------------------------------------------------------
# Save Failed Record
# --------------------------------------------------------

def save_failed_record(product):
    """
    Saves failed products locally for replay.
    """

    os.makedirs(FAILED_DIRECTORY, exist_ok=True)

    failed_products = []

    if os.path.exists(FAILED_FILE):

        try:

            with open(FAILED_FILE, "r") as file:
                failed_products = json.load(file)

        except (json.JSONDecodeError, FileNotFoundError):
            failed_products = []

    failed_products.append(product)

    with open(FAILED_FILE, "w") as file:
        json.dump(failed_products, file, indent=4, default=str)

    logger.warning(
        "Product %s saved for replay.",
        product["product_id"]
    )


# --------------------------------------------------------
# Load Product into Bronze + Silver
# --------------------------------------------------------

def load_to_staging(product):
    """
    Inserts a transformed product into bronze_products, then silver_products.

    If loading fails, the product is stored locally so it can be replayed
    later.
    """

    conn = None
    cursor = None

    try:

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            INSERT INTO {BRONZE_TABLE} ({", ".join(BRONZE_COLUMNS)})
            VALUES ({", ".join(["?"] * len(BRONZE_COLUMNS))})
            """,
            tuple(product[col] for col in BRONZE_COLUMNS),
        )

        silver_load_timestamp = datetime.now(timezone.utc)

        cursor.execute(
            f"""
            INSERT INTO {SILVER_TABLE} ({", ".join(SILVER_COLUMNS)})
            VALUES ({", ".join(["?"] * len(SILVER_COLUMNS))})
            """,
            tuple(
                silver_load_timestamp if col == "silver_load_timestamp" else product[col]
                for col in SILVER_COLUMNS
            ),
        )

        conn.commit()

        logger.info(
            "Inserted Product %s into bronze + silver.",
            product["product_id"]
        )

    except Exception as e:

        logger.exception(
            "Failed loading Product %s",
            product["product_id"]
        )

        save_failed_record(product)

        raise

    finally:

        if cursor:
            cursor.close()

        if conn:
            conn.close()
