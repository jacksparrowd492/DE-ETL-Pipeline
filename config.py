"""
config.py
---------
Centralized configuration for the FakeStore ETL pipeline.
Loads values from the .env file at the project root.

Place this file at the project root, alongside extract/, validation/,
transform/, load/, kafka1/, etc. — the same directory that
fakestore_pipeline.py adds to sys.path.
"""

import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    # FakeStore API
    FAKESTORE_API_URL = os.getenv("FAKESTORE_API_URL", "https://fakestoreapi.com/products")

    # Databricks
    DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

    # Databricks Unity Catalog — all storage/retrieval targets this catalog+schema
    DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "fakestore_catalog")
    DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "etl_project")

    # Kafka
    KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


def table(name: str) -> str:
    """Fully-qualified Unity Catalog table name, e.g. table("bronze_products")
    -> "fakestore_catalog.etl_project.bronze_products"."""
    return f"{Config.DATABRICKS_CATALOG}.{Config.DATABRICKS_SCHEMA}.{name}"


config = Config()
