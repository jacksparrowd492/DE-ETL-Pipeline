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

    # Kafka security: empty = PLAINTEXT (the local broker). Amazon MSK with IAM
    # auth (deploy/README.md): SASL_SSL + AWS_MSK_IAM, which needs botocore.
    KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")
    KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")

    # Trino (serving/): the Streamlit Product Activity page reads user events from
    # delta.lakehouse there. Trino runs in WSL; localhost reaches it from Windows.
    TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
    TRINO_PORT = int(os.getenv("TRINO_PORT") or 8080)
    TRINO_USER = os.getenv("TRINO_USER", "streamlit")


def table(name: str) -> str:
    """Fully-qualified Unity Catalog table name, e.g. table("bronze_products")
    -> "fakestore_catalog.etl_project.bronze_products"."""
    return f"{Config.DATABRICKS_CATALOG}.{Config.DATABRICKS_SCHEMA}.{name}"


def kafka_security_kwargs() -> dict:
    """Extra KafkaProducer / KafkaConsumer settings for a secured cluster;
    {} for a plain local broker, so local behaviour is unchanged."""
    kwargs = {}
    if Config.KAFKA_SECURITY_PROTOCOL:
        kwargs["security_protocol"] = Config.KAFKA_SECURITY_PROTOCOL
    if Config.KAFKA_SASL_MECHANISM:
        kwargs["sasl_mechanism"] = Config.KAFKA_SASL_MECHANISM
    return kwargs


config = Config()
