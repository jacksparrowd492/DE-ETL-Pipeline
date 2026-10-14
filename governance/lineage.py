"""
lineage.py
----------
Data lineage of the pipeline, from origin to the final tables and dashboards.

One registry in code describes:

    DATASETS        every source, topic, hand-off, table, view and dashboard,
                    with owner, layer, PII flag and retention
    JOBS            every process that reads/writes them (Airflow tasks, the Kafka
                    consumer, the Spark stream, Trino views, maintenance jobs)
    COLUMN_LINEAGE  for every managed table: which upstream field(s) each column
                    comes from, and the transformation applied

The registry is used three ways:

  1. Runtime events - lineage_run(job) emits OpenLineage RunEvents (START, then
     COMPLETE or FAIL) with schema + column-lineage facets. They are always
     appended to logs/lineage/openlineage.jsonl, and also POSTed to an
     OpenLineage backend (e.g. Marquez, or DataHub / OpenMetadata's OpenLineage
     endpoint) when OPENLINEAGE_URL is set.
  2. Documentation - docs/LINEAGE.md is generated from it (Mermaid graph +
     column-level tables), so the doc cannot drift from the registry.
  3. Validation - validate() checks the registry against the code: the column
     lists of bronze/silver/fact/gold/dim and the Delta tables are read from
     the loaders' constants and the CREATE TABLE statements, so adding a column
     without mapping its lineage fails CI.

CLI
    python -m governance.lineage validate          # registry <-> code checks
    python -m governance.lineage docs [--check]    # (re)generate docs/LINEAGE.md
    python -m governance.lineage mermaid           # print the graph
    python -m governance.lineage json              # print the registry as JSON
    python -m governance.lineage emit              # emit one COMPLETE event per job
                                                   # (loads the full graph into Marquez)
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import json
import logging
import os
import re
import sys
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

try:  # the WSL Airflow venv may not have python-dotenv
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

from observability.logging_config import (
    PROJECT_ROOT,
    env_flag,
    get_run_context,
    log_dir,
    new_run_id,
)

logger = logging.getLogger(__name__)

JOB_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "fakestore_etl")
PRODUCER = "https://github.com/jacksparrowd492/DE-ETL-Pipeline/tree/main/governance/lineage.py"
OL_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
DOCS_FILE = os.path.join(PROJECT_ROOT, "docs", "LINEAGE.md")


# --------------------------------------------------------
# Namespaces (where each system lives; resolved from the environment)
# --------------------------------------------------------


def namespaces(use_env: bool = True) -> dict:
    """use_env=False gives the defaults, so generated docs don't depend on a local .env."""

    def env(name: str, default: str) -> str:
        return (os.getenv(name) or default) if use_env else default

    api = urlparse(env("FAKESTORE_API_URL", "https://fakestoreapi.com/products"))
    return {
        "api": f"{api.scheme}://{api.netloc}",
        "kafka": f"kafka://{env('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')}",
        "stream_kafka": f"kafka://{env('STREAM_KAFKA_BOOTSTRAP', 'localhost:9092')}",
        "xcom": "airflow://fakestore_pipeline",
        "databricks": f"databricks://{env('DATABRICKS_SERVER_HOSTNAME', '<workspace-host>')}",
        "file": "file://local",
        "lakehouse": env("LINEAGE_LAKEHOUSE_NAMESPACE", "hive://embedded-derby"),
        "trino": env("LINEAGE_TRINO_NAMESPACE", "trino://localhost:8080"),
        "dashboard": "dashboard://local",
    }


def _name_vars(use_env: bool) -> dict:
    def env(name: str, default: str) -> str:
        return (os.getenv(name) or default) if use_env else default

    return {
        "catalog": env("DATABRICKS_CATALOG", "fakestore_catalog"),
        "schema": env("DATABRICKS_SCHEMA", "etl_project"),
        "kafka_topic": env("KAFKA_TOPIC", "validated_products"),
    }


# --------------------------------------------------------
# Registry types
# --------------------------------------------------------


@dataclass(frozen=True)
class Dataset:
    key: str
    system: str  # key into namespaces()
    name: str
    description: str
    layer: str  # source | transport | staging | bronze | silver | gold | quarantine | serving | dashboard
    fields: tuple = ()  # ((name, type), ...)
    managed: bool = False  # a table this project writes -> column lineage is required
    owner: str = "data-eng"
    pii: str = "none"
    retention: str = "-"

    @property
    def field_names(self) -> list:
        return [f for f, _ in self.fields]

    def resolved_name(self, use_env: bool = True) -> str:
        """name with {catalog}/{schema}/{kafka_topic} filled in from the environment."""
        return self.name.format(**_name_vars(use_env))


@dataclass(frozen=True)
class Job:
    name: str
    description: str
    code: str
    orchestrator: str
    inputs: tuple
    outputs: tuple
    processing: str = "BATCH"  # BATCH | STREAMING
    kind: str = "transform"  # transform | maintenance | serving | dashboard


@dataclass(frozen=True)
class Col:
    """Where one output column comes from."""

    sources: tuple  # ((dataset_key, field), ...); empty = generated (timestamp, surrogate key)
    rule: str
    type: str = "DIRECT"  # OpenLineage: DIRECT | INDIRECT
    subtype: str = "TRANSFORMATION"  # IDENTITY | TRANSFORMATION | AGGREGATION | JOIN | ...


def _f(*pairs) -> tuple:
    return tuple(pairs)


# --------------------------------------------------------
# Datasets
# --------------------------------------------------------

API_FIELDS = _f(
    ("id", "int"),
    ("title", "string"),
    ("price", "double"),
    ("category", "string"),
    ("description", "string"),
    ("image", "string"),
    ("rating.rate", "double"),
    ("rating.count", "int"),
)

PRODUCT_COLS = _f(
    ("product_id", "INT"),
    ("product_name", "STRING"),
    ("product_category", "STRING"),
    ("price", "DOUBLE"),
    ("price_category", "STRING"),
    ("rating_rate", "DOUBLE"),
    ("rating_count", "INT"),
    ("product_description", "STRING"),
    ("image_url", "STRING"),
    ("data_source", "STRING"),
    ("etl_load_timestamp", "TIMESTAMP"),
)

USER_EVENT_FIELDS = _f(
    ("event_id", "string"),
    ("user_id", "string"),
    ("event", "string"),
    ("timestamp", "string"),
    ("product_id", "int"),
    ("kafka.partition", "int"),
    ("kafka.offset", "bigint"),
    ("kafka.timestamp", "timestamp"),
    ("value", "binary"),
)

_DATASETS = [
    # ---- batch path: FakeStore -> Kafka -> Databricks ----
    Dataset(
        "api.products",
        "api",
        "/products",
        "FakeStore REST API, GET /products (extract/extractor.py).",
        "source",
        API_FIELDS,
        owner="external (fakestoreapi.com)",
    ),
    Dataset(
        "xcom.raw_products",
        "xcom",
        "extract.products",
        "Raw API records handed from extract to validate (Airflow XCom, or memory in run_pipeline_no_airflow.py).",
        "staging",
        API_FIELDS,
        retention="Airflow metadata DB retention",
    ),
    Dataset(
        "xcom.validated_products",
        "xcom",
        "validate.validated_products",
        "Records with id/title/price/category present and a numeric price (validation/validator.py).",
        "staging",
        API_FIELDS,
        retention="Airflow metadata DB retention",
    ),
    Dataset(
        "kafka.validated_products",
        "kafka",
        "{kafka_topic}",
        "Validated products + batch_id + ingestion_time added by kafka1/producer.py.",
        "transport",
        API_FIELDS + _f(("batch_id", "string"), ("ingestion_time", "string")),
        retention="Kafka topic retention.ms (broker default 7 days)",
    ),
    Dataset(
        "file.failed_products",
        "file",
        "failed_records/failed_products.json",
        "Transformed rows whose Databricks write failed; replayed to Kafka by replay/replay.py.",
        "quarantine",
        PRODUCT_COLS,
        retention="until replayed",
    ),
    Dataset(
        "dbx.bronze_products",
        "databricks",
        "{catalog}.{schema}.bronze_products",
        "Raw + classified product rows, one row per ingested message (append-only).",
        "bronze",
        PRODUCT_COLS + _f(("rating_category", "STRING"), ("product_value_segment", "STRING")),
        managed=True,
        retention="Delta default (time travel 30 days)",
    ),
    Dataset(
        "dbx.silver_products",
        "databricks",
        "{catalog}.{schema}.silver_products",
        "Cleaned copy of bronze; the source of gold and of the Streamlit dashboard.",
        "silver",
        PRODUCT_COLS
        + _f(
            ("product_value_segment", "STRING"),
            ("rating_category", "STRING"),
            ("silver_load_timestamp", "TIMESTAMP"),
        ),
        managed=True,
        retention="Delta default (time travel 30 days)",
    ),
    Dataset(
        "dbx.dim_category",
        "databricks",
        "{catalog}.{schema}.dim_category",
        "Category dimension: surrogate category_id per display name.",
        "gold",
        _f(("category_id", "INT"), ("product_category", "STRING")),
        managed=True,
    ),
    Dataset(
        "dbx.fact_products",
        "databricks",
        "{catalog}.{schema}.fact_products",
        "Star-schema fact, one row per product (MERGE on product_id), FK to dim_category.",
        "gold",
        _f(
            ("product_id", "INT"),
            ("category_id", "INT"),
            ("price", "DOUBLE"),
            ("rating_rate", "DOUBLE"),
            ("rating_count", "INT"),
            ("price_category", "STRING"),
            ("product_value_segment", "STRING"),
            ("rating_category", "STRING"),
        ),
        managed=True,
    ),
    Dataset(
        "dbx.gold_product_summary",
        "databricks",
        "{catalog}.{schema}.gold_product_summary",
        "Per-category aggregate, recomputed from silver for the category of each new product.",
        "gold",
        _f(
            ("product_category", "STRING"),
            ("total_products", "BIGINT"),
            ("average_price", "DOUBLE"),
            ("minimum_price", "DOUBLE"),
            ("maximum_price", "DOUBLE"),
            ("average_rating", "DOUBLE"),
            ("total_rating_count", "BIGINT"),
            ("gold_load_timestamp", "TIMESTAMP"),
        ),
        managed=True,
    ),
    # ---- streaming path: user-events -> Spark -> Delta -> Trino ----
    Dataset(
        "kafka.user_events",
        "stream_kafka",
        "user-events",
        "Synthetic clickstream JSON (streaming/producer.py); ~10% deliberately invalid.",
        "source",
        USER_EVENT_FIELDS,
        pii="pseudonymous (user_id)",
        retention="Kafka topic retention.ms (broker default 7 days)",
    ),
    Dataset(
        "delta.user_events",
        "lakehouse",
        "lakehouse.user_events",
        "Validated events, Delta Lake, PARTITIONED BY event_date, MERGE-deduplicated on event_id.",
        "silver",
        _f(
            ("event_id", "STRING"),
            ("user_id", "STRING"),
            ("event", "STRING"),
            ("event_time", "TIMESTAMP"),
            ("event_date", "DATE"),
            ("kafka_partition", "INT"),
            ("kafka_offset", "BIGINT"),
            ("kafka_timestamp", "TIMESTAMP"),
            ("ingested_at", "TIMESTAMP"),
            ("product_id", "INT"),
        ),
        managed=True,
        pii="pseudonymous (user_id)",
        retention="log 30 d (7 d after expiry DAG), deleted files 7 d",
    ),
    Dataset(
        "delta.user_events_rejected",
        "lakehouse",
        "lakehouse.user_events_rejected",
        "Quarantine for messages that failed validation, with the reason.",
        "quarantine",
        _f(
            ("raw_json", "STRING"),
            ("reason", "STRING"),
            ("kafka_partition", "INT"),
            ("kafka_offset", "BIGINT"),
            ("kafka_timestamp", "TIMESTAMP"),
            ("ingested_at", "TIMESTAMP"),
            ("ingest_date", "DATE"),
        ),
        managed=True,
        pii="may contain user_id inside raw_json",
        retention="log 30 d (7 d after expiry DAG), deleted files 7 d",
    ),
    Dataset(
        "delta.products",
        "lakehouse",
        "lakehouse.products",
        "FakeStore product catalog (Delta, overwritten on each load) that user events join to.",
        "silver",
        _f(
            ("product_id", "INT"),
            ("product_name", "STRING"),
            ("product_category", "STRING"),
            ("price", "DOUBLE"),
            ("price_category", "STRING"),
            ("rating_rate", "DOUBLE"),
            ("rating_count", "INT"),
            ("rating_category", "STRING"),
            ("image_url", "STRING"),
            ("loaded_at", "TIMESTAMP"),
        ),
        managed=True,
        retention="current catalog only (overwrite); old versions until VACUUM",
    ),
    Dataset(
        "trino.user_events",
        "trino",
        "delta.lakehouse.user_events",
        "The same Delta folder registered in Trino (register_table); no copy.",
        "serving",
        (),
    ),
    Dataset(
        "trino.user_events_rejected",
        "trino",
        "delta.lakehouse.user_events_rejected",
        "The same Delta folder registered in Trino (register_table); no copy.",
        "serving",
        (),
    ),
    Dataset(
        "trino.products",
        "trino",
        "delta.lakehouse.products",
        "The same Delta folder registered in Trino (register_table); no copy.",
        "serving",
        (),
    ),
    Dataset(
        "trino.v_events_by_type",
        "trino",
        "delta.lakehouse.v_events_by_type",
        "Serving view: events and distinct users per event type and day.",
        "serving",
        _f(("event", "varchar"), ("event_date", "date"), ("events", "bigint"), ("users", "bigint")),
        managed=True,
    ),
    Dataset(
        "trino.v_events_by_user",
        "trino",
        "delta.lakehouse.v_events_by_user",
        "Serving view: events per user and day.",
        "serving",
        _f(("user_id", "varchar"), ("event_date", "date"), ("events", "bigint")),
        managed=True,
        pii="pseudonymous (user_id)",
    ),
    Dataset(
        "trino.v_events_per_minute",
        "trino",
        "delta.lakehouse.v_events_per_minute",
        "Serving view: events per minute and type.",
        "serving",
        _f(("minute", "timestamp"), ("event", "varchar"), ("events", "bigint")),
        managed=True,
    ),
    Dataset(
        "trino.v_product_activity",
        "trino",
        "delta.lakehouse.v_product_activity",
        "Serving view: events and distinct users per product, event type and day, "
        "with the product's name, category and price.",
        "serving",
        _f(
            ("product_id", "integer"),
            ("product_name", "varchar"),
            ("product_category", "varchar"),
            ("price", "double"),
            ("event", "varchar"),
            ("event_date", "date"),
            ("events", "bigint"),
            ("users", "bigint"),
        ),
        managed=True,
    ),
    # ---- consumers ----
    Dataset(
        "dash.streamlit",
        "dashboard",
        "streamlit_app",
        "Streamlit dashboard (streamlit_app.py).",
        "dashboard",
    ),
    Dataset(
        "dash.metabase",
        "dashboard",
        "metabase/User Events - Trino Lakehouse",
        "Metabase dashboard built by serving/metabase_setup.py.",
        "dashboard",
    ),
    Dataset(
        "dash.powerbi",
        "dashboard",
        "powerbi/User_Event_Analytics_Trino.pbix",
        "Power BI report reading Trino via trino.dbapi.",
        "dashboard",
    ),
]

DATASETS = {d.key: d for d in _DATASETS}


# --------------------------------------------------------
# Jobs
# --------------------------------------------------------

_JOBS = [
    Job(
        "fakestore_pipeline.extract",
        "GET the product list from the FakeStore API.",
        "extract/extractor.py:extract_products",
        "Airflow DAG fakestore_pipeline (or run_pipeline_no_airflow.py)",
        ("api.products",),
        ("xcom.raw_products",),
    ),
    Job(
        "fakestore_pipeline.validate",
        "Drop records missing id/title/price/category or with a non-numeric price.",
        "validation/validator.py:validate_products",
        "Airflow DAG fakestore_pipeline (or run_pipeline_no_airflow.py)",
        ("xcom.raw_products",),
        ("xcom.validated_products",),
    ),
    Job(
        "fakestore_pipeline.send_to_kafka",
        "Add batch_id + ingestion_time and publish each product (acks=all, 5 retries).",
        "kafka1/producer.py:send_products",
        "Airflow DAG fakestore_pipeline (or run_pipeline_no_airflow.py)",
        ("xcom.validated_products",),
        ("kafka.validated_products",),
    ),
    Job(
        "fakestore_pipeline.replay_failed",
        "Republish rows whose Databricks write failed back to Kafka.",
        "replay/replay.py:replay_failed_records",
        "Airflow DAG fakestore_pipeline (or run_pipeline_no_airflow.py)",
        ("file.failed_products",),
        ("kafka.validated_products",),
    ),
    Job(
        "kafka_consumer.load_bronze_silver",
        "Transform each message (transform/transformer.py) and INSERT it into bronze, then silver; "
        "a failed write is saved to failed_records/ for replay.",
        "kafka1/consumer.py:consume -> load/databricks_loader.py:load_to_staging",
        "long-running process: python -m kafka1.consumer",
        ("kafka.validated_products",),
        ("dbx.bronze_products", "dbx.silver_products", "file.failed_products"),
        processing="STREAMING",
    ),
    Job(
        "kafka_consumer.promote_to_warehouse",
        "Look up / create the category id, MERGE the product into fact_products and "
        "recompute that category's gold_product_summary row from silver.",
        "staging/staging_manager.py:promote_to_warehouse",
        "long-running process: python -m kafka1.consumer",
        ("kafka.validated_products", "dbx.silver_products", "dbx.dim_category"),
        ("dbx.dim_category", "dbx.fact_products", "dbx.gold_product_summary"),
        processing="STREAMING",
    ),
    Job(
        "streaming.kafka_to_delta",
        "Spark Structured Streaming: from_json + validation, MERGE valid events on event_id, "
        "append invalid ones to the quarantine table (exactly-once via checkpoint WAL).",
        "streaming/spark_delta.py:write_batch",
        "long-running process: python streaming/spark_delta.py",
        ("kafka.user_events",),
        ("delta.user_events", "delta.user_events_rejected"),
        processing="STREAMING",
    ),
    Job(
        "streaming.load_products",
        "Clean the FakeStore catalog with transform_product() and overwrite the Delta "
        "products table (delta-rs, no Spark).",
        "streaming/load_products.py:main",
        "manual: python streaming/load_products.py (week3_up.sh creates it if missing)",
        ("api.products",),
        ("delta.products",),
    ),
    Job(
        "serving.register_tables",
        "Register the Delta folders in Trino's catalog (no data copied).",
        "serving/register_tables.sql",
        "serving/register_tables.sh",
        ("delta.user_events", "delta.user_events_rejected"),
        ("trino.user_events", "trino.user_events_rejected"),
        kind="serving",
    ),
    Job(
        "serving.register_products",
        "Register the products Delta folder in Trino's catalog (no data copied).",
        "serving/register_tables.sql",
        "serving/register_tables.sh",
        ("delta.products",),
        ("trino.products",),
        kind="serving",
    ),
    Job(
        "serving.views",
        "Aggregated serving views for the BI dashboards.",
        "serving/views.sql",
        "serving/register_tables.sh",
        ("trino.user_events", "trino.products"),
        (
            "trino.v_events_by_type",
            "trino.v_events_by_user",
            "trino.v_events_per_minute",
            "trino.v_product_activity",
        ),
        kind="serving",
    ),
    Job(
        "maintenance.delta_compaction",
        "OPTIMIZE (bin-packing) of cold partitions. Rewrites files, never changes rows.",
        "maintenance/lakehouse_ops.py:compact",
        "Airflow DAG delta_compaction (hourly)",
        ("delta.user_events", "delta.user_events_rejected"),
        ("delta.user_events", "delta.user_events_rejected"),
        kind="maintenance",
    ),
    Job(
        "maintenance.delta_snapshot_expiry",
        "Delete commits older than delta.logRetentionDuration (ends time travel to them).",
        "maintenance/lakehouse_ops.py:expire_snapshots",
        "Airflow DAG delta_snapshot_expiry (02:00)",
        ("delta.user_events", "delta.user_events_rejected"),
        ("delta.user_events", "delta.user_events_rejected"),
        kind="maintenance",
    ),
    Job(
        "maintenance.delta_vacuum",
        "Delete data files no snapshot references any more (after 7 days).",
        "maintenance/lakehouse_ops.py:vacuum",
        "Airflow DAG delta_vacuum (03:00)",
        ("delta.user_events", "delta.user_events_rejected"),
        ("delta.user_events", "delta.user_events_rejected"),
        kind="maintenance",
    ),
    Job(
        "dashboard.streamlit",
        "Reads silver/gold/bronze/dim/fact live from Databricks; the Product Activity "
        "page also reads v_product_activity from Trino.",
        "streamlit_app.py",
        "streamlit run streamlit_app.py",
        (
            "dbx.bronze_products",
            "dbx.silver_products",
            "dbx.gold_product_summary",
            "dbx.dim_category",
            "dbx.fact_products",
            "file.failed_products",
            "trino.v_product_activity",
        ),
        ("dash.streamlit",),
        kind="dashboard",
    ),
    Job(
        "dashboard.metabase",
        "Dashboard queries run on Trino.",
        "serving/metabase_setup.py",
        "Metabase (localhost:3000)",
        (
            "trino.user_events",
            "trino.user_events_rejected",
            "trino.v_events_by_type",
            "trino.v_product_activity",
        ),
        ("dash.metabase",),
        kind="dashboard",
    ),
    Job(
        "dashboard.powerbi",
        "Python source using trino.dbapi; refresh re-queries Trino.",
        "User_Event_Analytics_Trino.pbix",
        "Power BI Desktop",
        ("trino.user_events", "trino.user_events_rejected"),
        ("dash.powerbi",),
        kind="dashboard",
    ),
]

JOBS = {j.name: j for j in _JOBS}


# --------------------------------------------------------
# Column-level lineage of the managed tables
# --------------------------------------------------------

K = "kafka.validated_products"
SILVER = "dbx.silver_products"
UE = "kafka.user_events"
EV = "trino.user_events"
API = "api.products"
TP = "trino.products"


def _product_columns() -> dict:
    """Columns of the transformed row (transform/transformer.py) shared by bronze and silver."""
    return {
        "product_id": Col(((K, "id"),), "int(id)", subtype="IDENTITY"),
        "product_name": Col(((K, "title"),), "title.strip()"),
        "product_category": Col(
            ((K, "category"),), "normalize_category(): Title Case, 'jewelery' -> 'Jewelry'"
        ),
        "price": Col(((K, "price"),), "float(price)", subtype="IDENTITY"),
        "price_category": Col(
            ((K, "price"),), "classify_price(): <50 Budget, <100 Standard, else Premium"
        ),
        "rating_rate": Col(
            ((K, "rating.rate"),), "float(rating.rate), 0.0 if missing", subtype="IDENTITY"
        ),
        "rating_count": Col(
            ((K, "rating.count"),), "int(rating.count), 0 if missing", subtype="IDENTITY"
        ),
        "product_description": Col(
            ((K, "description"),), "as is, '' if missing", subtype="IDENTITY"
        ),
        "image_url": Col(((K, "image"),), "as is, '' if missing", subtype="IDENTITY"),
        "data_source": Col((), "constant 'Fake Store API'"),
        "etl_load_timestamp": Col((), "datetime.now(UTC) at transform time"),
        "rating_category": Col(
            ((K, "rating.rate"),),
            "classify_rating(): <3 Poor, <4 Average, <4.5 Good, else Excellent",
        ),
        "product_value_segment": Col(
            ((K, "price"),), "classify_value_segment(): <50 Low, <100 Medium, else High Value"
        ),
    }


def _silver_columns() -> dict:
    cols = _product_columns()
    cols["silver_load_timestamp"] = Col((), "datetime.now(UTC) at load time")
    return cols


def _fact_columns() -> dict:
    p = _product_columns()
    cols = {
        k: p[k]
        for k in (
            "product_id",
            "price",
            "rating_rate",
            "rating_count",
            "price_category",
            "product_value_segment",
            "rating_category",
        )
    }
    cols["category_id"] = Col(
        (("dbx.dim_category", "category_id"), (K, "category")),
        "dim_category lookup on normalize_category(category)",
        subtype="JOIN",
    )
    return cols


def _agg(field_name: str, fn: str) -> Col:
    return Col(
        ((SILVER, field_name),), f"{fn} over silver_products of the category", subtype="AGGREGATION"
    )


COLUMN_LINEAGE = {
    "dbx.bronze_products": _product_columns(),
    "dbx.silver_products": _silver_columns(),
    "dbx.dim_category": {
        "category_id": Col((), "generated: MAX(category_id) + 1 for a new category"),
        "product_category": Col(((K, "category"),), "normalize_category(category)"),
    },
    "dbx.fact_products": _fact_columns(),
    "dbx.gold_product_summary": {
        "product_category": Col(
            ((SILVER, "product_category"),), "GROUP BY key", subtype="IDENTITY"
        ),
        "total_products": _agg("product_id", "COUNT(*)"),
        "average_price": _agg("price", "AVG(price)"),
        "minimum_price": _agg("price", "MIN(price)"),
        "maximum_price": _agg("price", "MAX(price)"),
        "average_rating": _agg("rating_rate", "AVG(rating_rate)"),
        "total_rating_count": _agg("rating_count", "SUM(rating_count)"),
        "gold_load_timestamp": Col((), "current_timestamp() at MERGE"),
    },
    "delta.user_events": {
        "event_id": Col(
            ((UE, "event_id"), (UE, "kafka.partition"), (UE, "kafka.offset")),
            "event_id, or 'kafka-<partition>-<offset>' when absent",
        ),
        "user_id": Col(((UE, "user_id"),), "from_json, must be non-blank", subtype="IDENTITY"),
        "event": Col(((UE, "event"),), "from_json, must be non-blank", subtype="IDENTITY"),
        "event_time": Col(((UE, "timestamp"),), "to_timestamp(timestamp), must parse"),
        "event_date": Col(((UE, "timestamp"),), "to_date(event_time) - partition column"),
        "kafka_partition": Col(
            ((UE, "kafka.partition"),), "Kafka record metadata", subtype="IDENTITY"
        ),
        "kafka_offset": Col(((UE, "kafka.offset"),), "Kafka record metadata", subtype="IDENTITY"),
        "kafka_timestamp": Col(
            ((UE, "kafka.timestamp"),), "Kafka record metadata", subtype="IDENTITY"
        ),
        "ingested_at": Col((), "current_timestamp() in the micro-batch"),
        "product_id": Col(
            ((UE, "product_id"),),
            "from_json, optional (null for login/logout and older events)",
            subtype="IDENTITY",
        ),
    },
    "delta.user_events_rejected": {
        "raw_json": Col(((UE, "value"),), "CAST(value AS STRING)", subtype="IDENTITY"),
        "reason": Col(
            ((UE, "user_id"), (UE, "event"), (UE, "timestamp")),
            "validation rule that failed: malformed_json / missing_user_id / missing_event / invalid_timestamp",
            type="INDIRECT",
            subtype="CONDITIONAL",
        ),
        "kafka_partition": Col(
            ((UE, "kafka.partition"),), "Kafka record metadata", subtype="IDENTITY"
        ),
        "kafka_offset": Col(((UE, "kafka.offset"),), "Kafka record metadata", subtype="IDENTITY"),
        "kafka_timestamp": Col(
            ((UE, "kafka.timestamp"),), "Kafka record metadata", subtype="IDENTITY"
        ),
        "ingested_at": Col((), "current_timestamp() in the micro-batch"),
        "ingest_date": Col((), "to_date(ingested_at) - partition column"),
    },
    "delta.products": {
        "product_id": Col(((API, "id"),), "int(id)", subtype="IDENTITY"),
        "product_name": Col(((API, "title"),), "title.strip()"),
        "product_category": Col(
            ((API, "category"),), "normalize_category(): Title Case, 'jewelery' -> 'Jewelry'"
        ),
        "price": Col(((API, "price"),), "float(price)", subtype="IDENTITY"),
        "price_category": Col(
            ((API, "price"),), "classify_price(): <50 Budget, <100 Standard, else Premium"
        ),
        "rating_rate": Col(
            ((API, "rating.rate"),), "float(rating.rate), 0.0 if missing", subtype="IDENTITY"
        ),
        "rating_count": Col(
            ((API, "rating.count"),), "int(rating.count), 0 if missing", subtype="IDENTITY"
        ),
        "rating_category": Col(
            ((API, "rating.rate"),),
            "classify_rating(): <3 Poor, <4 Average, <4.5 Good, else Excellent",
        ),
        "image_url": Col(((API, "image"),), "as is, '' if missing", subtype="IDENTITY"),
        "loaded_at": Col((), "datetime.now(UTC) at load time"),
    },
    "trino.v_events_by_type": {
        "event": Col(((EV, "event"),), "GROUP BY key", subtype="IDENTITY"),
        "event_date": Col(((EV, "event_date"),), "GROUP BY key", subtype="IDENTITY"),
        "events": Col(((EV, "event_id"),), "count(*)", subtype="AGGREGATION"),
        "users": Col(((EV, "user_id"),), "count(DISTINCT user_id)", subtype="AGGREGATION"),
    },
    "trino.v_events_by_user": {
        "user_id": Col(((EV, "user_id"),), "GROUP BY key", subtype="IDENTITY"),
        "event_date": Col(((EV, "event_date"),), "GROUP BY key", subtype="IDENTITY"),
        "events": Col(((EV, "event_id"),), "count(*)", subtype="AGGREGATION"),
    },
    "trino.v_events_per_minute": {
        "minute": Col(((EV, "event_time"),), "date_trunc('minute', event_time)"),
        "event": Col(((EV, "event"),), "GROUP BY key", subtype="IDENTITY"),
        "events": Col(((EV, "event_id"),), "count(*)", subtype="AGGREGATION"),
    },
    "trino.v_product_activity": {
        "product_id": Col(
            ((EV, "product_id"),), "GROUP BY key, WHERE product_id IS NOT NULL", subtype="IDENTITY"
        ),
        "product_name": Col(
            ((TP, "product_name"), (EV, "product_id")),
            "LEFT JOIN products ON product_id",
            subtype="JOIN",
        ),
        "product_category": Col(
            ((TP, "product_category"), (EV, "product_id")),
            "LEFT JOIN products ON product_id",
            subtype="JOIN",
        ),
        "price": Col(
            ((TP, "price"), (EV, "product_id")), "LEFT JOIN products ON product_id", subtype="JOIN"
        ),
        "event": Col(((EV, "event"),), "GROUP BY key", subtype="IDENTITY"),
        "event_date": Col(((EV, "event_date"),), "GROUP BY key", subtype="IDENTITY"),
        "events": Col(((EV, "event_id"),), "count(*)", subtype="AGGREGATION"),
        "users": Col(((EV, "user_id"),), "count(DISTINCT user_id)", subtype="AGGREGATION"),
    },
}

# Trino tables are the Delta tables under another catalog name: same columns
for _key, _src in (
    ("trino.user_events", "delta.user_events"),
    ("trino.user_events_rejected", "delta.user_events_rejected"),
    ("trino.products", "delta.products"),
):
    DATASETS[_key] = Dataset(**{**DATASETS[_key].__dict__, "fields": DATASETS[_src].fields})


# --------------------------------------------------------
# Graph queries
# --------------------------------------------------------


def upstream(dataset_key: str) -> list:
    """Every dataset `dataset_key` is (transitively) derived from, nearest first."""
    seen, frontier, order = set(), [dataset_key], []
    while frontier:
        nxt = []
        for key in frontier:
            for job in JOBS.values():
                if key in job.outputs and job.kind != "maintenance":
                    for i in job.inputs:
                        if i != key and i not in seen:
                            seen.add(i)
                            order.append(i)
                            nxt.append(i)
        frontier = nxt
    return order


def downstream(dataset_key: str) -> list:
    """Every dataset derived (transitively) from `dataset_key`, nearest first."""
    seen, frontier, order = set(), [dataset_key], []
    while frontier:
        nxt = []
        for key in frontier:
            for job in JOBS.values():
                if key in job.inputs and job.kind != "maintenance":
                    for o in job.outputs:
                        if o != key and o not in seen:
                            seen.add(o)
                            order.append(o)
                            nxt.append(o)
        frontier = nxt
    return order


# --------------------------------------------------------
# OpenLineage events
# --------------------------------------------------------


def _facet(schema: str, **body) -> dict:
    return {"_producer": PRODUCER, "_schemaURL": schema, **body}


def _dataset_ref(key: str, with_columns: bool) -> dict:
    d = DATASETS[key]
    facets = {
        "documentation": _facet(
            "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
            description=d.description,
        ),
    }
    if d.fields:
        facets["schema"] = _facet(
            "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
            fields=[{"name": n, "type": t} for n, t in d.fields],
        )
    if d.owner:
        facets["ownership"] = _facet(
            "https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json#/$defs/OwnershipDatasetFacet",
            owners=[{"name": d.owner, "type": "TEAM"}],
        )
    if with_columns and key in COLUMN_LINEAGE:
        ns = namespaces()
        facets["columnLineage"] = _facet(
            "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
            fields={
                column: {
                    "inputFields": [
                        {
                            "namespace": ns[DATASETS[src].system],
                            "name": DATASETS[src].resolved_name(),
                            "field": src_field,
                            "transformations": [
                                {
                                    "type": c.type,
                                    "subtype": c.subtype,
                                    "description": c.rule,
                                    "masking": False,
                                }
                            ],
                        }
                        for src, src_field in c.sources
                    ],
                    "transformationDescription": c.rule,
                }
                for column, c in COLUMN_LINEAGE[key].items()
            },
        )
    return {"namespace": namespaces()[d.system], "name": d.resolved_name(), "facets": facets}


def run_event(
    job_name: str,
    event_type: str,
    run_id: str | None = None,
    error: BaseException | None = None,
) -> dict:
    job = JOBS[job_name]
    ctx = get_run_context()
    pipeline_run = run_id or ctx["run_id"]
    # OpenLineage needs a UUID: derive a stable one from the pipeline run + job
    ol_run = (
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"{job_name}:{pipeline_run}"))
        if pipeline_run
        else str(uuid.uuid4())
    )

    run_facets = {
        "etl_run": _facet(
            "https://github.com/jacksparrowd492/DE-ETL-Pipeline/schemas/EtlRunFacet.json",
            pipeline_run_id=pipeline_run,
            component=ctx["component"],
        )
    }
    if error is not None:
        run_facets["errorMessage"] = _facet(
            "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet",
            message=f"{type(error).__name__}: {error}",
            programmingLanguage="python",
            stackTrace="".join(traceback.format_exception(type(error), error, error.__traceback__)),
        )

    return {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": PRODUCER,
        "schemaURL": OL_SCHEMA,
        "run": {"runId": ol_run, "facets": run_facets},
        "job": {
            "namespace": JOB_NAMESPACE,
            "name": job_name,
            "facets": {
                "documentation": _facet(
                    "https://openlineage.io/spec/facets/1-0-1/DocumentationJobFacet.json#/$defs/DocumentationJobFacet",
                    description=job.description,
                ),
                "sourceCodeLocation": _facet(
                    "https://openlineage.io/spec/facets/1-0-1/SourceCodeLocationJobFacet.json#/$defs/SourceCodeLocationJobFacet",
                    type="git",
                    url=f"https://github.com/jacksparrowd492/DE-ETL-Pipeline/blob/main/{job.code.split(':')[0].split(' ')[0]}",
                    path=job.code,
                ),
                "jobType": _facet(
                    "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    processingType=job.processing,
                    integration="CUSTOM",
                    jobType="TASK",
                ),
            },
        },
        "inputs": [_dataset_ref(k, with_columns=False) for k in job.inputs],
        # column lineage only on COMPLETE, where it describes what was written
        "outputs": [_dataset_ref(k, with_columns=event_type == "COMPLETE") for k in job.outputs],
    }


def emit(event: dict) -> bool:
    """Append the event to logs/lineage/openlineage.jsonl and POST it to
    OPENLINEAGE_URL if set. Never raises."""

    ok = True
    try:
        if not env_flag("LOG_TO_FILE", True):
            raise FileNotFoundError  # cloud runtime: only the HTTP backend below
        directory = os.path.join(log_dir(), "lineage")
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, "openlineage.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(event, default=str) + "\n")
    except FileNotFoundError:
        pass
    except OSError:
        logger.exception("Could not write lineage event")
        ok = False

    url = os.environ.get("OPENLINEAGE_URL")
    if url:
        headers = {"Content-Type": "application/json"}
        if os.environ.get("OPENLINEAGE_API_KEY"):
            headers["Authorization"] = f"Bearer {os.environ['OPENLINEAGE_API_KEY']}"
        endpoint = os.environ.get("OPENLINEAGE_ENDPOINT", "api/v1/lineage")
        try:
            r = requests.post(
                f"{url.rstrip('/')}/{endpoint.lstrip('/')}",
                data=json.dumps(event, default=str),
                headers=headers,
                timeout=5,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning("OpenLineage backend rejected/unreachable: %s", e)
            ok = False
    return ok


@contextlib.contextmanager
def lineage_run(job_name: str, run_id: str | None = None):
    """Emit START, then COMPLETE (or FAIL with the error) around a job run."""
    run_id = run_id or get_run_context()["run_id"] or new_run_id()
    emit(run_event(job_name, "START", run_id))
    try:
        yield
    except BaseException as e:
        emit(run_event(job_name, "FAIL", run_id, error=e))
        raise
    emit(run_event(job_name, "COMPLETE", run_id))


# --------------------------------------------------------
# Validation against the code
# --------------------------------------------------------


def _ddl_columns(source: str, marker: str) -> list:
    """Column names of the first CREATE TABLE ... ( ... ) after `marker` in `source`."""
    start = source.index(marker)
    body = source[source.index("(", start) + 1 :]
    cols = []
    for line in body.splitlines():
        if re.match(r"^\s*\)", line):
            break
        m = re.match(r"^\s*([a-z_][a-z0-9_]*)\s+[A-Z]", line)
        if m:
            cols.append(m.group(1))
    return cols


def _read(rel: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel), encoding="utf-8") as f:
        return f.read()


def code_columns() -> dict:
    """The column lists the code actually writes, per managed dataset."""
    from load.databricks_loader import BRONZE_COLUMNS, SILVER_COLUMNS
    from staging.staging_manager import FACT_COLUMNS

    staging_src = _read("staging/staging_manager.py")
    lakehouse_src = _read("streaming/lakehouse.py")
    return {
        "dbx.bronze_products": list(BRONZE_COLUMNS),
        "dbx.silver_products": list(SILVER_COLUMNS),
        "dbx.fact_products": list(FACT_COLUMNS),
        "dbx.dim_category": _ddl_columns(staging_src, "{DIM_CATEGORY_TABLE} ("),
        "dbx.gold_product_summary": _ddl_columns(staging_src, "{GOLD_SUMMARY_TABLE} ("),
        "delta.user_events": _ddl_columns(lakehouse_src, "{EVENTS_TABLE} ("),
        "delta.user_events_rejected": _ddl_columns(lakehouse_src, "{REJECTED_TABLE} ("),
        "delta.products": _assigned_tuple(_read("streaming/load_products.py"), "COLUMNS"),
    }


def _assigned_tuple(source: str, name: str) -> list:
    """The literal value of `NAME = (...)` in a module, without importing it."""
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return list(ast.literal_eval(node.value))
    raise ValueError(f"{name} not found")


def _view_columns() -> dict:
    """SELECT-list output names of each view in serving/views.sql."""
    out = {}
    for name, select in re.findall(
        r"VIEW\s+delta\.lakehouse\.(\w+)\s+AS\s+SELECT\s+(.*?)\s+FROM",
        _read("serving/views.sql"),
        re.S | re.I,
    ):
        cols = []
        for expr in re.split(r",(?![^(]*\))", select):
            m = re.search(r"(?:\bAS\s+)?(\w+)\s*$", expr.strip(), re.I)
            cols.append(m.group(1))
        out[f"trino.{name}"] = cols
    return out


def validate() -> list:
    """Return a list of problems; empty means the registry matches the code."""
    errors = []

    for job in JOBS.values():
        for key in job.inputs + job.outputs:
            if key not in DATASETS:
                errors.append(f"job {job.name}: unknown dataset {key!r}")

    produced = {o for j in JOBS.values() for o in j.outputs}
    for key, d in DATASETS.items():
        if d.layer not in ("source",) and key not in produced:
            errors.append(f"dataset {key}: no job produces it")

    for key, d in DATASETS.items():
        if not d.managed:
            continue
        mapped = COLUMN_LINEAGE.get(key, {})
        missing = [c for c in d.field_names if c not in mapped]
        extra = [c for c in mapped if c not in d.field_names]
        if missing:
            errors.append(f"{key}: columns without lineage: {missing}")
        if extra:
            errors.append(f"{key}: lineage for unknown columns: {extra}")
        job_inputs = {i for j in JOBS.values() if key in j.outputs for i in j.inputs}
        for column, c in mapped.items():
            for src, src_field in c.sources:
                if src not in DATASETS:
                    errors.append(f"{key}.{column}: unknown source dataset {src!r}")
                elif src_field not in DATASETS[src].field_names:
                    errors.append(f"{key}.{column}: {src} has no field {src_field!r}")
                elif src not in job_inputs:
                    errors.append(f"{key}.{column}: {src} is not an input of the job writing {key}")

    try:
        actual = {**code_columns(), **_view_columns()}
    except Exception as e:  # noqa: BLE001 - report, don't crash the check
        errors.append(f"could not read column lists from the code: {e}")
        actual = {}
    for key, cols in actual.items():
        if key not in DATASETS:
            errors.append(f"{key}: exists in code but not in the lineage registry")
        elif DATASETS[key].field_names != cols:
            errors.append(
                f"{key}: registry columns {DATASETS[key].field_names} != code columns {cols}"
            )

    try:
        from transform.transformer import transform_product

        sample = {"id": 1, "title": "x", "price": 1, "category": "electronics"}
        keys = list(transform_product(sample))
        if keys != DATASETS["dbx.bronze_products"].field_names:
            errors.append(f"transform_product() keys {keys} != bronze_products columns")
    except Exception as e:  # noqa: BLE001
        errors.append(f"could not run transform_product(): {e}")

    return errors


# --------------------------------------------------------
# Mermaid + Markdown docs
# --------------------------------------------------------

_SYSTEM_TITLES = {
    "api": "Source",
    "xcom": "Airflow hand-offs",
    "kafka": "Kafka (batch)",
    "file": "Local replay queue",
    "databricks": "Databricks Unity Catalog",
    "stream_kafka": "Kafka (streaming)",
    "lakehouse": "Delta Lake lakehouse",
    "trino": "Trino serving layer",
    "dashboard": "Dashboards",
}


def _node_id(prefix: str, key: str) -> str:
    return prefix + re.sub(r"[^A-Za-z0-9]", "_", key)


def mermaid(include_maintenance: bool = False) -> str:
    lines = ["flowchart LR"]
    by_system: dict = {}
    for d in DATASETS.values():
        by_system.setdefault(d.system, []).append(d)
    for system, title in _SYSTEM_TITLES.items():
        if system not in by_system:
            continue
        lines.append(f'  subgraph {_node_id("s_", system)}["{title}"]')
        for d in by_system[system]:
            name = d.resolved_name(use_env=False)
            label = (
                name.split(".")[-1] if d.system in ("databricks", "lakehouse", "trino") else name
            )
            lines.append(
                f'    {_node_id("d_", d.key)}[("{label}")]'
                if d.managed
                else f'    {_node_id("d_", d.key)}["{label}"]'
            )
        lines.append("  end")
    for job in JOBS.values():
        if job.kind == "maintenance" and not include_maintenance:
            continue
        jid = _node_id("j_", job.name)
        lines.append(f'  {jid}(["{job.name}"]):::job')
        for i in job.inputs:
            lines.append(f"  {_node_id('d_', i)} --> {jid}")
        for o in job.outputs:
            lines.append(f"  {jid} --> {_node_id('d_', o)}")
    lines.append("  classDef job fill:#1a2438,stroke:#3987e5,color:#f1f5f9")
    return "\n".join(lines)


def dot(highlight: str | None = None, include_maintenance: bool = False) -> str:
    """Graphviz DOT of the same graph; `highlight` (a dataset key) bolds it and its lineage."""

    related = set()
    if highlight:
        related = {highlight, *upstream(highlight), *downstream(highlight)}
    colors = {
        "source": "#94a3b8",
        "transport": "#a78bfa",
        "staging": "#64748b",
        "quarantine": "#d03b3b",
        "bronze": "#b45309",
        "silver": "#cbd5e1",
        "gold": "#eab308",
        "serving": "#3987e5",
        "dashboard": "#0ca30c",
    }
    faded = ' fontcolor="#475569"'
    lines = [
        "digraph lineage {",
        '  rankdir=LR; bgcolor="transparent"; nodesep=0.25; ranksep=0.45;',
        '  node [fontname="Helvetica" fontsize=10 fontcolor="#f1f5f9" color="#243049"'
        ' style=filled fillcolor="#131b2e"];',
        '  edge [color="#475569" arrowsize=0.6];',
    ]
    for d in DATASETS.values():
        name = d.resolved_name(use_env=False)
        label = name.split(".")[-1] if d.system in ("databricks", "lakehouse", "trino") else name
        shape = "cylinder" if d.managed else "box"
        pen = 2.5 if d.key == highlight else 1
        fade = faded if highlight and d.key not in related else ""
        lines.append(
            f'  "{d.key}" [label="{label}\\n({d.layer})" shape={shape} penwidth={pen} '
            f'color="{colors.get(d.layer, "#243049")}"{fade}];'
        )
    for job in JOBS.values():
        if job.kind == "maintenance" and not include_maintenance:
            continue
        jid = f"job:{job.name}"
        on_path = not highlight or bool(set(job.inputs) & related and set(job.outputs) & related)
        fade = "" if on_path else faded
        lines.append(
            f'  "{jid}" [label="{job.name}" shape=oval fontsize=9 fillcolor="#1a2438" '
            f'color="#3987e5"{fade}];'
        )
        for i in job.inputs:
            lines.append(f'  "{i}" -> "{jid}";')
        for o in job.outputs:
            lines.append(f'  "{jid}" -> "{o}";')
    lines.append("}")
    return "\n".join(lines)


def _md_escape(text: str) -> str:
    return str(text).replace("|", "\\|")


def render_docs() -> str:
    ns = namespaces(use_env=False)
    out = [
        "# Data lineage",
        "",
        "<!-- Generated by `python -m governance.lineage docs` from governance/lineage.py."
        " Do not edit by hand: CI fails if this file is out of date. -->",
        "",
        "Lineage of every table this project writes, from origin to the dashboards.",
        "The registry in [governance/lineage.py](../governance/lineage.py) is the single",
        "source of truth: this page is generated from it, CI checks it against the column",
        "lists in the loaders and `CREATE TABLE` statements, and each pipeline run emits it",
        "as [OpenLineage](https://openlineage.io) events to `logs/lineage/openlineage.jsonl`",
        "(and to Marquez or any OpenLineage backend when `OPENLINEAGE_URL` is set).",
        "",
        "## Table-level lineage",
        "",
        "Cylinders are tables this project writes; rounded boxes are the jobs between them.",
        "The Delta maintenance jobs (compaction, snapshot expiry, vacuum) read and write the",
        "same tables without changing any row, so they are listed below instead of drawn.",
        "",
        "```mermaid",
        mermaid(),
        "```",
        "",
        "## Final tables",
        "",
        "| Table | Layer | Upstream (nearest first) | Downstream | PII | Retention |",
        "|---|---|---|---|---|---|",
    ]
    for key, d in DATASETS.items():
        if not d.managed:
            continue
        up = (
            ", ".join(f"`{DATASETS[u].resolved_name(use_env=False)}`" for u in upstream(key)) or "-"
        )
        down = (
            ", ".join(f"`{DATASETS[x].resolved_name(use_env=False)}`" for x in downstream(key))
            or "-"
        )
        out.append(
            f"| `{d.resolved_name(use_env=False)}` | {d.layer} | {up} | {down} | {d.pii} | {d.retention} |"
        )

    out += ["", "## Column-level lineage", ""]
    for key, cols in COLUMN_LINEAGE.items():
        d = DATASETS[key]
        out += [
            f"### `{d.resolved_name(use_env=False)}`",
            "",
            f"{d.description} Written by: "
            + ", ".join(
                f"`{j.name}` ({j.code})"
                for j in JOBS.values()
                if key in j.outputs and j.kind != "maintenance"
            )
            + ".",
            "",
            "| Column | Type | Comes from | Transformation |",
            "|---|---|---|---|",
        ]
        types = dict(d.fields)
        for column, c in cols.items():
            src = (
                "<br>".join(
                    f"`{DATASETS[s].resolved_name(use_env=False)}`.`{f}`" for s, f in c.sources
                )
                or "*generated*"
            )
            out.append(
                f"| `{column}` | {types.get(column, '')} | {src} | {_md_escape(c.rule)} ({c.subtype.lower()}) |"
            )
        out.append("")

    out += [
        "## Jobs",
        "",
        "| Job | Kind | Runs as | Code | Reads | Writes |",
        "|---|---|---|---|---|---|",
    ]
    for j in JOBS.values():
        out.append(
            f"| `{j.name}` | {j.kind} ({j.processing.lower()}) | {_md_escape(j.orchestrator)} | `{j.code}` | "
            + ", ".join(f"`{DATASETS[i].resolved_name(use_env=False)}`" for i in j.inputs)
            + " | "
            + ", ".join(f"`{DATASETS[o].resolved_name(use_env=False)}`" for o in j.outputs)
            + " |"
        )

    out += [
        "",
        "## Namespaces",
        "",
        "OpenLineage identifies a dataset by namespace + name. Namespaces come from the",
        "environment (`.env`), so the same registry names the local and the cloud deployment.",
        "",
        "| System | Namespace (this environment) |",
        "|---|---|",
    ]
    out += [f"| {_SYSTEM_TITLES[k]} | `{v}` |" for k, v in ns.items() if k in _SYSTEM_TITLES]
    out += [
        "",
        "## Governance notes",
        "",
        "* **PII.** The product data has none. `user_id` in the streaming tables is a",
        "  pseudonymous id (`user_001`...). `user_events_rejected.raw_json` keeps the whole",
        "  invalid message, so it can contain a `user_id` too; it follows the same retention.",
        "* **Retention.** Delta keeps the transaction log 30 days (7 after the",
        "  `delta_snapshot_expiry` DAG) and deleted data files 7 days (`delta_vacuum`).",
        "  Time travel, and so reproducing an old dashboard number, ends with them.",
        "* **No silent copies.** Trino reads the Delta folders in place (`register_table`),",
        "  so the Trino tables have exactly the lineage and retention of the Delta tables.",
        "* **Failed writes are traceable.** A product whose Databricks write fails goes to",
        "  `failed_records/failed_products.json` and back to Kafka through `replay_failed`;",
        "  it is never dropped. Every run's OpenLineage events carry the pipeline `run_id`",
        "  that also tags its log lines, metrics and alerts (`logs/*.jsonl`).",
        "* **Changing a table.** Add the column to the loader/DDL *and* to `COLUMN_LINEAGE`,",
        "  then run `python -m governance.lineage docs`. CI's Config Validation job runs",
        "  `validate` and `docs --check` and fails until both are done.",
        "",
    ]
    return "\n".join(out)


# --------------------------------------------------------
# CLI
# --------------------------------------------------------


def _registry_json() -> dict:
    return {
        "namespaces": namespaces(),
        "datasets": {
            k: {**d.__dict__, "fields": [list(f) for f in d.fields]} for k, d in DATASETS.items()
        },
        "jobs": {k: j.__dict__ for k, j in JOBS.items()},
        "column_lineage": {
            t: {c: {**v.__dict__, "sources": [list(s) for s in v.sources]} for c, v in cols.items()}
            for t, cols in COLUMN_LINEAGE.items()
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline data lineage")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate", help="check the registry against the code")
    docs = sub.add_parser("docs", help="generate docs/LINEAGE.md")
    docs.add_argument("--check", action="store_true", help="fail if docs/LINEAGE.md is out of date")
    sub.add_parser("mermaid", help="print the Mermaid graph")
    sub.add_parser("json", help="print the registry as JSON")
    sub.add_parser("emit", help="emit one COMPLETE event per job (full graph to the backend)")
    args = parser.parse_args(argv)

    if args.cmd == "validate":
        errors = validate()
        for e in errors:
            print(f"  - {e}")
        print("Lineage validation FAILED." if errors else "Lineage validation passed.")
        return 1 if errors else 0

    if args.cmd == "docs":
        text = render_docs()
        current = open(DOCS_FILE, encoding="utf-8").read() if os.path.exists(DOCS_FILE) else ""
        if args.check:
            if current.replace("\r\n", "\n") != text:
                print(f"{DOCS_FILE} is out of date: run python -m governance.lineage docs")
                return 1
            print("docs/LINEAGE.md is up to date.")
            return 0
        os.makedirs(os.path.dirname(DOCS_FILE), exist_ok=True)
        with open(DOCS_FILE, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        print(f"Wrote {DOCS_FILE}")
        return 0

    if args.cmd == "mermaid":
        print(mermaid(include_maintenance=True))
        return 0

    if args.cmd == "json":
        print(json.dumps(_registry_json(), indent=2, default=str))
        return 0

    if args.cmd == "emit":
        run_id = f"static-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
        ok = all(emit(run_event(name, "COMPLETE", run_id)) for name in JOBS)
        print(
            f"Emitted {len(JOBS)} events (run {run_id})" + ("" if ok else " - some failed, see log")
        )
        return 0 if ok else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
