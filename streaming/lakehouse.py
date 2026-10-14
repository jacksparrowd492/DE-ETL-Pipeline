"""
lakehouse.py
------------
Shared settings for the Week 2 lakehouse layer (Delta Lake + Hive Metastore).

Both spark_delta.py (the streaming sink) and spark_verify.py (the checks)
import from here, so they always agree on the catalog, the table
definitions and where everything lives on disk.

Layout (all under streaming/_lakehouse/, git-ignored):

    metastore_db/                     Hive Metastore (embedded Derby database):
                                      the table CATALOG - database/table names,
                                      schemas, partition columns, locations
    warehouse/lakehouse.db/
        user_events/                  Delta table, PARTITIONED BY event_date
            _delta_log/               Delta transaction log (one JSON per commit)
            event_date=2026-09-23/    one folder per partition value
        user_events_rejected/         quarantine table for invalid messages
    checkpoints/user_events/          Structured Streaming checkpoint
        offsets/                      WRITE-AHEAD LOG: batch N's Kafka offsets are
                                      written here BEFORE batch N is processed
        commits/                      batch N is marked done here AFTER the
                                      Delta commit succeeded

Cloud / cluster mode (LAKEHOUSE_MODE=cluster, see deploy/README.md): the root is a
URI such as s3://bucket/lakehouse (LAKEHOUSE_ROOT), the cluster supplies the master,
the Delta + Kafka jars and the metastore (e.g. the AWS Glue Data Catalog), so none
of the local-only settings below (local[*], spark.jars.packages, Derby) are applied.

Catalog choice: the Hive Metastore runs embedded inside the Spark driver
(Derby database on disk). It is a real Hive Metastore with the same schema a
standalone metastore service uses, so moving to a shared metastore later is
a config change (hive.metastore.uris), not a code change. The embedded Derby
database allows only ONE Spark process at a time - see spark_verify.py --live.
"""

import os
import sys
from importlib.metadata import PackageNotFoundError, version

# PySpark 3.5.1 / Hadoop 3.3.4 requires Java 8-17 (Java 23+ removes Subject.getSubject)
for _jdk in [
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot",
    r"C:\Program Files\Eclipse Adoptium\jdk-17",
    r"C:\Program Files\Java\jdk-17",
]:
    if os.path.isdir(_jdk):
        os.environ["JAVA_HOME"] = _jdk
        os.environ["PATH"] = os.path.join(_jdk, "bin") + os.pathsep + os.environ.get("PATH", "")
        break

import pyspark  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

# --------------------------------------------------
# Paths
# --------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_MODE = os.environ.get("LAKEHOUSE_MODE", "local").lower()  # local | cluster
LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT") or os.path.join(HERE, "_lakehouse")
IS_URI = "://" in LAKEHOUSE_ROOT  # s3://, abfss://, gs://, dbfs:/ ... (not a local folder)

# Forward slashes: these strings end up in Hadoop paths and a JDBC URL
_root = LAKEHOUSE_ROOT.replace("\\", "/")
WAREHOUSE_DIR = f"{_root}/warehouse"
METASTORE_DIR = f"{_root}/metastore_db"
CHECKPOINT_DIR = f"{_root}/checkpoints/user_events"

# --------------------------------------------------
# Catalog objects
# --------------------------------------------------

DATABASE = "lakehouse"
EVENTS_TABLE = f"{DATABASE}.user_events"
REJECTED_TABLE = f"{DATABASE}.user_events_rejected"

# Managed tables live at <warehouse>/<db>.db/<table>; spark_verify.py --live
# reads them by these paths when the metastore is locked by the stream
EVENTS_PATH = f"{WAREHOUSE_DIR}/{DATABASE}.db/user_events"
REJECTED_PATH = f"{WAREHOUSE_DIR}/{DATABASE}.db/user_events_rejected"

# Partition by the EVENT's date (not arrival time), so queries such as
# "WHERE event_date = '2026-09-23'" read only that day's folder. One folder per
# day keeps partitions large enough to avoid a small-files problem.
EVENTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
    event_id         STRING    NOT NULL COMMENT 'Unique event key used by MERGE (dedup)',
    user_id          STRING    NOT NULL,
    event            STRING    NOT NULL,
    event_time       TIMESTAMP NOT NULL COMMENT 'Event time from the producer (UTC)',
    event_date       DATE      NOT NULL COMMENT 'Partition column = to_date(event_time)',
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_timestamp  TIMESTAMP,
    ingested_at      TIMESTAMP COMMENT 'When Spark wrote the row',
    product_id       INT       COMMENT 'FakeStore product the event is about (null for login/logout)'
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Validated user events streamed from Kafka topic user-events'
TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.checkpointInterval'           = '10'
)
"""

REJECTED_DDL = f"""
CREATE TABLE IF NOT EXISTS {REJECTED_TABLE} (
    raw_json         STRING,
    reason           STRING    COMMENT 'Why validation failed',
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_timestamp  TIMESTAMP,
    ingested_at      TIMESTAMP,
    ingest_date      DATE      COMMENT 'Partition column = to_date(ingested_at)'
)
USING DELTA
PARTITIONED BY (ingest_date)
COMMENT 'Messages from user-events that failed validation (quarantine)'
TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
)
"""

# Columns added after the table was first created: (name, type, comment).
# create_catalog_objects() adds any that an existing table is missing, so an
# older lakehouse is upgraded in place (Delta ADD COLUMNS only touches the log;
# earlier rows read the new column as NULL).
EVENTS_ADDED_COLUMNS = [
    ("product_id", "INT", "FakeStore product the event is about (null for login/logout)"),
]

# --------------------------------------------------
# Packages (must match the installed PySpark / delta-spark versions)
# --------------------------------------------------

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
try:
    DELTA_PACKAGE = f"io.delta:delta-spark_2.12:{version('delta-spark')}"
except PackageNotFoundError:  # cluster mode: the cluster provides the Delta jars
    DELTA_PACKAGE = None


def build_spark(
    app_name: str, with_kafka: bool = True, with_metastore: bool = True
) -> SparkSession:
    """
    Spark session with the Delta Lake catalog.

    with_metastore=True  -> spark_catalog is backed by the Hive Metastore, so
                            tables are registered by name and survive restarts
    with_metastore=False -> in-memory catalog; tables are read by path only
                            (used by spark_verify.py --live)
    """

    cluster = LAKEHOUSE_MODE == "cluster"

    builder = SparkSession.builder.appName(app_name)

    if not cluster:
        # Windows: Spark needs hadoop.dll / winutils.exe on PATH
        hadoop_home = os.environ.get("HADOOP_HOME")
        if hadoop_home:
            os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]

        # Use this same interpreter for Spark's Python workers
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        if DELTA_PACKAGE is None:
            raise RuntimeError("pip install delta-spark (local mode needs it for the Delta jars)")
        packages = [DELTA_PACKAGE] + ([KAFKA_PACKAGE] if with_kafka else [])
        builder = builder.master("local[*]").config("spark.jars.packages", ",".join(packages))

    builder = (
        builder
        # Delta Lake: SQL extensions (MERGE, DESCRIBE HISTORY, ...) + catalog
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.shuffle.partitions", "4")  # small local job, few partitions
        .config("spark.sql.session.timeZone", "UTC")  # event_date = UTC calendar day
        .config("spark.ui.showConsoleProgress", "false")
    )

    if with_metastore and cluster:
        # the cluster's own metastore (Glue Data Catalog on EMR, Unity Catalog on Databricks)
        builder = builder.enableHiveSupport()
    elif with_metastore:
        builder = (
            builder.enableHiveSupport()
            # Pin the metastore database to a fixed folder, so every script
            # sees the same catalog no matter which directory it is run from
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={METASTORE_DIR};create=true",
            )
            # Keep Hive's scratch files inside the lakehouse folder instead of \tmp\hive
            .config("spark.hadoop.hive.exec.scratchdir", f"{_root}/tmp/hive")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def create_catalog_objects(spark: SparkSession) -> None:
    """Register the database and both tables in the Hive Metastore and add any
    columns an older events table is missing (idempotent)."""

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE} "
        "COMMENT 'Week 2 lakehouse: Kafka user-events in Delta Lake'"
    )
    spark.sql(EVENTS_DDL)
    spark.sql(REJECTED_DDL)

    existing = {c.lower() for c in spark.table(EVENTS_TABLE).columns}
    for name, sql_type, comment in EVENTS_ADDED_COLUMNS:
        if name not in existing:
            spark.sql(
                f"ALTER TABLE {EVENTS_TABLE} ADD COLUMNS ({name} {sql_type} COMMENT '{comment}')"
            )
            print(f"Schema upgrade: added column {name} {sql_type} to {EVENTS_TABLE}", flush=True)
