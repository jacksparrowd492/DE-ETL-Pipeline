"""
spark_verify.py
---------------
Prints evidence that the Week 2 lakehouse works, one section per requirement:

    1. CATALOG       lakehouse.user_events is registered in the Hive Metastore as a Delta table
    2. PARTITIONING  the table is PARTITIONED BY event_date: folders on disk + pruning in the plan
    3. WAL           streaming checkpoint (offsets/ written before commits/) and
                     Delta's transaction log (_delta_log/, DESCRIBE HISTORY)
    4. IDEMPOTENCY   no event_id appears twice; rejected messages sit in quarantine
    5. TIME TRAVEL   the table as it was at an earlier version

Usage
-----
    python streaming/spark_verify.py          # stop spark_delta.py first (see below)
    python streaming/spark_verify.py --live   # while spark_delta.py is running

The embedded Hive Metastore (Derby) accepts one Spark process at a time.
While spark_delta.py holds it, use --live: the tables are then read directly
by their storage path, which Delta supports without any catalog. That works
because the Delta table's schema and partitioning are stored in _delta_log/.
"""

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from lakehouse import (
    CHECKPOINT_DIR,
    DATABASE,
    EVENTS_PATH,
    EVENTS_TABLE,
    METASTORE_DIR,
    REJECTED_PATH,
    REJECTED_TABLE,
    build_spark,
)


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}", flush=True)


def batch_ids(folder: str) -> list[int]:
    """Numeric file names in a checkpoint log folder (offsets/ or commits/)."""

    if not os.path.isdir(folder):
        return []
    return sorted(int(n) for n in os.listdir(folder) if n.isdigit())


# --------------------------------------------------
# 1. Catalog
# --------------------------------------------------


def show_catalog(spark: SparkSession, live: bool) -> None:

    section("1. CATALOG: Delta Lake tables registered in the Hive Metastore")

    if live:
        print("--live: the metastore is locked by the running stream, so the catalog is skipped.")
        print(f"Tables are read by path instead: {EVENTS_PATH}")
        return

    print(f"spark.sql.catalogImplementation = {spark.conf.get('spark.sql.catalogImplementation')}")
    print(f"spark_catalog                   = {spark.conf.get('spark.sql.catalog.spark_catalog')}")
    print(f"Metastore database (Derby)      = {METASTORE_DIR}\n")

    spark.sql(f"SHOW TABLES IN {DATABASE}").show(truncate=False)

    keep = ("Type", "Provider", "Location", "Comment")
    (
        spark.sql(f"DESCRIBE TABLE EXTENDED {EVENTS_TABLE}")
        .filter(f"col_name IN {keep}")
        .select("col_name", "data_type")
        .show(truncate=False)
    )


# --------------------------------------------------
# 2. Partitioning
# --------------------------------------------------


def show_partitioning(spark: SparkSession, events: str) -> None:

    section("2. PARTITIONING: PARTITIONED BY (event_date)")

    (
        spark.sql(f"DESCRIBE DETAIL {events}")
        .select("format", "partitionColumns", "numFiles", "sizeInBytes")
        .show(truncate=False)
    )

    print("Rows per partition:")
    spark.sql(
        f"SELECT event_date, COUNT(*) AS rows FROM {events} GROUP BY event_date ORDER BY event_date"
    ).show(truncate=False)

    folders = sorted(d for d in os.listdir(EVENTS_PATH) if d.startswith("event_date="))
    print(f"Partition folders on disk ({EVENTS_PATH}):")
    for d in folders:
        files = [f for f in os.listdir(os.path.join(EVENTS_PATH, d)) if f.endswith(".parquet")]
        print(f"  {d}/   {len(files)} parquet file(s)")

    # Partition pruning: a filter on event_date becomes a PartitionFilter, so
    # Spark lists only that folder instead of scanning the whole table
    latest = spark.sql(f"SELECT MAX(event_date) AS d FROM {events}").collect()[0].d
    if latest:
        plan = spark.sql(f"SELECT * FROM {events} WHERE event_date = DATE'{latest}'")
        plan_text = plan._jdf.queryExecution().executedPlan().toString()
        pruning = [ln.strip() for ln in plan_text.splitlines() if "PartitionFilters" in ln]
        print(f"\nPhysical plan for WHERE event_date = '{latest}':")
        for ln in pruning:
            start = ln.find("PartitionFilters")
            print(f"  ... {ln[start : start + 120]} ...")


# --------------------------------------------------
# 3. Write-ahead log
# --------------------------------------------------


def show_wal(spark: SparkSession, events: str) -> None:

    section("3a. WAL: Structured Streaming checkpoint (offsets/ -> Delta commit -> commits/)")

    offsets = batch_ids(os.path.join(CHECKPOINT_DIR, "offsets"))
    commits = batch_ids(os.path.join(CHECKPOINT_DIR, "commits"))

    print(f"Checkpoint: {CHECKPOINT_DIR}")
    if not offsets:
        print("No batches logged yet - run spark_delta.py with the producer first.")
    else:
        print(f"offsets/  (written BEFORE a batch runs) : batches {offsets[0]}..{offsets[-1]}")
        print(
            f"commits/  (written AFTER it committed)  : batches {commits[0]}..{commits[-1]}"
            if commits
            else "commits/  : none"
        )
        pending = sorted(set(offsets) - set(commits))
        print(
            f"In flight / to replay on restart        : {pending or 'none (every logged batch committed)'}"
        )

        # Last line of an offsets file = Kafka offsets per topic partition
        with open(os.path.join(CHECKPOINT_DIR, "offsets", str(offsets[-1])), encoding="utf-8") as f:
            kafka_offsets = json.loads(f.read().strip().splitlines()[-1])
        print(f"\nKafka end offsets logged for batch {offsets[-1]}: {kafka_offsets}")

    section("3b. WAL: Delta transaction log (_delta_log/) - one atomic commit per write")

    log_dir = f"{EVENTS_PATH}/_delta_log"
    entries = os.listdir(log_dir)
    commits_json = [n for n in entries if n.endswith(".json") and n[:20].isdigit()]
    checkpoints = [n for n in entries if ".checkpoint" in n and n.endswith(".parquet")]
    print(f"{log_dir}")
    print(f"  {len(commits_json)} commit file(s) (00000000000000000000.json, ...)")
    print(f"  {len(checkpoints)} log checkpoint(s) (compacted every 10 commits)\n")

    # DESCRIBE HISTORY cannot be a subquery, so pick its columns in the DataFrame API
    (
        spark.sql(f"DESCRIBE HISTORY {events} LIMIT 15")
        .select(
            "version",
            "timestamp",
            "operation",
            col("operationMetrics")["numTargetRowsInserted"].alias("rows_inserted"),
            col("operationMetrics")["numOutputRows"].alias("rows_written"),
        )
        .show(truncate=False)
    )


# --------------------------------------------------
# 4. Idempotency and data quality
# --------------------------------------------------


def show_quality(spark: SparkSession, events: str, rejected: str) -> None:

    section("4. IDEMPOTENCY: MERGE on event_id keeps every event exactly once")

    spark.sql(f"""
        SELECT COUNT(*)                            AS total_rows,
               COUNT(DISTINCT event_id)            AS distinct_event_ids,
               COUNT(*) - COUNT(DISTINCT event_id) AS duplicates
        FROM {events}
    """).show(truncate=False)

    print("Rejected messages (quarantine table), by reason:")
    spark.sql(
        f"SELECT reason, COUNT(*) AS messages FROM {rejected} GROUP BY reason ORDER BY messages DESC"
    ).show(truncate=False)

    print("Latest events:")
    spark.sql(f"""
        SELECT event_id, user_id, event, product_id, event_time, event_date, kafka_partition, kafka_offset
        FROM {events}
        ORDER BY event_time DESC
        LIMIT 10
    """).show(truncate=False)


# --------------------------------------------------
# 5. Time travel
# --------------------------------------------------


def show_time_travel(spark: SparkSession, events: str) -> None:

    section("5. TIME TRAVEL: reading older versions from the transaction log")

    latest = spark.sql(f"DESCRIBE HISTORY {events} LIMIT 1").collect()[0].version
    versions = sorted({0, 1, latest // 2, latest})

    for v in versions:
        if v > latest:
            continue
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {events} VERSION AS OF {v}").collect()[0].n
        print(f"  VERSION AS OF {v:<4} -> {n} rows")


# --------------------------------------------------
# Main
# --------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify the Week 2 Delta Lake lakehouse")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Read tables by path (use while spark_delta.py is running)",
    )
    args = parser.parse_args()

    if not os.path.isdir(os.path.join(EVENTS_PATH, "_delta_log")):
        raise SystemExit("No lakehouse table yet - run streaming/spark_delta.py first.")

    spark = build_spark("VerifyLakehouse", with_kafka=False, with_metastore=not args.live)
    spark.sparkContext.setLogLevel("ERROR")

    events = f"delta.`{EVENTS_PATH}`" if args.live else EVENTS_TABLE
    rejected = f"delta.`{REJECTED_PATH}`" if args.live else REJECTED_TABLE

    try:
        show_catalog(spark, args.live)
        show_partitioning(spark, events)
        show_wal(spark, events)
        show_quality(spark, events, rejected)
        show_time_travel(spark, events)
    except Exception as exc:
        if "Derby" in str(exc) or "metastore" in str(exc).lower():
            raise SystemExit(
                "\nThe Hive Metastore is in use by another Spark process (spark_delta.py?).\n"
                "Stop it first, or run:  python streaming/spark_verify.py --live"
            ) from exc
        raise
    finally:
        spark.stop()
