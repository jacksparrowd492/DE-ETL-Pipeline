"""
spark_delta.py
--------------
Week 2: stream Kafka "user-events" into a partitioned Delta Lake table that
is registered in a Hive Metastore catalog.

    Kafka (user-events)
        -> readStream (Structured Streaming, micro-batch every 10 s)
        -> from_json + validation (same JSON contract as spark_streaming.py)
        -> foreachBatch:
             valid rows    -> MERGE INTO lakehouse.user_events           (PARTITIONED BY event_date)
             invalid rows  -> idempotent append to lakehouse.user_events_rejected

Write-ahead log and exactly-once
    1. Before batch N runs, Spark writes its Kafka offset range to
       checkpoints/user_events/offsets/N (the write-ahead log).
    2. The batch commits to Delta atomically: one new JSON file in
       _delta_log/. Readers never see half-written data.
    3. Only then does Spark write checkpoints/user_events/commits/N.
    If the job dies between 1 and 3, the restart re-reads offsets/N and replays
    exactly the same Kafka records. Replays cannot duplicate data:
      - user_events: MERGE only inserts event_ids that are not in the table yet
      - rejected:    Delta skips a write whose (txnAppId, txnVersion) it has
                     already committed

Usage
-----
    python streaming/spark_delta.py            # resume from the checkpoint (or start at earliest)
    python streaming/spark_delta.py --reset    # wipe table + catalog + checkpoint, start clean
    python streaming/spark_delta.py --trigger "5 seconds"

Then, with the stream stopped, inspect everything with:
    python streaming/spark_verify.py

Observability (Phase II)
    logs     console + logs/streaming.{log,jsonl}, tagged with this run's run_id
    metrics  per micro-batch: valid / rejected / inserted / duplicates, reject ratio,
             p95 ingestion latency (ingested_at - event_time); per trigger: input and
             processed rows/s and trigger time (StreamingQueryListener)
    alerts   high reject ratio, ingestion latency or trigger time above threshold
             (warning), the query stopping on an error (critical) - observability/stream_health.py
    lineage  OpenLineage START when the query starts, COMPLETE/FAIL when it stops

Cloud (LAKEHOUSE_MODE=cluster, LAKEHOUSE_ROOT=s3://...): see deploy/README.md. Kafka
security settings (SASL/IAM for Amazon MSK) are passed with --kafka-option key=value.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    current_timestamp,
    from_json,
    lit,
    nullif,
    percentile_approx,
    to_date,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Same folder as this script, so these are importable when run directly
from lakehouse import (
    CHECKPOINT_DIR,
    EVENTS_TABLE,
    IS_URI,
    LAKEHOUSE_ROOT,
    REJECTED_TABLE,
    build_spark,
    create_catalog_objects,
)
from producer import ensure_topic

# project root, for the shared observability / governance packages
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from governance.lineage import emit, run_event  # noqa: E402
from observability import run_context, setup_logging  # noqa: E402
from observability.cloud_secrets import load_secret_env  # noqa: E402
from observability.logging_config import new_run_id  # noqa: E402
from observability.stream_health import (  # noqa: E402
    observe_batch,
    observe_progress,
    observe_termination,
)

logger = logging.getLogger("spark_delta")
LINEAGE_JOB = "streaming.kafka_to_delta"

# --------------------------------------------------
# Defaults
# --------------------------------------------------

BOOTSTRAP_SERVER = os.environ.get("STREAM_KAFKA_BOOTSTRAP") or "localhost:9092"
TOPIC = os.environ.get("STREAM_KAFKA_TOPIC") or "user-events"
TRIGGER = "10 seconds"
MAX_OFFSETS_PER_TRIGGER = 10_000  # caps a batch after a long outage, keeps batches bounded

# --------------------------------------------------
# Schema (the JSON contract; event_id is new in Week 2, product_id is optional:
# set on shopping events, absent on login/logout and on older messages)
# --------------------------------------------------

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("event", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=True),
    ]
)


def blank(c):
    return c.isNull() | (trim(c) == "")


# --------------------------------------------------
# Parse + validate (streaming DataFrame)
# --------------------------------------------------


def parse_events(kafka_df: DataFrame) -> DataFrame:
    """
    One row per Kafka message with the parsed fields and a `reason` column:
    null for valid events, otherwise why the message was rejected.
    """

    parsed = kafka_df.select(
        col("value").cast("string").alias("raw_json"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("raw_json", "data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")

    event_time = to_timestamp(col("timestamp"))

    reason = (
        when(
            col("user_id").isNull() & col("event").isNull() & col("timestamp").isNull(),
            "malformed_json",
        )
        .when(blank(col("user_id")), "missing_user_id")
        .when(blank(col("event")), "missing_event")
        .when(event_time.isNull(), "invalid_timestamp")
    )

    return (
        parsed.withColumn("event_time", event_time)
        .withColumn("event_date", to_date(event_time))
        # Messages from before Week 2 have no event_id: fall back to their Kafka
        # position, which is just as stable across replays
        .withColumn(
            "event_id",
            coalesce(
                nullif(trim(col("event_id")), lit("")),
                concat(lit("kafka-"), col("kafka_partition"), lit("-"), col("kafka_offset")),
            ),
        )
        .withColumn("reason", reason)
    )


# --------------------------------------------------
# Sink (runs once per micro-batch, on the driver)
# --------------------------------------------------


def stream_id() -> str:
    """
    The query's id from the checkpoint metadata. It stays the same across
    restarts of the same checkpoint and changes when the checkpoint is reset,
    so it is a safe txnAppId for idempotent Delta writes.
    """

    if not IS_URI:
        with open(os.path.join(CHECKPOINT_DIR, "metadata"), encoding="utf-8") as f:
            return json.load(f)["id"]

    # object store (s3://...): read it through Hadoop's file system API
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{CHECKPOINT_DIR}/metadata")
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(fs.open(path), "UTF-8"))
    try:
        return json.loads(reader.readLine())["id"]
    finally:
        reader.close()


def ingest_latency_p95(valid: DataFrame) -> float | None:
    """p95 of ingested_at - event_time in seconds over the batch's valid events."""
    row = valid.agg(
        percentile_approx(
            col("ingested_at").cast("double") - col("event_time").cast("double"), 0.95
        ).alias("p95")
    ).first()
    return float(row.p95) if row and row.p95 is not None else None


def write_batch(batch_df: DataFrame, batch_id: int) -> None:

    spark = batch_df.sparkSession
    batch_df = batch_df.persist()  # read the Kafka slice once, use it three times

    try:
        valid = (
            batch_df.filter(col("reason").isNull())
            .dropDuplicates(["event_id"])  # MERGE fails if a key appears twice in the source
            .select(
                "event_id",
                "user_id",
                "event",
                "event_time",
                "event_date",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                current_timestamp().alias("ingested_at"),
                "product_id",
            )
        )
        rejected = batch_df.filter(col("reason").isNotNull()).select(
            "raw_json",
            "reason",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            current_timestamp().alias("ingested_at"),
            to_date(current_timestamp()).alias("ingest_date"),
        )

        n_valid, n_rejected = valid.count(), rejected.count()
        if n_valid == 0 and n_rejected == 0:
            return
        inserted = None
        latency_p95 = ingest_latency_p95(valid) if n_valid else None

        # 1) Quarantine: plain append made idempotent by Delta's txnAppId/txnVersion
        if n_rejected:
            (
                rejected.write.format("delta")
                .mode("append")
                .option("txnAppId", f"user_events_rejected-{stream_id()}")
                .option("txnVersion", batch_id)
                .saveAsTable(REJECTED_TABLE)
            )

        # 2) Events: insert-only MERGE on event_id. The explicit list of dates
        #    lets Delta prune the target to just the partitions this batch touches
        #    instead of scanning the whole table for matches.
        if n_valid:
            inserted = 0
            dates = [r.event_date for r in valid.select("event_date").distinct().collect()]
            date_list = ", ".join(f"DATE'{d}'" for d in dates)

            table = DeltaTable.forName(spark, EVENTS_TABLE)
            before = table.history(1).collect()[0].version

            (
                table.alias("t")
                .merge(
                    valid.alias("s"),
                    f"t.event_date IN ({date_list}) "
                    "AND t.event_date = s.event_date AND t.event_id = s.event_id",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            # A MERGE that finds only duplicates may not commit a new version
            last = table.history(1).collect()[0]
            version = last.version
            if version > before:
                inserted = int(last.operationMetrics.get("numTargetRowsInserted", 0))

            print(
                f"[batch {batch_id}] valid={n_valid} inserted={inserted} "
                f"duplicates_skipped={n_valid - inserted} rejected={n_rejected} "
                f"-> {EVENTS_TABLE} v{version}, partitions {[str(d) for d in dates]}",
                flush=True,
            )
        else:
            print(f"[batch {batch_id}] valid=0 rejected={n_rejected}", flush=True)

        observe_batch(batch_id, EVENTS_TABLE, n_valid, n_rejected, inserted, latency_p95)

    finally:
        batch_df.unpersist()


class MetricsListener(StreamingQueryListener):
    """Per-trigger throughput and timing, from Spark's own progress reports."""

    def onQueryStarted(self, event):
        logger.info("Query started id=%s run=%s", event.id, event.runId)

    def onQueryProgress(self, event):
        p = event.progress
        observe_progress(
            p.name or "kafka_to_delta",
            p.batchId,
            p.numInputRows,
            p.inputRowsPerSecond,
            p.processedRowsPerSecond,
            p.durationMs.get("triggerExecution"),
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        logger.info("Query terminated id=%s exception=%s", event.id, event.exception)


# --------------------------------------------------
# Main
# --------------------------------------------------


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description="Kafka user-events -> Delta Lake (Hive Metastore)")
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVER, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic")
    parser.add_argument("--trigger", default=TRIGGER, help="Micro-batch interval")
    parser.add_argument(
        "--kafka-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra Kafka client setting, e.g. security.protocol=SASL_SSL (repeatable)",
    )
    parser.add_argument(
        "--skip-topic-check",
        action="store_true",
        help="Don't create the topic (managed Kafka, or no admin rights)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the lakehouse (tables, catalog, checkpoint) and start from scratch",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_logging("streaming")
    load_secret_env()  # cloud: Slack/SMTP settings from Secrets Manager (no-op locally)

    if args.reset:
        if IS_URI:
            sys.exit("--reset only works on a local lakehouse; delete the cloud prefix by hand")
        # Table and checkpoint must be reset together: a checkpoint that points
        # past data the table no longer has would silently skip it
        shutil.rmtree(LAKEHOUSE_ROOT, ignore_errors=True)
        print(f"Reset: removed {LAKEHOUSE_ROOT}")

    if not args.skip_topic_check:
        ensure_topic(args.bootstrap, args.topic)

    spark = build_spark("KafkaToDeltaLakehouse")
    create_catalog_objects(spark)
    spark.streams.addListener(MetricsListener())

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        # Only used on the very first run; afterwards the checkpoint decides
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    )
    for item in args.kafka_option:
        key, _, value = item.partition("=")
        reader = reader.option(f"kafka.{key}", value)
    kafka_df = reader.load()

    query = (
        parse_events(kafka_df)
        .writeStream.queryName("kafka_to_delta")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=args.trigger)
        .start()
    )

    print(
        f"\nStreaming '{args.topic}' on {args.bootstrap} -> {EVENTS_TABLE} (Delta, Hive Metastore)"
    )
    print(f"Checkpoint / WAL: {CHECKPOINT_DIR}")
    print(f"Queries running: ['{query.name}']  |  Ctrl+C to stop\n", flush=True)

    with run_context(run_id=new_run_id(), task="kafka_to_delta"):
        emit(run_event(LINEAGE_JOB, "START"))
        error = None

        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\nStopping stream...")
        except Exception as e:  # StreamingQueryException: the query died
            error = e
            logger.exception("Streaming query failed")
            observe_termination(query.name, f"{type(e).__name__}: {e}")
            raise
        finally:
            if error is None:
                observe_termination(query.name, None)
            emit(run_event(LINEAGE_JOB, "FAIL" if error else "COMPLETE", error=error))
            query.stop()
            spark.stop()
