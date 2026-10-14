"""
spark_streaming.py
------------------
PySpark Structured Streaming job for the "user-events" demo.

Reads JSON events from Kafka and prints three live console streams:

    [RAW]     the Kafka message exactly as received (binary value -> string)
    [PARSED]  schema-enforced, validated columns (bad records filtered out)
    [COUNTS]  events per type per 10-second window (simple transformation)

All three queries run continuously (micro-batches every few seconds).
RAW and PARSED use outputMode("append") with the console sink. COUNTS uses
"update" by default (running counts each batch; pass --counts-mode append for
final-only counts) and skips empty batches.

Usage
-----
    python streaming/spark_streaming.py                    # only new messages
    python streaming/spark_streaming.py --from-beginning   # replay the topic
    python streaming/spark_streaming.py --counts-mode append

Runs locally on Windows without Docker. Needs Java 17 and HADOOP_HOME
pointing to a folder whose bin\\ contains winutils.exe and hadoop.dll.
"""

import argparse
import os
import shutil
import sys
import time

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
from pyspark.sql.functions import col, count, from_json, lit, to_timestamp, trim, window  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

# Same folder as this script, so it is importable when run directly
from producer import ensure_topic  # noqa: E402

# --------------------------------------------------
# Defaults
# --------------------------------------------------

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "user-events"

# Kafka connector must match the installed PySpark version (Scala 2.12 build)
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"

# Every run gets its OWN checkpoint folder (named by start time + process id),
# so several Spark jobs running at once never touch each other's files.
# The folder is deleted when the run stops, so each run is a clean demo.
CHECKPOINT_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_checkpoints")
CHECKPOINT_ROOT = os.path.join(
    CHECKPOINT_BASE, f"run-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"
)
STALE_CHECKPOINT_SECONDS = 24 * 3600  # leftovers from crashed runs older than this are pruned

TRIGGER = "5 seconds"  # micro-batch interval (keeps console output readable)

# --------------------------------------------------
# Schema (the JSON contract)
# --------------------------------------------------

EVENT_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=True),
        StructField("event", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
    ]
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------


def build_spark() -> SparkSession:

    # Windows: Spark needs hadoop.dll / winutils.exe on PATH
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]

    # Use this same interpreter for Spark's Python workers
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName("UserEventsStreaming")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")  # small local job, few partitions
        .config("spark.sql.session.timeZone", "UTC")  # show timestamps as sent (UTC)
        .config("spark.ui.showConsoleProgress", "false")  # no progress bars mixed into output
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# --------------------------------------------------
# Console table (same look as Spark's console sink)
# --------------------------------------------------


def format_table(columns: list[str], rows: list) -> str:
    cells = [[str(v) for v in row] for row in rows]
    widths = [max(len(c), *(len(r[i]) for r in cells)) for i, c in enumerate(columns)]

    border = "+" + "+".join("-" * w for w in widths) + "+"

    def line(values):
        return "|" + "|".join(v.ljust(w) for v, w in zip(values, widths)) + "|"

    return "\n".join([border, line(columns), border, *(line(r) for r in cells), border])


# --------------------------------------------------
# Pipeline
# --------------------------------------------------


def start_streams(
    spark: SparkSession,
    bootstrap: str,
    topic: str,
    starting_offsets: str,
    counts_mode: str = "update",
):

    # 1) Source: subscribe to the Kafka topic
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Kafka delivers key/value as binary -> cast to string
    raw_df = kafka_df.select(
        lit("RAW").alias("stream"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("raw_json"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
    )

    # 3) Enforce the schema: from_json() turns the string into a struct.
    #    Malformed JSON or missing fields become nulls instead of crashing.
    parsed_df = (
        raw_df.select(from_json(col("raw_json"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # 4) Validation: drop anything without a usable user_id, event or timestamp
    valid_df = parsed_df.filter(
        col("user_id").isNotNull()
        & (trim(col("user_id")) != "")
        & col("event").isNotNull()
        & (trim(col("event")) != "")
        & col("event_time").isNotNull()
    ).select(lit("PARSED").alias("stream"), "user_id", "event", "timestamp", "event_time")

    # 5) Transformation: count events per type in 10-second tumbling windows.
    #    The watermark lets Spark drop state for windows older than 5s past
    #    the newest event (and, in append mode, decides when a window is final).
    counts_df = (
        valid_df.withWatermark("event_time", "5 seconds")
        .groupBy(window(col("event_time"), "10 seconds"), col("event"))
        .agg(count("*").alias("event_count"))
        .select(
            lit("COUNTS").alias("stream"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event",
            "event_count",
        )
    )

    # 6) Sinks: print every stream to the console in append mode
    def to_console(df, name: str):
        return (
            df.writeStream.queryName(name)
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 100)
            .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, name))
            .trigger(processingTime=TRIGGER)
            .start()
        )

    # 7) COUNTS sink: foreachBatch instead of format("console") so that empty
    #    micro-batches are skipped rather than printed as empty tables.
    #    "update"  -> every batch prints the running counts of windows that changed
    #    "append"  -> each window prints once, when the watermark closes it
    def print_counts(batch_df, batch_id: int):
        # Cast timestamps to strings inside Spark (session tz = UTC); collect()
        # would otherwise convert them to the machine's local time zone
        rows = (
            batch_df.withColumn("window_start", col("window_start").cast("string"))
            .withColumn("window_end", col("window_end").cast("string"))
            .orderBy("window_start", "event")
            .collect()  # tiny: windows x events
        )
        if not rows:
            return

        # One print() call for the whole block, so the other queries' console
        # output cannot land between the header and the table
        header = f"Batch: {batch_id}  (COUNTS, {counts_mode} mode)"
        divider = "-" * 43
        table = format_table(batch_df.columns, rows)
        print(f"{divider}\n{header}\n{divider}\n{table}\n", flush=True)

    counts_query = (
        counts_df.writeStream.queryName("event_counts")
        .foreachBatch(print_counts)
        .outputMode(counts_mode)
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "event_counts"))
        .trigger(processingTime=TRIGGER)
        .start()
    )

    return [
        to_console(raw_df, "raw_events"),
        to_console(valid_df, "parsed_events"),
        counts_query,
    ]


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description="PySpark streaming consumer for user-events")
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVER, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read the whole topic instead of only new messages",
    )
    parser.add_argument(
        "--counts-mode",
        choices=["update", "append"],
        default="update",
        help="COUNTS output: running counts every batch (update) or final counts once (append)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Only prune OLD leftover run folders (e.g. from a crash); never a folder
    # that another Spark job might be using right now
    if os.path.isdir(CHECKPOINT_BASE):
        for entry in os.scandir(CHECKPOINT_BASE):
            if time.time() - entry.stat().st_mtime > STALE_CHECKPOINT_SECONDS:
                shutil.rmtree(entry.path, ignore_errors=True)

    # Spark fails on a topic that does not exist yet, so create it up front.
    # This lets you start the Spark job before or after the producer.
    ensure_topic(args.bootstrap, args.topic)

    spark = build_spark()
    offsets = "earliest" if args.from_beginning else "latest"

    queries = start_streams(spark, args.bootstrap, args.topic, offsets, args.counts_mode)

    print(f"\nStreaming from '{args.topic}' on {args.bootstrap} (startingOffsets={offsets})")
    print(f"Queries running: {[q.name for q in queries]}  |  Ctrl+C to stop\n", flush=True)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streams...")
    finally:
        for q in queries:
            q.stop()
        spark.stop()
        shutil.rmtree(CHECKPOINT_ROOT, ignore_errors=True)  # this run's folder only
