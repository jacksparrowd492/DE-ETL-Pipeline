# Real-time user-events streaming (Kafka → PySpark)

This demo runs entirely on your own machine, without Docker. A Python producer prints each JSON event and sends it to the Kafka topic `user-events`. A PySpark Structured Streaming job reads that topic and prints three live console streams:

| Stream   | What it shows                                                               |
|----------|-----------------------------------------------------------------------------|
| `RAW`    | Each Kafka message as received, with the binary value cast to a string, plus its partition and offset |
| `PARSED` | Fields parsed with `from_json()` against a `StructType` schema. Invalid rows are filtered out |
| `COUNTS` | The number of events of each type in 10-second tumbling windows            |

All three streams run a micro-batch every 5 seconds. `RAW` and `PARSED` use `outputMode("append")` with the console sink. `COUNTS` prints running counts in `update` mode by default, and it skips batches with nothing to show instead of printing empty tables.

## Prerequisites

- Java 17, Python 3.10+, `pip install pyspark==3.5.1 kafka-python`
- Kafka 3.9 extracted to `F:\Kafka\kafka_2.13-3.9.2`. This demo runs it in KRaft mode, so ZooKeeper isn't needed.
- `HADOOP_HOME` pointing to a folder whose `bin\` contains `winutils.exe` and `hadoop.dll`. Here that's `F:\hadoop`.
- The first Spark run downloads `spark-sql-kafka-0-10_2.12:3.5.1` into `%USERPROFILE%\.ivy2`. Later runs use that cached copy.

## Quick start: Spark and producer output in ONE terminal

Start Kafka first (Terminal 1 below). Then run:

```powershell
cd "F:\Files\DE LAB\ETL-Pipeline"
python streaming\run_demo.py                # runs until Ctrl+C
python streaming\run_demo.py --count 50     # send 50 messages, print the results, then stop
python streaming\run_demo.py --rate 200     # high velocity
```

`run_demo.py` starts the Spark job and waits until its queries are running. Then it starts the producer. Both outputs are forwarded to your terminal, and each line is prefixed with `[SPARK]` or `[PRODUCER]`.

## Run it (3 terminals, PowerShell)

Spark's tables appear **only in the Spark terminal**, and only while it runs alongside the producer.

### Terminal 1: Kafka broker

```powershell
cd F:\Kafka\kafka_2.13-3.9.2

# ONE-TIME ONLY (skip if win-logs\ already exists): format the storage dir
$id = .\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format -t $id -c .\config\kraft\server-windows.properties

# Start the broker (leave this window open)
.\bin\windows\kafka-server-start.bat .\config\kraft\server-windows.properties
```

`server-windows.properties` should contain these settings:

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
log.dirs=F:/Kafka/kafka_2.13-3.9.2/win-logs
```

(Optional) Create the topic by hand. Both scripts also create it automatically:

```powershell
.\bin\windows\kafka-topics.bat --create --topic user-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

### Terminal 2: Spark streaming job

```powershell
cd "F:\Files\DE LAB\ETL-Pipeline"
python streaming\spark_streaming.py
# add --from-beginning to replay everything already in the topic
```

Wait until it prints `Queries running: [...]`.

### Terminal 3: Producer

```powershell
cd "F:\Files\DE LAB\ETL-Pipeline"
python streaming\producer.py               # 5 msg/s, readable
python streaming\producer.py --rate 200    # high velocity
python streaming\producer.py --rate 0      # as fast as possible
python streaming\producer.py --count 100   # stop after 100
python streaming\producer.py --bad-ratio 0 # no deliberately bad messages
```

Press `Ctrl+C` in each terminal to stop.

## What to look for

- By default, about 10% of messages are deliberately bad: a `null` user_id, a missing `event`, or truncated JSON. They appear in yellow on the producer side and in the `RAW` stream, but never in `PARSED` or `COUNTS`.
- By default (`update` mode), each batch's `COUNTS` table shows the running totals for every window that changed in that batch. The same window can appear in several batches as its counts grow.
- Add `--counts-mode append` to `spark_streaming.py` or `run_demo.py` to print each window only once, with its final counts. A window closes when later events push the watermark past `window end + 5s`, which is about 15–20 seconds after the window ends while the producer is running. If you stop the producer, the last window is held back until more data arrives.
- Each Spark run writes checkpoints to its own folder, `streaming\_checkpoints\run-<time>-<pid>\`, and deletes that folder when it stops. Several Spark jobs can run at once without breaking each other.

## Week 2: Lakehouse storage (Delta Lake + Hive Metastore)

`spark_delta.py` reads the same `user-events` topic and writes the stream into a **Delta Lake** table instead of the console. The table is registered in a **Hive Metastore** catalog.

```
Kafka user-events ──► spark_delta.py (micro-batch every 10 s)
                        ├─ valid   ──► MERGE INTO lakehouse.user_events          PARTITIONED BY event_date
                        └─ invalid ──► append     lakehouse.user_events_rejected PARTITIONED BY ingest_date
```

| Requirement | How it's met |
|---|---|
| **Table catalog (Hive Metastore)** | `enableHiveSupport()` with an embedded Hive Metastore stored in `_lakehouse/metastore_db`. `spark_catalog` is Delta's `DeltaCatalog`. Tables are registered by name (`lakehouse.user_events`) and survive restarts. Moving to a shared metastore service only needs `hive.metastore.uris` set; the code stays the same. |
| **Proper partitioning** | `PARTITIONED BY (event_date)`, where `event_date = to_date(event_time)` uses the time the event **happened**, not when it arrived. Each day gets its own folder, so `WHERE event_date = …` only reads that folder. `spark_verify.py` shows the `PartitionFilters` in the query plan. |
| **Write-ahead log (WAL)** | The checkpoint's `offsets/N` is written **before** batch N runs. The Delta commit (`_delta_log/000…N.json`) is atomic. `commits/N` is written only **after** the commit. After a crash, Spark replays every batch that has an `offsets/` entry but no matching `commits/` entry. |
| **Exactly-once on replay** | Events are loaded with an insert-only `MERGE` on `event_id`, which the producer now sends as a UUID. Rejected messages are appended with Delta's `txnAppId`/`txnVersion`, so a replayed batch is skipped. |

The checkpoint and the tables are kept between runs. Restarting `spark_delta.py` continues from the last committed Kafka offsets.

### Run it

Start Kafka (Terminal 1 above), then:

```powershell
cd "F:\Files\DE LAB\ETL-Pipeline"
pip install delta-spark==3.2.0             # once; matches PySpark 3.5.x

# Terminal 2: stream into the lakehouse (--reset = start from an empty lakehouse)
python streaming\spark_delta.py --reset

# Terminal 3: producer
python streaming\producer.py --count 300 --rate 50
```

Each micro-batch prints a line like this:

```
[batch 2] valid=96 inserted=96 duplicates_skipped=0 rejected=10 -> lakehouse.user_events v3, partitions ['2026-09-23']
```

Or run it in a single terminal: `python streaming\run_demo.py --lakehouse --count 300`.

### Verify it

```powershell
python streaming\spark_verify.py          # after stopping spark_delta.py: full check through the Hive Metastore
python streaming\spark_verify.py --live   # while spark_delta.py is still running
```

`spark_verify.py` prints one section for each requirement:

1. The tables in the metastore (provider `delta`, `MANAGED`, their locations).
2. `partitionColumns`, rows per partition, the partition folders on disk, and partition pruning in the query plan.
3. The checkpoint's `offsets/` and `commits/` entries, the Kafka offsets logged for the last batch, the `_delta_log/` files, and `DESCRIBE HISTORY`.
4. A duplicate check (`total_rows = distinct_event_ids`) and the rejected messages grouped by reason.
5. Time travel (`VERSION AS OF n`).

The embedded metastore (Derby) allows **one Spark process at a time**. While the stream is running, `--live` reads the tables straight from their storage path. That works because a Delta table stores its own schema and partitioning in `_delta_log/`.

### Demonstrate crash recovery (WAL replay)

1. Let `spark_delta.py` process a few batches, then close its window (a hard kill, not Ctrl+C).
2. Delete the newest file in `streaming\_lakehouse\checkpoints\user_events\commits\` and its `.N.crc`. That recreates a crash that happened after the Delta commit but before Spark recorded the batch as done.
3. Start `python streaming\spark_delta.py` again (**without** `--reset`). It replays that batch from `offsets/N`:
   ```
   [batch 1] valid=6208 inserted=0 duplicates_skipped=6208 rejected=694 -> lakehouse.user_events v2, ...
   ```
   The table version doesn't change and no rows are duplicated. `spark_verify.py` still reports `duplicates = 0`.

### Product tracking

Shopping events (`page_view`, `click`, `add_to_cart`, `remove_from_cart`, `purchase`) carry a
`product_id` (FakeStore ids 1-20, the same ids as `silver_products` in Databricks). `login` and
`logout` have none. Each simulated user has a cart, so `remove_from_cart` and `purchase` take a
product that user added earlier when there is one.

* `spark_delta.py` stores it in `lakehouse.user_events.product_id`. On start it adds the column
  to an existing table (`ALTER TABLE ... ADD COLUMNS`); older rows read it as `NULL`.
* `python streaming\load_products.py` writes the catalog to the Delta table `lakehouse.products`
  with delta-rs (no Spark, so it can run while the stream is up). It reads the FakeStore API,
  or `scripts/fixtures/fakestore_products.json` when offline (`--offline`, 10 products only).
* Trino joins the two in `delta.lakehouse.v_product_activity` (`serving/views.sql`), which the
  Metabase *Product activity* section and the Streamlit **Products** page read.

After upgrading: restart `spark_delta.py` once, run `load_products.py`, then
`TRINO_ACTION=restart bash serving/week3_up.sh --rebuild` in WSL.

### Files

```
streaming/
  lakehouse.py       shared Spark session (Delta + Hive Metastore), paths, table DDL
  spark_delta.py     Kafka -> Delta streaming sink (foreachBatch + MERGE)
  spark_verify.py    catalog / partitioning / WAL / idempotency / time-travel checks
  load_products.py   FakeStore catalog -> Delta table lakehouse.products (delta-rs)
  _lakehouse/        (git-ignored) metastore_db, warehouse, checkpoints
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `UnsatisfiedLinkError ... NativeIO$Windows` | `HADOOP_HOME\bin` is missing `hadoop.dll`/`winutils.exe`, or they don't match Hadoop 3.3.x |
| `NoBrokersAvailable` from the producer | Terminal 1 isn't running, or it isn't listening on `localhost:9092` |
| `Invalid receive (size = 1195725856 ...)` in the broker log | Something sent HTTP to port 9092, for example a browser. It's harmless |
| `Failed to delete ... spark-xxxx\userFiles-...\*.jar` when Spark stops | Harmless on Windows. The jar is still locked when Spark cleans up its temp folder |
| `SUCCESS: The process with PID ... has been terminated` when Spark stops | Normal. On Windows, PySpark runs `taskkill` to shut down its Java process |
| `Another instance of Derby may have already booted the database` | `spark_delta.py` is still using the Hive Metastore. Stop it, or run `spark_verify.py --live` |
| Old batches are skipped after you delete the tables | The checkpoint and the tables belong together. Use `spark_delta.py --reset` to clear both |
| Kafka won't start after a crash (`AccessDeniedException` on log files) | Stop Kafka, delete `win-logs\`, then run the one-time format step again |
| Kafka shuts down ~20 s after starting: `all log dirs in ...win-logs have failed`, `FileSystemException ... .index.deleted ... being used by another process` | Data older than `log.retention.hours` (7 days) is being deleted, and Windows can't rename open segment files. Set `log.retention.hours=-1` in `server-windows.properties` and restart. Don't wipe `win-logs\`, because the Spark checkpoint still points at those offsets |
| `KafkaTimeoutError: Unable to bootstrap from localhost:9092` | Kafka isn't running. Start Terminal 1 first, and check that it stays up |
| `UnsupportedOperationException: getSubject is not supported` when Spark starts | The default `java` is too new (Java 23+). Before starting Spark, set `$env:JAVA_HOME` to a Java 17 JDK and put its `bin` first on `PATH` |
