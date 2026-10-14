# Week 4 – Airflow automation & performance optimization

```
spark_delta.py ──► Delta tables (streaming/_lakehouse/warehouse/lakehouse.db)
  (every 10 s)        ▲               │
                      │               └──► Trino 483 ──► Metabase / Power BI
       Airflow 3.3 (WSL) ── maintenance/dags/lakehouse_maintenance.py
         delta_compaction       hourly   bin-packing OPTIMIZE
         delta_snapshot_expiry  02:00    expire old table versions
         delta_vacuum           03:00    delete unreferenced data files
         lakehouse_optimize_and_benchmark  (manual) benchmark → maintenance → benchmark → report
```

## Run it (WSL Ubuntu, no Docker)

```bash
bash "/mnt/f/Files/DE LAB/ETL-Pipeline/maintenance/setup_airflow.sh"   # once
bash "/mnt/f/Files/DE LAB/ETL-Pipeline/serving/start_trino.sh"         # Trino on :8080
bash "/mnt/f/Files/DE LAB/ETL-Pipeline/maintenance/start_airflow.sh"   # Airflow on :8081
bash "/mnt/f/Files/DE LAB/ETL-Pipeline/maintenance/start_airflow.sh" stop
```

* UI: http://localhost:8081. User `admin`; the password is in
  `~/airflow/simple_auth_manager_passwords.json.generated`.
* `setup_airflow.sh` uses `uv` to create `~/airflow-venv` (Python 3.12, Airflow 3.3.2 with
  the official constraints, `deltalake`, `trino`). It also writes `~/airflow/env.sh`, which sets
  `dags_folder` to `maintenance/dags`, the SQLite DB and the API settings. Airflow's own
  database stays on the Linux file system because SQLite locking does not work on `/mnt/f`.
* Trino already uses port 8080, so the Airflow API server **and** the Execution API URL that
  task workers call are moved to 8081. If only the port is changed, tasks stay `queued`.
* Every maintenance task uses the 1-slot pool `lakehouse_maintenance`, so two maintenance
  jobs never rewrite a table at the same time. Failed tasks retry twice, 5 minutes apart.
  A commit conflict with the stream is retried this way.

By hand, without Airflow:

```bash
source ~/airflow/env.sh && cd "/mnt/f/Files/DE LAB/ETL-Pipeline"
python maintenance/lakehouse_ops.py stats
python maintenance/lakehouse_ops.py compact  [--table user_events] [--hot-days 1]
python maintenance/lakehouse_ops.py expire   [--retention-days 7]
python maintenance/lakehouse_ops.py vacuum   [--retention-hours 168] [--dry-run] [--force]
python maintenance/benchmark.py run --label before | after
python maintenance/benchmark.py compare before after [--report benchmark_report.md]
# benchmark an older layout via Delta time travel (until vacuum deletes its files):
python maintenance/benchmark.py run --label old --as-of user_events=456 --as-of user_events_rejected=451
```

Tests: `pytest tests/test_lakehouse_ops.py tests/test_benchmark.py` runs anywhere (tiny Delta
tables in a temp dir, fake Trino cursor). `tests/test_maintenance_dags.py` needs Airflow, so it
only runs in the WSL venv: `source ~/airflow/env.sh && python -m pytest tests/test_maintenance_dags.py`.

## Design

**Engine: delta-rs, not Trino.** Trino's Delta connector refuses to write to a local file
system:

> Writes are not enabled on the local filesystem … `delta.enable-non-concurrent-writes`

Trino cannot commit atomically on local storage while Spark is writing the same table, so
that switch stays off. `deltalake` (delta-rs) commits with an atomic create-if-absent, so it
can run next to the stream. Trino remains the query engine that the benchmark measures.
Compatibility was checked: Spark 3.5 / Delta 3.2 reads the tables after compaction, expiry
and vacuum, and can still append to them. The protocol stays at reader 1 / writer 2.

**Delta has no separate snapshot objects** (unlike Iceberg). A snapshot is one table
version, which is one `_delta_log/N.json` commit:

| Task | What it does | Safety |
|---|---|---|
| **Compaction** (`delta_compaction`, hourly) | Delta `OPTIMIZE`: bin-packs the small files of each daily partition into files of up to 128 MB. A short-circuit task skips a table that has fewer than `min_small_files` (10) small files to compact. | The newest `hot_days` (1) partitions are skipped. The stream's MERGE reads exactly those partitions, and removing files it has read would fail its commit (`ConcurrentDeleteReadException`). |
| **Snapshot expiry** (`delta_snapshot_expiry`, daily) | Sets `delta.logRetentionDuration` (`retention_days`, 7) and deletes older commits. Spark's own log cleanup uses the same table property. Also deletes the hidden Hadoop checksum files (`.N.json.crc`) of removed commits, which delta-rs leaves behind. | Only commits up to a checkpoint that is itself past the retention are removed, so the current state never changes. Time travel to removed versions ends. The property is only rewritten when it changes, because that metadata commit would make a concurrent MERGE retry. |
| **Vacuum** (`delta_vacuum`, daily after expiry) | Deletes data files no longer in the current snapshot, such as the small files replaced by compaction, once they have been unreferenced for `delta.deletedFileRetentionDuration` (7 days). | A retention below 1 hour needs `force: true`. Below that, vacuum could delete files Spark has written but not committed yet, so use it only with `spark_delta.py` stopped. |

## Benchmark: before vs after (2026-10-05)

`lakehouse_optimize_and_benchmark` ran in Airflow with the stream stopped, so both runs
saw the same data. Each query ran 3 warm-up and 15 measured runs; the table shows medians.
The queries are the Metabase dashboard queries plus a one-partition query and a point lookup.
Full numbers are in [`results/`](results/): `benchmark_before_2026-10-05.json`,
`benchmark_after_2026-10-05.json` and [`benchmark_report_2026-10-05.md`](results/benchmark_report_2026-10-05.md).
Each run of the DAG writes its own files, named by the `tag` param (default: the UTC date),
and the report also lists what each maintenance step did.

| Table | Rows | Data files before → after | Size before → after |
|---|---:|---:|---:|
| user_events | 6,541 | 130 → **2** | 498 KB → 120 KB |
| user_events_rejected | 745 | 303 → **2** | 684 KB → 18 KB |

| Query | Before ms | After ms | Speed-up | Trino CPU ms | Splits |
|---|---:|---:|---:|---:|---:|
| total_events | 707 | 458 | 1.5x | 20 → 1 | 139 → 11 |
| kpi_tiles | 761 | 507 | 1.5x | 286 → 11 | 163 → 35 |
| events_by_type | 642 | 466 | 1.4x | 171 → 6 | 156 → 28 |
| events_per_minute | 838 | 475 | 1.8x | 241 → 7 | 156 → 28 |
| user_activity_matrix | 828 | 475 | 1.7x | 238 → 6 | 156 → 28 |
| ingest_latency_p95 | 638 | 459 | 1.4x | 200 → 5 | 139 → 11 |
| data_quality_pct | 2112 | 904 | 2.3x | 76 → 2 | 459 → 30 |
| rejected_by_reason | 747 | 469 | 1.6x | 265 → 4 | 329 → 28 |
| one_day_partition | 624 | 454 | 1.4x | 157 → 2 | 139 → 17 |
| point_lookup | 455 | 455 | 1.0x | 3 → 2 | 1 → 1 |
| **all 10 queries** | **8,352** | **5,121** | **1.6x** | | |

What the numbers show:

* **Small files, not data volume, were the cost.** Trino creates one split per file, so 130
  files of ~4 KB meant ~140–160 splits and up to 286 ms of CPU for 6,541 rows. After
  compaction the same queries use 1–11 ms of CPU, 20–80x less.
* **Response time only improved 1.4–2.3x**, because about 450 ms of every query is fixed
  overhead (planning, scheduling, client polling) that does not depend on the data. The query
  that reads both tables (`data_quality_pct`, 459 splits) gained the most.
* **The point lookup did not change.** Delta's per-file min/max statistics already pruned it
  to one file before compaction.
* Compaction also shrank the data (1.18 MB → 138 KB). Parquet compresses large row groups
  much better than ~100-row files.
* With the stream running all day (~8,600 micro-batches), these numbers grow with the file
  count. The hourly compaction DAG keeps every finished day at one file per partition.

## Recurring runs and second benchmark (2026-10-12)

**The schedules ran on their own.** On 2026-10-12, as soon as Airflow came back up, it ran the
overdue schedules (stream stopped, 22,079 rows). Values are from the Airflow task logs:

| DAG run | user_events | user_events_rejected |
|---|---|---|
| `delta_compaction` | 327 → 3 data files (4.1 KB → 134 KB average) | 798 → 3 data files |
| later hourly `delta_compaction` runs | skipped by the short-circuit (< 10 small files) | skipped |
| `delta_vacuum`, default 7-day retention | 130 files / 498 KB deleted | 303 files / 684 KB deleted |
| `delta_snapshot_expiry`, 7 days | 0 commits removed: the compaction commit's post-commit log cleanup had already removed versions 60–119 | same |

The vacuum deleted exactly the 433 files that the 2026-10-05 compaction had replaced, now
that they were older than 7 days. A second vacuum run counted the same 303 files again
(0 bytes freed): delta-rs lists candidates from the log's tombstones even after the files
are gone. `vacuum()` now only counts files that are still on disk.

**Compaction, measured with time travel.** Compaction had already run before a "before"
benchmark could be taken. Its replaced files are kept for 7 days, though, so the old layout
could still be read with `--as-of user_events=456 --as-of user_events_rejected=451`
(`FOR VERSION AS OF`: same rows, 1,125 small files). The comparison is in
[`benchmark_report_2026-10-12.md`](results/benchmark_report_2026-10-12.md):

* **The engine work collapsed.** There were 1,151 → 32 splits, and Trino CPU fell 20–470x
  (for example `rejected_by_reason` went from 3,811 to 8 ms).
* **Response time only improved 1.2x in total**, and one query was slower (0.6x). Every query
  now spent about 1.4 s before any data was read.

**Why: the `_delta_log`, not the data.** Trino's query stats showed `analysisTime` 1.6–2.0 s
and execution about 40 ms, and `SELECT 1` takes 21 ms. To find the latest version, Trino lists
`_delta_log` and stats each entry, and each `stat` over the WSL `/mnt/f` (9P) mount costs a
few ms. At 7-day retention the log held 340 commits but 869 entries. Most of the rest were
hidden Hadoop `.crc` sidecars, and 113 of those belonged to commits that expiry had already
deleted. On 2026-10-05 the log was about 3x smaller, which is why the fixed overhead was
only about 450 ms then.

**Snapshot expiry removes that cost.** To measure it, both compacted tables were copied
(`cp -a`) and registered as `delta.bench.*`. The copy then went from 7-day to 1-day log
retention plus orphaned-`.crc` cleanup. Results are in
[`benchmark_report_2026-10-12_log_expiry.md`](results/benchmark_report_2026-10-12_log_expiry.md):

| Copy of both tables | `_delta_log` entries | Median per query | All 10 queries |
|---|---:|---:|---:|
| 7-day log retention | 869 / 860 | 1,333–2,604 ms | 15,113 ms |
| 1-day retention + `.crc` cleanup | 24 / 15 | 173–327 ms | 2,028 ms (**7.5x faster**) |

Same files, same splits, same CPU. Only the log got shorter. On this setup the log
retention is the biggest single performance setting. It is a trade-off against time
travel: at 1 day, versions older than about a day are gone. The real tables keep the default
of 7 days. To change it, set `retention_days` on `delta_snapshot_expiry`. With the stream
running all day (one commit per 10-second micro-batch), the 7-day log would be far larger
than here, so a short retention matters even more then.

## Files

```
maintenance/
  lakehouse_ops.py            compaction / snapshot expiry / vacuum / stats (delta-rs)
  benchmark.py                Trino benchmark + before/after report
  dags/lakehouse_maintenance.py   the four Airflow DAGs
  setup_airflow.sh            one-time Airflow install in WSL (uv, no sudo, no Docker)
  start_airflow.sh            start / stop Airflow standalone on :8081
  results/                    benchmark_{before,after}_<date>.json, benchmark_report_<date>.md
  logs/                       (git-ignored) airflow.log
```
