"""
Week 4: Airflow DAGs for recurring maintenance of the Delta Lake tables that
streaming/spark_delta.py writes (lakehouse.user_events, lakehouse.user_events_rejected).

    delta_compaction                  hourly  bin-packing OPTIMIZE of cold partitions
    delta_snapshot_expiry             02:00   delete commits older than the log retention
    delta_vacuum                      03:00   delete data files no snapshot references
    lakehouse_optimize_and_benchmark  manual  Trino benchmark -> all three -> benchmark again

All maintenance tasks share the 1-slot pool "lakehouse_maintenance", so two of them
never rewrite the same table at the same time. The work itself is in
maintenance/lakehouse_ops.py and maintenance/benchmark.py.

Phase II observability: a task that fails for good sends a Slack/email alert
(on_failure_callback), a retry sends a warning (on_retry_callback), and each
maintenance task records its duration and emits OpenLineage events (the table is
both input and output: the job rewrites files, never rows).
"""

import contextlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# project root, for the shared observability / governance packages
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import benchmark  # noqa: E402
import lakehouse_ops as ops  # noqa: E402
from governance.lineage import lineage_run  # noqa: E402
from observability import run_context, setup_logging, track_task  # noqa: E402
from observability.alerts import airflow_failure_callback, airflow_retry_callback  # noqa: E402

POOL = "lakehouse_maintenance"
START = datetime(2026, 10, 1)
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,  # a commit conflict with the streaming job is retried
    "retry_delay": timedelta(minutes=5),
    "pool": POOL,
    "on_failure_callback": airflow_failure_callback,
    "on_retry_callback": airflow_retry_callback,
}
TAGS = ["week4", "delta", "maintenance"]


def _params() -> dict:
    return get_current_context()["params"]


def _observed(task: str, lineage_job: str):
    """Logging + metrics + lineage around one maintenance task try. Failure
    alerts come from on_failure_callback, which knows whether a retry follows."""
    setup_logging("airflow_maintenance", console=False)
    ctx = get_current_context()
    stack = contextlib.ExitStack()
    stack.enter_context(run_context(run_id=ctx["run_id"], component="airflow_maintenance"))
    stack.enter_context(lineage_run(lineage_job))
    stack.enter_context(track_task(task, alert_on_failure=False))
    return stack


# --------------------------------------------------
# 1) File compaction (bin-packing)
# --------------------------------------------------


@dag(
    dag_id="delta_compaction",
    schedule="15 * * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=TAGS,
    params={
        "hot_days": Param(
            1,
            type="integer",
            minimum=0,
            description="Newest daily partitions to leave alone (still written by the stream). 0 = all, only with the stream stopped.",
        ),
        "min_small_files": Param(
            10,
            type="integer",
            minimum=2,
            description="Compact a table only when its cold partitions have at least this many small files.",
        ),
    },
    doc_md="""
Spark writes a few small Parquet files every 10-second micro-batch, and Trino opens
one split per file. This DAG bin-packs the files of each *cold* daily partition
into files of up to 128 MB (Delta `OPTIMIZE`). The newest partition is skipped so
the rewrite never conflicts with the stream's MERGE. If a table has fewer than
`min_small_files` small files to compact, its compaction task is skipped.
""",
)
def delta_compaction():
    for table in ops.TABLES:

        @task.short_circuit(task_id=f"needs_compaction__{table}")
        def needs_compaction(table: str = table) -> bool:
            p = _params()
            small = ops.cold_small_files(table, p["hot_days"])
            print(
                f"{table}: {small} small files in cold partitions (threshold {p['min_small_files']})"
            )
            return small >= p["min_small_files"]

        @task(task_id=f"compact__{table}")
        def compact(table: str = table) -> dict:
            with _observed(f"compact__{table}", "maintenance.delta_compaction"):
                result = ops.compact(table, hot_days=_params()["hot_days"])
            print(result)
            return result

        needs_compaction() >> compact()


delta_compaction()


# --------------------------------------------------
# 2) Snapshot expiry
# --------------------------------------------------


@dag(
    dag_id="delta_snapshot_expiry",
    schedule="0 2 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=TAGS,
    params={
        "retention_days": Param(
            7,
            type="integer",
            minimum=1,
            description="Keep table versions (time travel) for this many days.",
        ),
    },
    doc_md="""
A Delta snapshot is one `_delta_log/N.json` commit. Each 10-second micro-batch adds
one, up to ~8,600 commits a day while the stream runs. This DAG sets `delta.logRetentionDuration` and
removes commits older than it, up to a checkpoint that is itself past the retention.
The current table is never affected; time travel to the removed versions ends.
""",
)
def delta_snapshot_expiry():
    for table in ops.TABLES:

        @task(task_id=f"expire_snapshots__{table}")
        def expire(table: str = table) -> dict:
            with _observed(f"expire_snapshots__{table}", "maintenance.delta_snapshot_expiry"):
                result = ops.expire_snapshots(table, retention_days=_params()["retention_days"])
            print(result)
            return result

        expire()


delta_snapshot_expiry()


# --------------------------------------------------
# 3) Vacuum
# --------------------------------------------------


@dag(
    dag_id="delta_vacuum",
    schedule="0 3 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=TAGS,
    params={
        "retention_hours": Param(
            None,
            type=["null", "number"],
            minimum=0,
            description="Empty = the table's delta.deletedFileRetentionDuration (7 days).",
        ),
        "dry_run": Param(
            False, type="boolean", description="Only list the files that would be deleted."
        ),
        "force": Param(
            False,
            type="boolean",
            description="Allow a retention below 1 hour. Only with spark_delta.py stopped.",
        ),
    },
    doc_md="""
After compaction the replaced small files are still on disk: older snapshots
reference them. VACUUM deletes data files that the current snapshot no longer
references once they have been unreferenced longer than the retention.
""",
)
def delta_vacuum():
    for table in ops.TABLES:

        @task(task_id=f"vacuum__{table}")
        def vacuum(table: str = table) -> dict:
            p = _params()
            with _observed(f"vacuum__{table}", "maintenance.delta_vacuum"):
                result = ops.vacuum(
                    table, p["retention_hours"], dry_run=p["dry_run"], force=p["force"]
                )
            print(result)
            return result

        vacuum()


delta_vacuum()


# --------------------------------------------------
# 4) Before/after benchmark around one full maintenance pass
# --------------------------------------------------


@dag(
    dag_id="lakehouse_optimize_and_benchmark",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=[*TAGS, "benchmark", "trino"],
    params={
        "tag": Param(
            None,
            type=["null", "string"],
            description="Names the result files (benchmark_before_<tag>.json, benchmark_report_<tag>.md). Empty = today's UTC date.",
        ),
        "runs": Param(15, type="integer", minimum=1),
        "warmup": Param(3, type="integer", minimum=0),
        "hot_days": Param(1, type="integer", minimum=0),
        "retention_days": Param(7, type="integer", minimum=1),
        "vacuum_retention_hours": Param(None, type=["null", "number"], minimum=0),
        "force": Param(
            False, type="boolean", description="Allow vacuum below 1 hour (stream stopped)."
        ),
    },
    doc_md="""
Benchmarks the Trino queries of the dashboard, runs compaction, snapshot expiry and
vacuum on both tables, then benchmarks again and writes
`maintenance/results/benchmark_report_<tag>.md`, including what each maintenance step did.
Earlier runs are kept: every run writes its own `<tag>` files.
""",
)
def lakehouse_optimize_and_benchmark():
    @task
    def run_tag() -> str:
        return _params()["tag"] or datetime.now(timezone.utc).date().isoformat()

    @task
    def benchmark_before(tag: str) -> str:
        p = _params()
        return benchmark.run(f"before_{tag}", p["runs"], p["warmup"])

    @task
    def compact_tables() -> list:
        return [ops.compact(t, hot_days=_params()["hot_days"]) for t in ops.TABLES]

    @task
    def expire_snapshots() -> list:
        return [
            ops.expire_snapshots(t, retention_days=_params()["retention_days"]) for t in ops.TABLES
        ]

    @task
    def vacuum_tables() -> list:
        p = _params()
        return [ops.vacuum(t, p["vacuum_retention_hours"], force=p["force"]) for t in ops.TABLES]

    @task
    def benchmark_after(tag: str) -> str:
        p = _params()
        return benchmark.run(f"after_{tag}", p["runs"], p["warmup"])

    @task
    def report(tag: str, compaction: list, expiry: list, vacuum: list) -> str:
        return benchmark.compare(
            f"before_{tag}",
            f"after_{tag}",
            report=f"benchmark_report_{tag}.md",
            maintenance={"compaction": compaction, "snapshot_expiry": expiry, "vacuum": vacuum},
        )

    tag = run_tag()
    compacted = compact_tables()
    expired = expire_snapshots()
    vacuumed = vacuum_tables()
    after = benchmark_after(tag)
    benchmark_before(tag) >> compacted >> expired >> vacuumed >> after
    after >> report(tag, compacted, expired, vacuumed)


lakehouse_optimize_and_benchmark()
