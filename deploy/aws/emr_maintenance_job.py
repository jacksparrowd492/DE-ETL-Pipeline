"""
emr_maintenance_job.py
----------------------
Delta table maintenance on EMR Serverless: the cloud counterpart of
maintenance/lakehouse_ops.py (which uses delta-rs on the local file system).
Same three operations, done with Delta's own Spark SQL commands against the
Glue-catalogued tables in S3:

    compact  OPTIMIZE ... WHERE <partition> < today - hot_days   (bin-packing, cold partitions only)
    expire   set delta.logRetentionDuration; Delta deletes older commits at its
             next checkpoint (time travel to them ends)
    vacuum   VACUUM ... RETAIN <hours> HOURS                       (delete unreferenced files)

    spark-submit emr_maintenance_job.py --action compact --table all [--hot-days 1]

Runs with the pipeline's observability: JSON logs to CloudWatch, metrics as log
lines, OpenLineage events, and a Slack/email/SNS alert if an operation fails.
"""

from __future__ import annotations

import argparse
import sys

from governance.lineage import lineage_run
from lakehouse import DATABASE, build_spark
from observability import run_context, setup_logging, track_task
from observability.cloud_secrets import load_secret_env
from observability.logging_config import new_run_id

TABLES = {"user_events": "event_date", "user_events_rejected": "ingest_date"}
LINEAGE_JOBS = {
    "compact": "maintenance.delta_compaction",
    "expire": "maintenance.delta_snapshot_expiry",
    "vacuum": "maintenance.delta_vacuum",
}
MIN_SAFE_VACUUM_HOURS = 1


def compact(spark, table: str, hot_days: int) -> dict:
    partition = TABLES[table]
    row = spark.sql(
        f"OPTIMIZE {DATABASE}.{table} WHERE {partition} < date_sub(current_date(), {int(hot_days)})"
    ).first()
    m = row.metrics if row is not None else None
    return {
        "files_added": int(m.numFilesAdded) if m else 0,
        "files_removed": int(m.numFilesRemoved) if m else 0,
    }


def expire(spark, table: str, retention_days: int) -> dict:
    wanted = f"interval {int(retention_days)} days"
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {DATABASE}.{table}").collect()}
    changed = props.get("delta.logRetentionDuration") != wanted
    if changed:  # only commit when it changes: a metadata commit makes a concurrent MERGE retry
        spark.sql(
            f"ALTER TABLE {DATABASE}.{table} SET TBLPROPERTIES "
            f"('delta.logRetentionDuration' = '{wanted}')"
        )
    return {"retention": wanted, "changed": int(changed)}


def vacuum(spark, table: str, retention_hours: float, force: bool) -> dict:
    if retention_hours < MIN_SAFE_VACUUM_HOURS and not force:
        raise ValueError(f"vacuum below {MIN_SAFE_VACUUM_HOURS}h needs --force (stream stopped)")
    if retention_hours < 168:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM {DATABASE}.{table} RETAIN {float(retention_hours)} HOURS")
    # VACUUM returns the table path, not the files; the count is in the commit history
    end = (
        spark.sql(f"DESCRIBE HISTORY {DATABASE}.{table} LIMIT 5")
        .filter("operation = 'VACUUM END'")
        .first()
    )
    metrics = (end.operationMetrics or {}) if end is not None else {}
    return {"files_deleted": int(metrics.get("numDeletedFiles", 0))}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Delta maintenance on EMR Serverless")
    p.add_argument("--action", choices=["compact", "expire", "vacuum", "all"], required=True)
    p.add_argument("--table", choices=[*TABLES, "all"], default="all")
    p.add_argument("--hot-days", type=int, default=1)
    p.add_argument("--retention-days", type=int, default=7)
    p.add_argument("--vacuum-hours", type=float, default=168)
    p.add_argument("--force", action="store_true")
    p.add_argument(
        "--no-failure-alert",
        action="store_true",
        help="leave failure alerts to the caller (the MWAA DAG's on_failure_callback)",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging("maintenance")
    load_secret_env()

    actions = ["compact", "expire", "vacuum"] if args.action == "all" else [args.action]
    tables = list(TABLES) if args.table == "all" else [args.table]
    spark = build_spark("lakehouse-maintenance", with_kafka=False)

    try:
        with run_context(run_id=new_run_id()):
            for action in actions:
                with lineage_run(LINEAGE_JOBS[action]):
                    for table in tables:
                        with track_task(
                            f"{action}__{table}", alert_on_failure=not args.no_failure_alert
                        ) as t:
                            if action == "compact":
                                result = compact(spark, table, args.hot_days)
                            elif action == "expire":
                                result = expire(spark, table, args.retention_days)
                            else:
                                result = vacuum(spark, table, args.vacuum_hours, args.force)
                            for kind, n in result.items():
                                if isinstance(n, int):
                                    t.count(kind, n)
                            print(f"{action} {table}: {result}", flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
