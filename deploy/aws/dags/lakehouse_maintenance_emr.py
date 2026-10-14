"""
lakehouse_maintenance_emr.py
----------------------------
MWAA version of maintenance/dags/lakehouse_maintenance.py: the same three DAGs on
the same schedules, but each task submits deploy/aws/emr_maintenance_job.py to
EMR Serverless (Delta's OPTIMIZE / log retention / VACUUM on S3) instead of
running delta-rs against a local folder.

    delta_compaction       hourly :15   compact cold partitions
    delta_snapshot_expiry  02:00        log retention -> old commits removed
    delta_vacuum           03:00        delete unreferenced data files

Settings come from the MWAA startup script (deploy/aws/mwaa/startup.sh.tpl).
A task that fails for good alerts through Slack/email/SNS (on_failure_callback);
the EMR job itself runs with --no-failure-alert so a failure alerts once.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.sdk import dag

from emr_settings import maintenance_job_driver, monitoring
from observability.alerts import airflow_failure_callback, airflow_retry_callback

TABLES = ("user_events", "user_events_rejected")
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,  # a commit conflict with the streaming job is retried
    "retry_delay": timedelta(minutes=5),
    "pool": "lakehouse_maintenance",  # create it once in the MWAA UI: 1 slot
    "on_failure_callback": airflow_failure_callback,
    "on_retry_callback": airflow_retry_callback,
}
COMMON = {
    "start_date": datetime(2026, 10, 1),
    "catchup": False,
    "max_active_runs": 1,
    "default_args": DEFAULT_ARGS,
    "tags": ["delta", "maintenance", "emr-serverless"],
}


def emr_task(task_id: str, action: str, table: str, extra: list | None = None):
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=os.environ.get("EMR_APPLICATION_ID", ""),
        execution_role_arn=os.environ.get("EMR_JOB_ROLE_ARN", ""),
        job_driver=maintenance_job_driver(action, table, ["--no-failure-alert", *(extra or [])]),
        configuration_overrides=monitoring(f"maintenance/{action}"),
        name=f"{action}-{table}",
        wait_for_completion=True,
    )


@dag(dag_id="delta_compaction", schedule="15 * * * *", **COMMON)
def delta_compaction():
    for table in TABLES:
        emr_task(f"compact__{table}", "compact", table, ["--hot-days", "1"])


@dag(dag_id="delta_snapshot_expiry", schedule="0 2 * * *", **COMMON)
def delta_snapshot_expiry():
    for table in TABLES:
        emr_task(f"expire_snapshots__{table}", "expire", table, ["--retention-days", "7"])


@dag(dag_id="delta_vacuum", schedule="0 3 * * *", **COMMON)
def delta_vacuum():
    for table in TABLES:
        emr_task(f"vacuum__{table}", "vacuum", table, ["--vacuum-hours", "168"])


delta_compaction()
delta_snapshot_expiry()
delta_vacuum()
