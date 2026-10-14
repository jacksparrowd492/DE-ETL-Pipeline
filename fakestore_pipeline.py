"""
fakestore_pipeline.py
---------------------
Airflow DAG: extract -> validate -> send_to_kafka -> replay_failed.

Observability (Phase II)
    logging   every task logs to Airflow's task log and to logs/airflow_tasks.{log,jsonl},
              tagged with the DAG run_id and task (observability.setup_logging)
    metrics   duration / success / record counts / SLA per task (logs/metrics.jsonl)
    alerts    on_failure_callback -> Slack + email when a task fails for good;
              on_retry_callback   -> warning when a task fails but will retry;
              latency-SLA and data-quality warnings from inside the tasks
    lineage   OpenLineage START/COMPLETE/FAIL per task (governance/lineage.py)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pipeline_tasks
from observability import run_context, setup_logging
from observability.alerts import airflow_failure_callback, airflow_retry_callback
from observability.cloud_secrets import load_secret_env


# -----------------------------
# TASK FUNCTIONS
# -----------------------------


def _context(context):
    """Log setup + run context for one task try. Airflow already shows the
    console in the task log, so only the file handlers are added."""
    setup_logging("airflow_tasks", console=False)
    load_secret_env()  # MWAA: secret settings from Secrets Manager (no-op locally)
    return run_context(run_id=context["run_id"], component="airflow_tasks")


def extract_task(**context):
    with _context(context):
        products = pipeline_tasks.extract(alert_on_failure=False)
    context["ti"].xcom_push(key="products", value=products)


def validate_task(**context):
    ti = context["ti"]
    products = ti.xcom_pull(task_ids="extract", key="products")
    with _context(context):
        validated = pipeline_tasks.validate(products, alert_on_failure=False)
    ti.xcom_push(key="validated_products", value=validated)


def kafka_task(**context):
    ti = context["ti"]
    validated = ti.xcom_pull(task_ids="validate", key="validated_products")
    with _context(context):
        return pipeline_tasks.send_to_kafka(validated, alert_on_failure=False)


def replay_task(**context):
    with _context(context):
        return pipeline_tasks.replay_failed(alert_on_failure=False)


# -----------------------------
# DAG CONFIG
# -----------------------------

default_args = {
    "owner": "batman",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": airflow_failure_callback,
    "on_retry_callback": airflow_retry_callback,
}

with DAG(
    dag_id="fakestore_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["fakestore", "batch", "kafka", "databricks"],
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
    )

    kafka = PythonOperator(
        task_id="send_to_kafka",
        python_callable=kafka_task,
    )

    replay = PythonOperator(
        task_id="replay_failed",
        python_callable=replay_task,
    )

    extract >> validate >> kafka >> replay
