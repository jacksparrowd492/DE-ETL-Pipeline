"""
fakestore_pipeline.py
"""

from datetime import datetime
import sys

# Add project root to path
sys.path.append("/mnt/f/Files/DE LAB/Exercise5_ETL")

from airflow import DAG
from airflow.operators.python import PythonOperator

from extract.extractor import extract_products
from validation.validator import validate_products
from kafka1.producer import send_products
from replay.replay import replay_failed_records


# -----------------------------
# TASK FUNCTIONS
# -----------------------------


def extract_task(**context):
    products = extract_products()
    context["ti"].xcom_push(key="products", value=products)


def validate_task(**context):
    ti = context["ti"]
    products = ti.xcom_pull(task_ids="extract", key="products")
    validated = validate_products(products)
    ti.xcom_push(key="validated_products", value=validated)


def kafka_task(**context):
    ti = context["ti"]
    validated = ti.xcom_pull(task_ids="validate", key="validated_products")
    send_products(validated)


def replay_task():
    replay_failed_records()


# -----------------------------
# DAG CONFIG
# -----------------------------

default_args = {
    "owner": "batman",
    "retries": 1,
}

with DAG(
    dag_id="fakestore_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # ✅ FIXED HERE
    catchup=False,
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
