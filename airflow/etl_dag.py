"""
etl_dag.py
----------
Apache Airflow DAG for the FakeStore ETL Pipeline.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from extract.extractor import extract_products
from validation.validator import validate_products
from kafka1.producer import send_products
from replay.replay import replay_failed_records

# ----------------------------------------------------
# ETL Functions
# ----------------------------------------------------


def extract_task(**context):

    products = extract_products()

    context["ti"].xcom_push(key="products", value=products)


def validate_task(**context):

    ti = context["ti"]

    products = ti.xcom_pull(key="products", task_ids="extract")

    validated = validate_products(products)

    ti.xcom_push(key="validated_products", value=validated)


def kafka_task(**context):

    ti = context["ti"]

    validated = ti.xcom_pull(key="validated_products", task_ids="validate")

    send_products(validated)


def replay_task():

    replay_failed_records()


# ----------------------------------------------------
# DAG
# ----------------------------------------------------

with DAG(
    dag_id="FakeStore_ETL",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ETL", "Kafka", "Databricks"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_task)

    validate = PythonOperator(task_id="validate", python_callable=validate_task)

    kafka = PythonOperator(task_id="send_to_kafka", python_callable=kafka_task)

    replay = PythonOperator(task_id="replay_failed", python_callable=replay_task)

    extract >> validate >> kafka >> replay
