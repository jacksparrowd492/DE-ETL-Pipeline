"""
emr_settings.py
---------------
One definition of how the Spark jobs are submitted to EMR Serverless, used by
both deploy/aws/deploy.sh (starts the streaming job) and the MWAA DAG
deploy/aws/dags/lakehouse_maintenance_emr.py (runs the maintenance jobs), so
the two can never drift apart.

    python deploy/aws/emr_settings.py streaming   # print the StartJobRun request as JSON

Everything comes from the environment (set by the MWAA startup script, or by
deploy.sh from `terraform output`): ARTIFACTS_BUCKET, LAKEHOUSE_ROOT,
KAFKA_BOOTSTRAP_SERVER (MSK, SASL/IAM), PIPELINE_SECRET_ID, ALERT_SNS_TOPIC_ARN,
PIPELINE_LOG_GROUP, ALERT_ENV, AWS_REGION.
"""

from __future__ import annotations

import json
import os
import sys

DELTA_LIB = "/usr/share/aws/delta/lib"  # Delta Lake ships with EMR 7.x
KAFKA_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,software.amazon.msk:aws-msk-iam-auth:2.2.0"
)
MSK_IAM_OPTIONS = [
    "security.protocol=SASL_SSL",
    "sasl.mechanism=AWS_MSK_IAM",
    "sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;",
    "sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler",
]


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def driver_env() -> dict:
    """Environment of the Spark driver: cluster mode, cloud logging, alert routing."""
    return {
        "LAKEHOUSE_MODE": "cluster",
        "LAKEHOUSE_ROOT": _env("LAKEHOUSE_ROOT"),
        "LOG_FORMAT": "json",
        "LOG_TO_FILE": "false",
        "METRICS_TO_LOG": "true",
        "ALERT_ENV": _env("ALERT_ENV", "dev"),
        "ALERT_SNS_TOPIC_ARN": _env("ALERT_SNS_TOPIC_ARN"),
        "PIPELINE_SECRET_ID": _env("PIPELINE_SECRET_ID"),
        "AWS_REGION": _env("AWS_REGION", "us-east-1"),
        "LINEAGE_LAKEHOUSE_NAMESPACE": f"glue://{_env('AWS_REGION', 'us-east-1')}",
        "STREAM_KAFKA_BOOTSTRAP": _env("KAFKA_BOOTSTRAP_SERVER"),
    }


def spark_submit_parameters(with_kafka: bool) -> str:
    bucket = _env("ARTIFACTS_BUCKET")
    conf = {
        "spark.submit.pyFiles": f"s3://{bucket}/emr/pyfiles.zip,{DELTA_LIB}/delta-spark.jar",
        "spark.jars": f"{DELTA_LIB}/delta-spark.jar,{DELTA_LIB}/delta-storage.jar",
        "spark.hadoop.hive.metastore.client.factory.class": (
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        ),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }
    if with_kafka:
        conf["spark.jars.packages"] = KAFKA_PACKAGES  # from Maven Central, via the NAT gateway
    conf.update({f"spark.emr-serverless.driverEnv.{k}": v for k, v in driver_env().items() if v})
    return " ".join(f"--conf {k}={v}" for k, v in conf.items())


def monitoring(prefix: str) -> dict:
    return {
        "monitoringConfiguration": {
            "cloudWatchLoggingConfiguration": {
                "enabled": True,
                "logGroupName": _env("PIPELINE_LOG_GROUP"),
                "logStreamNamePrefix": prefix,
            }
        }
    }


def streaming_job_driver() -> dict:
    args = ["--skip-topic-check", "--trigger", "10 seconds"]
    if _env("KAFKA_BOOTSTRAP_SERVER"):
        args += ["--bootstrap", _env("KAFKA_BOOTSTRAP_SERVER")]
        for option in MSK_IAM_OPTIONS:
            args += ["--kafka-option", option]
    return {
        "sparkSubmit": {
            "entryPoint": f"s3://{_env('ARTIFACTS_BUCKET')}/emr/spark_delta.py",
            "entryPointArguments": args,
            "sparkSubmitParameters": spark_submit_parameters(with_kafka=True),
        }
    }


def maintenance_job_driver(action: str, table: str, extra_args: list | None = None) -> dict:
    return {
        "sparkSubmit": {
            "entryPoint": f"s3://{_env('ARTIFACTS_BUCKET')}/emr/emr_maintenance_job.py",
            "entryPointArguments": ["--action", action, "--table", table, *(extra_args or [])],
            "sparkSubmitParameters": spark_submit_parameters(with_kafka=False),
        }
    }


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else "streaming"
    if kind != "streaming":
        sys.exit("usage: emr_settings.py streaming")
    print(
        json.dumps(
            {"jobDriver": streaming_job_driver(), "configurationOverrides": monitoring("streaming")}
        )
    )
