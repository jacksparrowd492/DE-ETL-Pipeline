#!/bin/sh
# MWAA startup script, rendered by Terraform (deploy/terraform/aws/mwaa.tf) and run on
# every scheduler/worker/webserver container start. Non-secret settings only: secret
# ones (SLACK_WEBHOOK_URL, SMTP_*, DATABRICKS_*) are read from Secrets Manager by the
# tasks themselves (observability/cloud_secrets.py), so they never sit in S3.

export ALERT_ENV="${environment}"
export AWS_REGION="${region}"
export PIPELINE_SECRET_ID="${secret_id}"
export ALERT_SNS_TOPIC_ARN="${sns_topic_arn}"

# batch path: produce to MSK with IAM auth
export KAFKA_BOOTSTRAP_SERVER="${kafka_bootstrap}"
export KAFKA_TOPIC="validated_products"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_SASL_MECHANISM="AWS_MSK_IAM"

# task logs go to MWAA's CloudWatch task log group; no local files on the workers
export LOG_TO_FILE="false"

# maintenance DAG -> EMR Serverless (deploy/aws/emr_settings.py)
export EMR_APPLICATION_ID="${emr_app_id}"
export EMR_JOB_ROLE_ARN="${emr_role_arn}"
export ARTIFACTS_BUCKET="${artifacts_bucket}"
export LAKEHOUSE_ROOT="${lakehouse_root}"
export PIPELINE_LOG_GROUP="${log_group}"
