# deploy/aws/deploy.sh reads these with `terraform output -json`.

output "region" {
  value = var.region
}

output "environment" {
  value = var.environment
}

output "artifacts_bucket" {
  value = local.buckets.artifacts
}

output "lakehouse_bucket" {
  value = local.buckets.lakehouse
}

output "lakehouse_root" {
  description = "LAKEHOUSE_ROOT for the Spark jobs"
  value       = local.lakehouse_root
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.emr_job.arn
}

output "kafka_bootstrap_brokers" {
  description = "SASL/IAM bootstrap string (port 9098), empty without MSK"
  value       = var.create_msk ? data.aws_msk_bootstrap_brokers.kafka[0].bootstrap_brokers_sasl_iam : ""
}

output "log_group" {
  value = aws_cloudwatch_log_group.pipeline.name
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "pipeline_secret_id" {
  description = "Put the secret settings here: aws secretsmanager put-secret-value --secret-id <this> --secret-string file://secret.json"
  value       = aws_secretsmanager_secret.pipeline.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.serving.name
}

output "mwaa_webserver_url" {
  value = var.create_mwaa ? aws_mwaa_environment.airflow[0].webserver_url : ""
}

output "consumer_instance_id" {
  value = var.create_consumer_host ? aws_instance.consumer[0].id : ""
}
