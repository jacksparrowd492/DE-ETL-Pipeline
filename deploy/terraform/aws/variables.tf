variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "fakestore-etl"
}

variable "environment" {
  description = "Deployment environment. Use one AWS account per environment: the Glue database names (lakehouse, lakehouse_serving) are fixed by the code."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "region" {
  description = "AWS region. The Databricks workspace already runs on AWS; keep it in the same region."
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC for MSK, EMR Serverless, MWAA and the consumer host."
  type        = string
}

variable "private_subnet_ids" {
  description = "At least two private subnets in different AZs, with a NAT gateway for outbound HTTPS (FakeStore API, Databricks, Slack, Maven for the Kafka connector)."
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "MSK Serverless and MWAA need at least two subnets in different AZs."
  }
}

variable "alert_email" {
  description = "Email that receives the CloudWatch alarms (SNS asks it to confirm once). Empty = no subscription."
  type        = string
  default     = ""
}

variable "create_msk" {
  description = "Create an MSK Serverless cluster (IAM auth). false = bring your own Kafka / Confluent Cloud."
  type        = bool
  default     = true
}

variable "create_mwaa" {
  description = "Create an Amazon MWAA (managed Airflow) environment for the DAGs. It costs from ~$0.49/h, so it is off by default."
  type        = bool
  default     = false
}

variable "mwaa_airflow_version" {
  description = "MWAA Airflow version. The maintenance DAG uses the Airflow 3 SDK (airflow.sdk)."
  type        = string
  default     = "3.0.6"
}

variable "create_consumer_host" {
  description = "Create a small EC2 instance that runs the Kafka -> Databricks consumer (python -m kafka1.consumer) as a systemd service."
  type        = bool
  default     = false
}

variable "consumer_instance_type" {
  type    = string
  default = "t3.small"
}

variable "emr_release_label" {
  description = "EMR Serverless release. emr-7.5.0 = Spark 3.5.2 + Delta 3.2.1, matching the local pyspark 3.5 / delta-spark 3.2."
  type        = string
  default     = "emr-7.5.0"
}

variable "emr_max_cpu" {
  type    = string
  default = "16 vCPU"
}

variable "emr_max_memory" {
  type    = string
  default = "64 GB"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "lakehouse_noncurrent_days" {
  description = "Days S3 keeps a noncurrent object version. Delta VACUUM deletes files; with bucket versioning they stay recoverable this long."
  type        = number
  default     = 30
}

variable "stream_latency_alarm_seconds" {
  description = "Alarm when the stream's p95 ingestion latency (ingested_at - event_time) stays above this."
  type        = number
  default     = 120
}

variable "e2e_latency_alarm_seconds" {
  description = "Alarm when the batch path's Kafka -> Databricks latency stays above this."
  type        = number
  default     = 300
}
