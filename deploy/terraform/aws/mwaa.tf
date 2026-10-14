# Amazon MWAA (managed Airflow) for the DAGs, behind var.create_mwaa.
#   mwaa/dags/          fakestore_pipeline.py + the code it imports, and
#                       lakehouse_maintenance_emr.py (compaction / expiry / vacuum on EMR)
#   mwaa/requirements.txt, mwaa/startup.sh   uploaded here; DAGs by deploy/aws/deploy.sh

locals {
  mwaa_name = local.name
  mwaa_arn  = "arn:aws:airflow:${var.region}:${local.account_id}:environment/${local.mwaa_name}"
}

resource "aws_s3_object" "mwaa_requirements" {
  count  = var.create_mwaa ? 1 : 0
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "mwaa/requirements.txt"
  source = "${path.module}/../../aws/mwaa/requirements.txt"
  etag   = filemd5("${path.module}/../../aws/mwaa/requirements.txt")
}

resource "aws_s3_object" "mwaa_startup" {
  count  = var.create_mwaa ? 1 : 0
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "mwaa/startup.sh"
  content = templatefile("${path.module}/../../aws/mwaa/startup.sh.tpl", {
    environment      = var.environment
    region           = var.region
    secret_id        = aws_secretsmanager_secret.pipeline.name
    sns_topic_arn    = aws_sns_topic.alerts.arn
    kafka_bootstrap  = var.create_msk ? data.aws_msk_bootstrap_brokers.kafka[0].bootstrap_brokers_sasl_iam : ""
    emr_app_id       = aws_emrserverless_application.spark.id
    emr_role_arn     = aws_iam_role.emr_job.arn
    artifacts_bucket = local.buckets.artifacts
    lakehouse_root   = local.lakehouse_root
    log_group        = aws_cloudwatch_log_group.pipeline.name
  })
}

data "aws_iam_policy_document" "mwaa_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa" {
  count              = var.create_mwaa ? 1 : 0
  name               = "${local.name}-mwaa"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume.json
}

data "aws_iam_policy_document" "mwaa" {
  statement {
    sid       = "PublishMetrics"
    actions   = ["airflow:PublishMetrics"]
    resources = [local.mwaa_arn]
  }

  statement {
    sid       = "ReadDags"
    actions   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
    resources = [aws_s3_bucket.this["artifacts"].arn, "${aws_s3_bucket.this["artifacts"].arn}/*"]
  }

  statement {
    sid       = "AccountPublicAccessBlock"
    actions   = ["s3:GetAccountPublicAccessBlock"]
    resources = ["*"]
  }

  statement {
    sid = "AirflowLogs"
    actions = [
      "logs:CreateLogStream", "logs:CreateLogGroup", "logs:PutLogEvents", "logs:GetLogEvents",
      "logs:GetLogRecord", "logs:GetLogGroupFields", "logs:GetQueryResults",
    ]
    resources = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:airflow-${local.mwaa_name}-*"]
  }

  statement {
    sid       = "DescribeLogsAndMetrics"
    actions   = ["logs:DescribeLogGroups", "cloudwatch:PutMetricData"]
    resources = ["*"]
  }

  statement {
    sid = "CeleryQueue"
    actions = [
      "sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage",
    ]
    resources = ["arn:aws:sqs:${var.region}:*:airflow-celery-*"]
  }

  statement {
    sid       = "KmsViaSqsAndS3"
    actions   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"]
    resources = ["*"]
    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values   = ["sqs.${var.region}.amazonaws.com", "s3.${var.region}.amazonaws.com"]
    }
  }

  statement {
    sid       = "RunEmrJobs"
    actions   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:GetApplication", "emr-serverless:StartApplication"]
    resources = [aws_emrserverless_application.spark.arn, "${aws_emrserverless_application.spark.arn}/jobruns/*"]
  }

  statement {
    sid       = "PassEmrJobRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_job.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  statement {
    sid       = "ProduceToMsk"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:DescribeTopic", "kafka-cluster:CreateTopic", "kafka-cluster:WriteData"]
    resources = ["${local.msk_prefix}:cluster/${local.name}/*", "${local.msk_prefix}:topic/${local.name}/*"]
  }

  statement {
    sid       = "AlertsAndSecrets"
    actions   = ["sns:Publish", "secretsmanager:GetSecretValue"]
    resources = [aws_sns_topic.alerts.arn, aws_secretsmanager_secret.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "mwaa" {
  count  = var.create_mwaa ? 1 : 0
  name   = "mwaa"
  role   = aws_iam_role.mwaa[0].id
  policy = data.aws_iam_policy_document.mwaa.json
}

resource "aws_mwaa_environment" "airflow" {
  count = var.create_mwaa ? 1 : 0

  name                   = local.mwaa_name
  airflow_version        = var.mwaa_airflow_version
  environment_class      = "mw1.small"
  max_workers            = 2
  execution_role_arn     = aws_iam_role.mwaa[0].arn
  source_bucket_arn      = aws_s3_bucket.this["artifacts"].arn
  dag_s3_path            = "mwaa/dags"
  requirements_s3_path   = aws_s3_object.mwaa_requirements[0].key
  startup_script_s3_path = aws_s3_object.mwaa_startup[0].key
  webserver_access_mode  = "PRIVATE_ONLY"

  network_configuration {
    security_group_ids = [aws_security_group.data_plane.id]
    subnet_ids         = slice(var.private_subnet_ids, 0, 2)
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "WARNING"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "WARNING"
    }
  }

  depends_on = [aws_iam_role_policy.mwaa]
}
