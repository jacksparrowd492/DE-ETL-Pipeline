# Storage, catalog, Kafka, Spark (EMR Serverless), Trino serving (Athena) and secrets
# for the FakeStore ETL lakehouse. Monitoring is in monitoring.tf, Airflow in mwaa.tf,
# the batch consumer host in consumer.tf. See deploy/README.md.

data "aws_caller_identity" "current" {}

locals {
  name       = "${var.project}-${var.environment}"
  account_id = data.aws_caller_identity.current.account_id

  buckets = {
    lakehouse = "${local.name}-lakehouse-${local.account_id}" # Delta tables + checkpoints
    artifacts = "${local.name}-artifacts-${local.account_id}" # code, DAGs, Athena results
  }

  lakehouse_root = "s3://${local.buckets.lakehouse}/lakehouse"
  glue_arn       = "arn:aws:glue:${var.region}:${local.account_id}"
  msk_prefix     = "arn:aws:kafka:${var.region}:${local.account_id}"
}

# --------------------------------------------------------
# S3: versioned, encrypted, private
# --------------------------------------------------------

resource "aws_s3_bucket" "this" {
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id

  versioning_configuration {
    status = "Enabled" # MWAA requires it on the DAG bucket; on the lakehouse it makes VACUUM recoverable
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.this["lakehouse"].id

  rule {
    id     = "expire-vacuumed-versions"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.lakehouse_noncurrent_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.this["artifacts"].id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

# --------------------------------------------------------
# Glue Data Catalog (replaces the local embedded Derby metastore)
# --------------------------------------------------------

resource "aws_glue_catalog_database" "lakehouse" {
  name         = "lakehouse" # = DATABASE in streaming/lakehouse.py
  description  = "Delta tables written by streaming/spark_delta.py (Spark's catalog)"
  location_uri = "${local.lakehouse_root}/warehouse/lakehouse.db"
}

resource "aws_glue_catalog_database" "serving" {
  name        = "lakehouse_serving"
  description = "Athena (Trino engine) tables + views over the same Delta folders: deploy/aws/athena_tables.sql"
}

# --------------------------------------------------------
# Network: one security group for everything that talks to Kafka
# --------------------------------------------------------

resource "aws_security_group" "data_plane" {
  name        = "${local.name}-data-plane"
  description = "MSK, EMR Serverless, MWAA and the consumer host talk to each other"
  vpc_id      = var.vpc_id

  ingress {
    description = "Members of this group (MSK IAM on 9098, MWAA web/workers)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "HTTPS to AWS APIs, Databricks, FakeStore, Slack, SMTP via NAT"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --------------------------------------------------------
# Kafka: MSK Serverless (IAM auth only)
# --------------------------------------------------------

resource "aws_msk_serverless_cluster" "kafka" {
  count        = var.create_msk ? 1 : 0
  cluster_name = local.name

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.data_plane.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

data "aws_msk_bootstrap_brokers" "kafka" {
  count       = var.create_msk ? 1 : 0
  cluster_arn = aws_msk_serverless_cluster.kafka[0].arn
}

# --------------------------------------------------------
# Spark: EMR Serverless application (streaming job + maintenance jobs)
# --------------------------------------------------------

resource "aws_emrserverless_application" "spark" {
  name          = local.name
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"

  maximum_capacity {
    cpu    = var.emr_max_cpu
    memory = var.emr_max_memory
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.data_plane.id]
  }
}

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${local.name}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    sid       = "ReadWriteBuckets"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = flatten([for b in aws_s3_bucket.this : [b.arn, "${b.arn}/*"]])
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:UpdatePartition",
      "glue:DeletePartition", "glue:BatchCreatePartition", "glue:BatchGetPartition",
      "glue:BatchDeletePartition", "glue:GetUserDefinedFunctions",
    ]
    resources = [
      "${local.glue_arn}:catalog",
      "${local.glue_arn}:database/default",
      "${local.glue_arn}:database/lakehouse",
      "${local.glue_arn}:table/lakehouse/*",
    ]
  }

  statement {
    sid = "MskIam"
    actions = [
      "kafka-cluster:Connect", "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic", "kafka-cluster:CreateTopic", "kafka-cluster:ReadData",
      "kafka-cluster:WriteData", "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup",
    ]
    resources = [
      "${local.msk_prefix}:cluster/${local.name}/*",
      "${local.msk_prefix}:topic/${local.name}/*",
      "${local.msk_prefix}:group/${local.name}/*",
    ]
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams", "logs:DescribeLogGroups"]
    resources = [aws_cloudwatch_log_group.pipeline.arn, "${aws_cloudwatch_log_group.pipeline.arn}:*"]
  }

  statement {
    sid       = "PipelineSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "emr_job" {
  name   = "pipeline"
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}

# --------------------------------------------------------
# Serving: Athena (Trino engine) workgroup
# --------------------------------------------------------

resource "aws_athena_workgroup" "serving" {
  name          = local.name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${local.buckets.artifacts}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# --------------------------------------------------------
# Secrets: one JSON secret with SLACK_WEBHOOK_URL, SMTP_*, DATABRICKS_TOKEN, ...
# Terraform creates the container only; the value is set out of band
# (deploy/README.md), so it never lands in Terraform state.
# --------------------------------------------------------

resource "aws_secretsmanager_secret" "pipeline" {
  name        = "${local.name}/pipeline"
  description = "Secret settings for the pipeline (read by observability/cloud_secrets.py and the MWAA startup script)"
}
