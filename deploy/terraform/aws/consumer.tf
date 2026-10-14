# The batch path's long-running Kafka -> Databricks consumer (python -m kafka1.consumer)
# on a small EC2 instance as a systemd service. No Docker: the instance installs
# Python 3.11 and the code bundle that deploy/package.py builds. Access is through
# SSM Session Manager - no SSH key, no inbound port.

data "aws_ssm_parameter" "al2023" {
  count = var.create_consumer_host ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  count              = var.create_consumer_host ? 1 : 0
  name               = "${local.name}-consumer"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "consumer_ssm" {
  count      = var.create_consumer_host ? 1 : 0
  role       = aws_iam_role.consumer[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "consumer_cloudwatch_agent" {
  count      = var.create_consumer_host ? 1 : 0
  role       = aws_iam_role.consumer[0].name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

data "aws_iam_policy_document" "consumer" {
  statement {
    sid       = "ReadCodeBundle"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.this["artifacts"].arn, "${aws_s3_bucket.this["artifacts"].arn}/consumer/*"]
  }

  statement {
    sid       = "ConsumeFromMsk"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData", "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
    resources = ["${local.msk_prefix}:cluster/${local.name}/*", "${local.msk_prefix}:topic/${local.name}/*", "${local.msk_prefix}:group/${local.name}/*"]
  }

  statement {
    sid       = "AlertsAndSecrets"
    actions   = ["sns:Publish", "secretsmanager:GetSecretValue"]
    resources = [aws_sns_topic.alerts.arn, aws_secretsmanager_secret.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "consumer" {
  count  = var.create_consumer_host ? 1 : 0
  name   = "consumer"
  role   = aws_iam_role.consumer[0].id
  policy = data.aws_iam_policy_document.consumer.json
}

resource "aws_iam_instance_profile" "consumer" {
  count = var.create_consumer_host ? 1 : 0
  name  = "${local.name}-consumer"
  role  = aws_iam_role.consumer[0].name
}

resource "aws_instance" "consumer" {
  count                  = var.create_consumer_host ? 1 : 0
  ami                    = data.aws_ssm_parameter.al2023[0].value
  instance_type          = var.consumer_instance_type
  subnet_id              = var.private_subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.data_plane.id]
  iam_instance_profile   = aws_iam_instance_profile.consumer[0].name

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  root_block_device {
    encrypted   = true
    volume_size = 16
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/../../aws/consumer_user_data.sh.tpl", {
    environment      = var.environment
    region           = var.region
    artifacts_bucket = local.buckets.artifacts
    secret_id        = aws_secretsmanager_secret.pipeline.name
    sns_topic_arn    = aws_sns_topic.alerts.arn
    kafka_bootstrap  = var.create_msk ? data.aws_msk_bootstrap_brokers.kafka[0].bootstrap_brokers_sasl_iam : ""
    log_group        = aws_cloudwatch_log_group.pipeline.name
  })

  tags = {
    Name = "${local.name}-consumer"
  }
}
