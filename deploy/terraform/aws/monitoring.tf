# Cloud monitoring. In the cloud the jobs run with LOG_FORMAT=json, LOG_TO_FILE=false and
# METRICS_TO_LOG=true, so every log line, alert and metric sample is a JSON line in this
# log group. Metric filters turn them into CloudWatch metrics; alarms page SNS (email).
# Slack/email alerts from the code itself (observability/alerts.py) still fire too: the
# alarms are the second net, e.g. for a job that died before it could send anything.

locals {
  metric_namespace = "FakestoreETL/${var.environment}"

  # name => [filter pattern, metric value]
  metric_filters = {
    CriticalAlerts         = ["{ $.alert.severity = \"critical\" }", "1"]
    WarningAlerts          = ["{ $.alert.severity = \"warning\" }", "1"]
    TaskFailures           = ["{ $.metric.name = \"task_success\" && $.metric.value = 0 }", "1"]
    StreamIngestLatencyP95 = ["{ $.metric.name = \"stream_ingest_latency_p95_seconds\" }", "$.metric.value"]
    StreamRejectRatio      = ["{ $.metric.name = \"stream_reject_ratio\" }", "$.metric.value"]
    StreamTriggers         = ["{ $.metric.name = \"stream_input_rows\" }", "1"]
    KafkaE2ELatency        = ["{ $.metric.name = \"kafka_end_to_end_latency_seconds\" }", "$.metric.value"]
    ConsumerFailures       = ["{ $.metric.name = \"consumer_messages\" && $.metric.labels.status = \"failed\" }", "1"]
  }
}

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/${var.project}/${var.environment}"
  retention_in_days = var.log_retention_days
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_metric_filter" "pipeline" {
  for_each       = local.metric_filters
  name           = "${local.name}-${each.key}"
  log_group_name = aws_cloudwatch_log_group.pipeline.name
  pattern        = each.value[0]

  metric_transformation {
    name      = each.key
    namespace = local.metric_namespace
    value     = each.value[1]
  }
}

# --------------------------------------------------------
# Alarms -> SNS
# --------------------------------------------------------

locals {
  # name => [metric, statistic, threshold, periods, datapoints, missing data, description]
  alarms = {
    critical-alerts = ["CriticalAlerts", "Sum", 1, 1, 1, "notBreaching", "A job sent a critical alert (task failed, consumer or stream stopped)."]
    task-failures   = ["TaskFailures", "Sum", 1, 1, 1, "notBreaching", "A pipeline task failed."]
    consumer-errors = ["ConsumerFailures", "Sum", 1, 1, 1, "notBreaching", "The Kafka -> Databricks consumer failed to load a product."]
    stream-latency  = ["StreamIngestLatencyP95", "Maximum", var.stream_latency_alarm_seconds, 3, 2, "notBreaching", "Events reach the lakehouse late (p95 ingested_at - event_time)."]
    kafka-latency   = ["KafkaE2ELatency", "Maximum", var.e2e_latency_alarm_seconds, 3, 2, "notBreaching", "Products reach Databricks late (producer -> consumer)."]
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline" {
  for_each = local.alarms

  alarm_name          = "${local.name}-${each.key}"
  alarm_description   = each.value[6]
  namespace           = local.metric_namespace
  metric_name         = each.value[0]
  statistic           = each.value[1]
  threshold           = each.value[2]
  evaluation_periods  = each.value[3]
  datapoints_to_alarm = each.value[4]
  treat_missing_data  = each.value[5]
  period              = 300
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  depends_on = [aws_cloudwatch_log_metric_filter.pipeline]
}

# The stream reports progress every trigger (10 s). No progress for 15 minutes means it
# died or hangs - even if it could not send an alert itself.
resource "aws_cloudwatch_metric_alarm" "stream_stalled" {
  alarm_name          = "${local.name}-stream-stalled"
  alarm_description   = "No Spark trigger progress for 15 minutes: the streaming job is down or stuck."
  namespace           = local.metric_namespace
  metric_name         = "StreamTriggers"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  depends_on = [aws_cloudwatch_log_metric_filter.pipeline]
}

# --------------------------------------------------------
# Dashboard
# --------------------------------------------------------

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = local.name
  dashboard_body = jsonencode({
    widgets = [
      for i, m in [
        ["StreamIngestLatencyP95", "Maximum", "Stream ingestion latency p95 (s)"],
        ["KafkaE2ELatency", "Maximum", "Kafka -> Databricks latency (s)"],
        ["StreamRejectRatio", "Average", "Stream reject ratio"],
        ["StreamTriggers", "Sum", "Stream triggers / 5 min"],
        ["TaskFailures", "Sum", "Task failures"],
        ["CriticalAlerts", "Sum", "Critical alerts"],
        ] : {
        type   = "metric"
        x      = (i % 3) * 8
        y      = floor(i / 3) * 6
        width  = 8
        height = 6
        properties = {
          title   = m[2]
          region  = var.region
          stat    = m[1]
          period  = 300
          metrics = [[local.metric_namespace, m[0]]]
        }
      }
    ]
  })
}
