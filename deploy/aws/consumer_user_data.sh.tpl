#!/bin/bash
# EC2 user data for the Kafka -> Databricks consumer host, rendered by Terraform
# (deploy/terraform/aws/consumer.tf). Installs Python 3.11 + the code bundle from S3 and
# runs `python -m kafka1.consumer` under systemd: restarted 30 s after it stops (it stops
# on an error, after sending a critical alert). No Docker.
set -euxo pipefail

dnf install -y python3.11 python3.11-pip unzip amazon-cloudwatch-agent

useradd --system --home-dir /opt/fakestore-etl --shell /sbin/nologin etl || true
mkdir -p /opt/fakestore-etl /var/log/fakestore-etl

aws s3 cp "s3://${artifacts_bucket}/consumer/app.zip" /tmp/app.zip
rm -rf /opt/fakestore-etl/app && unzip -q /tmp/app.zip -d /opt/fakestore-etl/app
python3.11 -m venv /opt/fakestore-etl/venv
/opt/fakestore-etl/venv/bin/pip install --quiet -r /opt/fakestore-etl/app/requirements.txt
chown -R etl:etl /opt/fakestore-etl /var/log/fakestore-etl

cat > /etc/fakestore-etl.env <<ENV
ALERT_ENV=${environment}
AWS_REGION=${region}
PIPELINE_SECRET_ID=${secret_id}
ALERT_SNS_TOPIC_ARN=${sns_topic_arn}
KAFKA_BOOTSTRAP_SERVER=${kafka_bootstrap}
KAFKA_TOPIC=validated_products
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=AWS_MSK_IAM
LOG_FORMAT=json
LOG_TO_FILE=false
METRICS_TO_LOG=true
ENV

cat > /etc/systemd/system/fakestore-consumer.service <<UNIT
[Unit]
Description=FakeStore ETL Kafka -> Databricks consumer
After=network-online.target
Wants=network-online.target

[Service]
User=etl
WorkingDirectory=/opt/fakestore-etl/app
EnvironmentFile=/etc/fakestore-etl.env
ExecStart=/opt/fakestore-etl/venv/bin/python -m kafka1.consumer
Restart=always
RestartSec=30
StandardOutput=append:/var/log/fakestore-etl/consumer.log
StandardError=append:/var/log/fakestore-etl/consumer.log

[Install]
WantedBy=multi-user.target
UNIT

# JSON log lines -> the pipeline log group, where the metric filters / alarms live
cat > /opt/aws/amazon-cloudwatch-agent/etc/fakestore.json <<CW
{"logs": {"logs_collected": {"files": {"collect_list": [{
  "file_path": "/var/log/fakestore-etl/consumer.log",
  "log_group_name": "${log_group}",
  "log_stream_name": "consumer/{instance_id}"
}]}}}}
CW
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/fakestore.json

systemctl daemon-reload
systemctl enable --now fakestore-consumer
