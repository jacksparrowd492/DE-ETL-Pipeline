# Deploying to the cloud (AWS)

The pipeline runs locally on Windows + WSL. This folder deploys the same code to AWS,
where the Databricks workspace already runs (`*.cloud.databricks.com`), **without Docker**:
every runtime takes plain Python code (a zip, a DAG folder, or a venv on a VM).

## Target architecture

| Layer | Local (today) | AWS (this deployment) |
|---|---|---|
| Ingestion | Kafka 3.9 in WSL (KRaft) | **Amazon MSK Serverless** (IAM auth) |
| Processing | `streaming/spark_delta.py`, PySpark `local[*]` | same file on **EMR Serverless** (Spark 3.5, `--mode STREAMING`) |
| Storage | Delta tables in `streaming/_lakehouse/` | Delta tables in **S3** (versioned, KMS-encrypted) |
| Catalog | Hive metastore, embedded Derby | **AWS Glue Data Catalog** |
| Serving | Trino 483 + Metabase / Power BI | **Amazon Athena engine v3** (Trino) + the same BI tools via the Athena/Trino driver |
| Orchestration | Airflow 3.3 in WSL | **Amazon MWAA** (Airflow 3) |
| Maintenance | delta-rs on the local disk | `emr_maintenance_job.py`: Delta `OPTIMIZE` / log retention / `VACUUM` on EMR |
| Batch consumer | `python -m kafka1.consumer` | same command under **systemd on EC2** (SSM access, no SSH) |
| Batch warehouse | Databricks SQL warehouse | unchanged (already in the cloud) |
| Logs / metrics | `logs/*.jsonl`, `.prom` | **CloudWatch Logs** (JSON lines) → metric filters → **alarms** → **SNS** |
| Alerts | Slack / email | Slack / email / **SNS**, secrets in **Secrets Manager** |
| Lineage | `logs/lineage/openlineage.jsonl` | same events to any OpenLineage backend (`OPENLINEAGE_URL`) |

GCP and Azure have a direct equivalent for every row, and the code only changes in its
configuration:

| AWS | GCP | Azure |
|---|---|---|
| MSK Serverless | Managed Service for Apache Kafka | Event Hubs (Kafka endpoint) |
| EMR Serverless | Dataproc Serverless | Synapse Spark / HDInsight on AKS |
| S3 + Glue | GCS + Dataproc Metastore / BigLake | ADLS Gen2 + Hive Metastore / Unity Catalog |
| Athena (Trino) | Starburst Galaxy / Trino on Dataproc | Starburst on Azure / Trino on AKS |
| MWAA | Cloud Composer | Azure Data Factory Managed Airflow |
| CloudWatch + SNS | Cloud Logging / Monitoring + Pub/Sub | Azure Monitor + Action Groups |
| Secrets Manager | Secret Manager | Key Vault |

`LAKEHOUSE_ROOT` takes any Hadoop URI (`gs://`, `abfss://`), `--kafka-option` takes any
Kafka client setting (SASL/OAUTHBEARER for Event Hubs), and the alerting, metrics and
lineage code is cloud-neutral.

## What is in here

```
deploy/
  package.py                    builds dist/ (EMR zip, MWAA DAG folder, consumer bundle, manifest)
  terraform/aws/                the infrastructure (passes terraform fmt + validate)
    main.tf                     S3, Glue, security group, MSK, EMR Serverless, IAM, Athena, secret
    monitoring.tf               log group, metric filters, alarms -> SNS, CloudWatch dashboard
    mwaa.tf                     managed Airflow (create_mwaa = true)
    consumer.tf                 EC2 consumer host (create_consumer_host = true)
  aws/
    deploy.sh                   upload | start-stream | athena | restart-consumer | all
    emr_settings.py             how jobs are submitted to EMR (shared by deploy.sh and the DAG)
    emr_maintenance_job.py      cloud Delta maintenance (compact / expire / vacuum)
    dags/lakehouse_maintenance_emr.py   MWAA maintenance DAGs -> EMR jobs
    athena_tables.sql           serving tables + views (= serving/register_tables.sql + views.sql)
    mwaa/requirements.txt, mwaa/startup.sh.tpl, consumer_user_data.sh.tpl
.github/workflows/deploy.yml    manual, OIDC, plan -> apply -> upload -> restart
```

## First deployment

Prerequisites: an AWS account per environment, a VPC with two private subnets (different AZs)
behind a NAT gateway, AWS CLI v2, Terraform ≥ 1.10, Python 3.10+.

```bash
cd deploy/terraform/aws
cp terraform.tfvars.example dev.tfvars            # VPC, subnets, alert email (gitignored)
terraform init -backend-config=backend-dev.hcl    # S3 state bucket (see the example file)
terraform plan -var-file=dev.tfvars -out=dev.plan
terraform apply dev.plan

# secret settings: never in Terraform state or in git
aws secretsmanager put-secret-value --secret-id "$(terraform output -raw pipeline_secret_id)" \
  --secret-string '{"SLACK_WEBHOOK_URL":"https://hooks.slack.com/services/...",
                    "DATABRICKS_SERVER_HOSTNAME":"dbc-....cloud.databricks.com",
                    "DATABRICKS_HTTP_PATH":"/sql/1.0/warehouses/...",
                    "DATABRICKS_TOKEN":"dapi..."}'

cd ../../..
bash deploy/aws/deploy.sh all       # code to S3, start the stream, Athena tables, consumer
```

Confirm the SNS subscription email, then check: the EMR job run is `RUNNING`, the
CloudWatch dashboard `fakestore-etl-dev` shows stream triggers, and
`SELECT count(*) FROM lakehouse_serving.user_events` works in the Athena workgroup.

## Deployment strategy

* **Infrastructure as code.** Everything is Terraform; no console clicks except the secret's
  value. One state file and one AWS account per environment (`dev`, `staging`, `prod`).
* **Promotion through environments.** CI (`ci.yml`) gates every merge: lint, config and
  lineage checks, tests, ≥ 70 % coverage, an integration run and `terraform validate`. The
  same commit is then deployed to `dev`, then `staging`, then `prod` with the manual
  **Deploy (AWS)** workflow. The `prod` GitHub Environment requires a reviewer, and the job
  authenticates with OIDC, so there are no long-lived AWS keys anywhere.
* **Plan before apply.** The workflow always plans; it applies only when *apply* is ticked,
  from the plan file it just showed.
* **Immutable, traceable artifacts.** `package.py` writes `manifest.json` (git commit,
  file hashes) and `deploy.sh` stores it as `s3://<artifacts>/manifest/<commit>.json`.
  What runs in an environment is always a known commit.
* **Streaming job: stop-then-start on the same checkpoint.** `deploy.sh start-stream`
  cancels the running job and starts the new code against the same S3 checkpoint. The
  write-ahead log replays exactly the unfinished batch, and the MERGE on `event_id` plus
  Delta's `txnAppId`/`txnVersion` make the replay idempotent: no data lost or duplicated.
  EMR's streaming retry policy restarts a crashed job (5 attempts per hour).
* **Batch DAGs: MWAA picks up new DAG files from S3**; a run in progress finishes on the old
  code. The consumer host swaps its app folder atomically and keeps `failed_records/`.
* **Rollback** = deploy the previous commit (`git checkout <sha>`, `deploy.sh upload`,
  `start-stream`). Terraform rollbacks are a revert + plan + apply. S3 versioning keeps
  vacuumed Delta files for `lakehouse_noncurrent_days` (30) as a last resort.
* **Schema changes** follow the lineage rule: a new column needs `COLUMN_LINEAGE` updated, or
  CI fails (`python -m governance.lineage validate`).

## Monitoring in the cloud

The jobs run with `LOG_FORMAT=json`, `LOG_TO_FILE=false`, `METRICS_TO_LOG=true`, so every log
line, alert and metric sample is one JSON line in the `/fakestore-etl/<env>` log group:

* **Metric filters** build `CriticalAlerts`, `TaskFailures`, `ConsumerFailures`,
  `StreamIngestLatencyP95`, `StreamRejectRatio`, `StreamTriggers`, `KafkaE2ELatency`.
* **Alarms → SNS → email** for critical alerts, task/consumer failures, stream latency over
  120 s, Kafka → Databricks latency over 300 s, and **stream stalled** (no trigger for
  15 minutes). The stalled alarm catches a job that died without sending anything.
* The code's own alerts still go to Slack/email and now also SNS (`ALERT_SNS_TOPIC_ARN`).
* MWAA task failures alert through the DAGs' `on_failure_callback`; MWAA's own task logs are
  in its `airflow-<env>-Task` log group.

## Costs and what is off by default

MSK Serverless, EMR Serverless (billed per vCPU-hour while the stream runs; auto-stop after
15 idle minutes) and S3/Athena/CloudWatch are pay-per-use. MWAA (~$0.49/h, `create_mwaa`) and
the consumer EC2 instance (`create_consumer_host`) are off by default. `terraform destroy`
removes everything except the S3 state bucket.

## Status

The Terraform passes `terraform fmt -check` and `terraform validate` (also run by CI's
**Deploy Config** job), `package.py` builds all artifacts, and the scripts parse. It has
**not been applied to a real AWS account** from this repository yet: that needs an account,
a VPC and credentials, so the first `apply` should go to a `dev` account.
