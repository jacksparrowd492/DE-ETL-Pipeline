#!/usr/bin/env bash
# deploy.sh - ship the pipeline to the AWS environment Terraform created.
# Needs: AWS CLI v2 with credentials for the target account, Python 3.10+,
# `terraform apply` already done in deploy/terraform/aws. No Docker.
#
#   deploy/aws/deploy.sh upload            build dist/ and upload code, DAGs, consumer bundle
#   deploy/aws/deploy.sh start-stream      (re)start the streaming job on EMR Serverless
#   deploy/aws/deploy.sh athena            create the Athena (Trino) serving tables + views
#   deploy/aws/deploy.sh restart-consumer  reinstall + restart the consumer on its EC2 host
#   deploy/aws/deploy.sh all               upload, start-stream, athena, restart-consumer
#
# Rollout: code is versioned by commit (s3://<artifacts>/manifest/<commit>.json). The
# stream is restarted stop-then-start on the SAME checkpoint, so it resumes exactly
# where it stopped (Kafka offsets in the write-ahead log, idempotent Delta writes).
# Rollback = check out the previous commit and run `upload` + `start-stream` again.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="$ROOT/deploy/terraform/aws"
STREAM_JOB_NAME="kafka-to-delta"
PY="${PYTHON:-python}"

out() { "$PY" -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]]["value"])' "$1" < "$TF_OUT"; }

load_outputs() {
  TF_OUT="$(mktemp)"
  terraform -chdir="$TF_DIR" output -json > "$TF_OUT"
  export AWS_REGION="$(out region)"
  export ALERT_ENV="$(out environment)"
  export ARTIFACTS_BUCKET="$(out artifacts_bucket)"
  export LAKEHOUSE_ROOT="$(out lakehouse_root)"
  export KAFKA_BOOTSTRAP_SERVER="$(out kafka_bootstrap_brokers)"
  export PIPELINE_SECRET_ID="$(out pipeline_secret_id)"
  export ALERT_SNS_TOPIC_ARN="$(out alerts_topic_arn)"
  export PIPELINE_LOG_GROUP="$(out log_group)"
  EMR_APP_ID="$(out emr_application_id)"
  EMR_ROLE_ARN="$(out emr_job_role_arn)"
  ATHENA_WORKGROUP="$(out athena_workgroup)"
  CONSUMER_INSTANCE="$(out consumer_instance_id)"
}

upload() {
  "$PY" "$ROOT/deploy/package.py"
  local commit
  commit="$("$PY" -c 'import json; print(json.load(open("dist/manifest.json"))["commit"])')"
  aws s3 sync "$ROOT/dist/emr" "s3://$ARTIFACTS_BUCKET/emr/" --delete
  aws s3 sync "$ROOT/dist/mwaa/dags" "s3://$ARTIFACTS_BUCKET/mwaa/dags/" --delete
  aws s3 cp "$ROOT/dist/consumer/app.zip" "s3://$ARTIFACTS_BUCKET/consumer/app.zip"
  aws s3 cp "$ROOT/dist/manifest.json" "s3://$ARTIFACTS_BUCKET/manifest/$commit.json"
  aws s3 cp "$ROOT/dist/manifest.json" "s3://$ARTIFACTS_BUCKET/manifest/latest.json"
  echo "Uploaded commit $commit to s3://$ARTIFACTS_BUCKET"
}

start_stream() {
  local running
  running="$(aws emr-serverless list-job-runs --application-id "$EMR_APP_ID" \
    --states RUNNING PENDING SCHEDULED SUBMITTED \
    --query "jobRuns[?name=='$STREAM_JOB_NAME'].id" --output text)"
  for id in $running; do
    echo "Stopping stream job $id (it resumes from its checkpoint)"
    aws emr-serverless cancel-job-run --application-id "$EMR_APP_ID" --job-run-id "$id" > /dev/null
    until [ "$(aws emr-serverless get-job-run --application-id "$EMR_APP_ID" --job-run-id "$id" \
      --query jobRun.state --output text)" = "CANCELLED" ]; do sleep 10; done
  done

  local request
  request="$("$PY" "$ROOT/deploy/aws/emr_settings.py" streaming)"
  aws emr-serverless start-job-run \
    --application-id "$EMR_APP_ID" \
    --execution-role-arn "$EMR_ROLE_ARN" \
    --name "$STREAM_JOB_NAME" \
    --mode STREAMING \
    --retry-policy maxFailedAttemptsPerHour=5 \
    --job-driver "$("$PY" -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1])["jobDriver"]))' "$request")" \
    --configuration-overrides "$("$PY" -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1])["configurationOverrides"]))' "$request")"
}

athena() {
  "$PY" - "$ROOT/deploy/aws/athena_tables.sql" "$LAKEHOUSE_ROOT" <<'PY' |
import re, sys
sql = open(sys.argv[1], encoding="utf-8").read().replace("<lakehouse_root>", sys.argv[2])
sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
for stmt in (s.strip() for s in sql.split(";")):
    if stmt:
        print(" ".join(stmt.split()))
PY
  while IFS= read -r stmt; do
    local qid state
    qid="$(aws athena start-query-execution --work-group "$ATHENA_WORKGROUP" \
      --query-string "$stmt" --query QueryExecutionId --output text)"
    while :; do
      state="$(aws athena get-query-execution --query-execution-id "$qid" \
        --query QueryExecution.Status.State --output text)"
      case "$state" in SUCCEEDED) break ;; FAILED|CANCELLED) echo "Athena $state: $stmt" >&2; exit 1 ;; esac
      sleep 2
    done
    echo "ok: ${stmt:0:70}..."
  done
}

restart_consumer() {
  if [ -z "$CONSUMER_INSTANCE" ]; then echo "No consumer host (create_consumer_host = false)"; return; fi
  aws ssm send-command --instance-ids "$CONSUMER_INSTANCE" --document-name AWS-RunShellScript \
    --comment "fakestore-etl consumer redeploy" \
    --parameters 'commands=[
      "set -e",
      "aws s3 cp s3://'"$ARTIFACTS_BUCKET"'/consumer/app.zip /tmp/app.zip",
      "rm -rf /opt/fakestore-etl/app.new && unzip -q /tmp/app.zip -d /opt/fakestore-etl/app.new",
      "cp -a /opt/fakestore-etl/app/failed_records/. /opt/fakestore-etl/app.new/failed_records/ || true",
      "/opt/fakestore-etl/venv/bin/pip install --quiet -r /opt/fakestore-etl/app.new/requirements.txt",
      "systemctl stop fakestore-consumer",
      "rm -rf /opt/fakestore-etl/app.old && mv /opt/fakestore-etl/app /opt/fakestore-etl/app.old",
      "mv /opt/fakestore-etl/app.new /opt/fakestore-etl/app && chown -R etl:etl /opt/fakestore-etl",
      "systemctl start fakestore-consumer"
    ]' --query Command.CommandId --output text
}

cmd="${1:-upload}"
load_outputs
case "$cmd" in
  upload) upload ;;
  start-stream) start_stream ;;
  athena) athena ;;
  restart-consumer) restart_consumer ;;
  all) upload; start_stream; athena; restart_consumer ;;
  *) echo "usage: $0 [upload|start-stream|athena|restart-consumer|all]" >&2; exit 2 ;;
esac
