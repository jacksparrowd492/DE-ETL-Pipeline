#!/usr/bin/env bash
# Week 4 - start Airflow (api-server, scheduler, dag-processor, triggerer) in WSL.
#   bash maintenance/start_airflow.sh          # start in background
#   bash maintenance/start_airflow.sh stop
# Run maintenance/setup_airflow.sh once before. UI: http://localhost:8081
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "${AIRFLOW_HOME:-$HOME/airflow}/env.sh"   # API on 8081 (8080 is Trino)
LOG="$HERE/logs/airflow.log"
mkdir -p "$HERE/logs"

if [ "${1:-start}" = "stop" ]; then
  # the components rename themselves ("airflow api_server -- ...", "airflow worker -- ...")
  pkill -f "airflow-venv/bin/airflow" || true
  pkill -f "^airflow " || true
  for _ in $(seq 1 20); do pgrep -f "^airflow |airflow-venv/bin/airflow" >/dev/null || break; sleep 1; done
  pkill -9 -f "^airflow |airflow-venv/bin/airflow" || true
  echo ">> Airflow stopped"
  exit 0
fi

# one slot: two maintenance tasks never rewrite the lakehouse at the same time
airflow pools set lakehouse_maintenance 1 "Delta table maintenance (serialised)" >/dev/null

if pgrep -f "airflow standalone" >/dev/null; then
  echo ">> Airflow already running"
else
  nohup airflow standalone >"$LOG" 2>&1 &
  echo ">> Starting Airflow (log: $LOG)"
fi
for _ in $(seq 1 90); do
  if curl -fs "http://localhost:8081/api/v2/monitor/health" >/dev/null 2>&1; then
    echo ">> Airflow is up: http://localhost:8081"
    echo ">> Login: admin / password in $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
    exit 0
  fi
  sleep 2
done
echo "!! Airflow did not come up - see $LOG"
exit 1
