#!/usr/bin/env bash
# Week 4 - one-time install of Apache Airflow inside WSL (Ubuntu), no Docker, no sudo.
#   bash maintenance/setup_airflow.sh
# Creates ~/airflow-venv (Python 3.12 via uv) with Airflow + the Trino client and
# delta-rs, and an AIRFLOW_HOME in ~/airflow whose dags_folder is maintenance/dags.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.3.2}"
PY="3.12"
VENV="$HOME/airflow-venv"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"

if ! command -v uv >/dev/null && [ ! -x "$HOME/.local/bin/uv" ]; then
  echo ">> Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
UV="$(command -v uv || echo "$HOME/.local/bin/uv")"

[ -d "$VENV" ] || "$UV" venv --python "$PY" "$VENV"
echo ">> Installing apache-airflow==$AIRFLOW_VERSION (+ trino, deltalake) into $VENV"
"$UV" pip install --python "$VENV/bin/python" \
  "apache-airflow==$AIRFLOW_VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PY.txt"
"$UV" pip install --python "$VENV/bin/python" trino deltalake pyarrow

mkdir -p "$AIRFLOW_HOME"
# SQLite metadata DB + LocalExecutor is enough for one machine. Keep AIRFLOW_HOME on
# the Linux file system (SQLite locking does not work on /mnt/f); only the DAGs live on F:.
cat > "$AIRFLOW_HOME/env.sh" <<EOF
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__DAGS_FOLDER="$HERE/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30
# Trino owns port 8080, so the API server (UI + the Execution API that task workers
# call) moves to 8081
export AIRFLOW__API__PORT=8081
export AIRFLOW__API__BASE_URL=http://localhost:8081
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8081/execution/
export LAKEHOUSE_ROOT="$(cd "$HERE/../streaming/_lakehouse" && pwd)"
export TRINO_HOST=localhost TRINO_PORT=8080
source "$VENV/bin/activate"
EOF
source "$AIRFLOW_HOME/env.sh"
airflow version
airflow db migrate >/dev/null
echo ">> Done. Start it with: bash maintenance/start_airflow.sh"
