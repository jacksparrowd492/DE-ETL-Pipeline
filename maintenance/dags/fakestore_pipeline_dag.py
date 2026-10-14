"""
fakestore_pipeline_dag.py
-------------------------
Makes the batch DAG (fakestore_pipeline.py, at the project root) visible to the
WSL Airflow, whose dags_folder is this maintenance/dags folder (~/airflow/env.sh).
The DAG itself - tasks, retries, Slack/email failure callbacks - is defined
once, in fakestore_pipeline.py; this file only imports it.

New DAGs start paused (core.dags_are_paused_at_creation), so it runs only after
you unpause or trigger it in the UI (http://localhost:8081).
"""

import sys
from pathlib import Path

# project root: fakestore_pipeline.py and the packages it imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fakestore_pipeline import dag  # noqa: E402,F401  (Airflow collects module-level DAGs)
