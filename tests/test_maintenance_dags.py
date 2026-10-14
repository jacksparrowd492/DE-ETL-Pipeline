"""
test_maintenance_dags.py
------------------------
DAG integrity tests for maintenance/dags/lakehouse_maintenance.py: the file
imports cleanly, defines the four DAGs with the expected schedules, and every
maintenance task is serialised through the lakehouse_maintenance pool.

Needs Airflow, which does not run on native Windows (no os.register_at_fork),
so the module is skipped there and in CI; run it in the WSL Airflow venv:
    source ~/airflow/env.sh && python -m pytest tests/test_maintenance_dags.py
"""

import unittest
from pathlib import Path

import pytest

try:
    from airflow.models.dagbag import DagBag
except Exception:  # ImportError, or AttributeError on native Windows
    # unittest.SkipTest (not pytest.skip) so `python -m unittest discover` skips it too
    raise unittest.SkipTest("Airflow is not importable here")

DAG_FOLDER = Path(__file__).resolve().parents[1] / "maintenance" / "dags"
MAINTENANCE_DAGS = {
    "delta_compaction",
    "delta_snapshot_expiry",
    "delta_vacuum",
    "lakehouse_optimize_and_benchmark",
}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER))


def test_dag_file_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


@pytest.mark.parametrize(
    "dag_id, schedule",
    [
        ("delta_compaction", "15 * * * *"),
        ("delta_snapshot_expiry", "0 2 * * *"),
        ("delta_vacuum", "0 3 * * *"),
        ("lakehouse_optimize_and_benchmark", None),
    ],
)
def test_schedules(dagbag, dag_id, schedule):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert dag.schedule == schedule
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_every_task_uses_the_one_slot_pool(dagbag):
    for dag in (d for d in dagbag.dags.values() if d.dag_id in MAINTENANCE_DAGS):
        for t in dag.tasks:
            assert t.pool == "lakehouse_maintenance", f"{dag.dag_id}.{t.task_id}"


def test_compaction_is_short_circuited_per_table(dagbag):
    dag = dagbag.get_dag("delta_compaction")
    for table in ("user_events", "user_events_rejected"):
        check = dag.get_task(f"needs_compaction__{table}")
        assert check.downstream_task_ids == {f"compact__{table}"}


def test_benchmark_dag_order(dagbag):
    dag = dagbag.get_dag("lakehouse_optimize_and_benchmark")
    chain = [
        "benchmark_before",
        "compact_tables",
        "expire_snapshots",
        "vacuum_tables",
        "benchmark_after",
        "report",
    ]
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    assert {"compact_tables", "expire_snapshots", "vacuum_tables", "run_tag"} <= dag.get_task(
        "report"
    ).upstream_task_ids


def test_every_task_alerts_on_failure_and_retry(dagbag):
    from observability.alerts import airflow_failure_callback, airflow_retry_callback

    for dag in dagbag.dags.values():
        for t in dag.tasks:
            assert airflow_failure_callback in _callbacks(t.on_failure_callback), t.task_id
            assert airflow_retry_callback in _callbacks(t.on_retry_callback), t.task_id


def _callbacks(value):
    return value if isinstance(value, list) else [value]


def test_batch_pipeline_dag_is_exposed_with_its_alerts(dagbag):
    """fakestore_pipeline_dag.py imports the root DAG into this dags folder."""
    from observability.alerts import airflow_failure_callback, airflow_retry_callback

    dag = dagbag.get_dag("fakestore_pipeline")
    assert dag is not None
    chain = ["extract", "validate", "send_to_kafka", "replay_failed"]
    for upstream, downstream in zip(chain, chain[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    for t in dag.tasks:
        assert airflow_failure_callback in _callbacks(t.on_failure_callback)
        assert airflow_retry_callback in _callbacks(t.on_retry_callback)
