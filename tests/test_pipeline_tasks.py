"""
test_pipeline_tasks.py
----------------------
Tests for the instrumented pipeline steps (pipeline_tasks.py) shared by the
Airflow DAG and run_pipeline_no_airflow.py, and for the runner itself. The
production functions (API, Kafka, replay) are patched; the observability and
lineage code runs for real against the temp LOG_DIR from conftest.py.
"""

import json

import pytest

import pipeline_tasks
import run_pipeline_no_airflow


def _jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _alerts(log_dir):
    return _jsonl(log_dir / "alerts.jsonl")


def _lineage(log_dir):
    return _jsonl(log_dir / "lineage" / "openlineage.jsonl")


@pytest.fixture
def patched(mocker, raw_products):
    return {
        "extract": mocker.patch("pipeline_tasks.extract_products", return_value=raw_products),
        "send": mocker.patch(
            "pipeline_tasks.send_products", return_value={"success": 1, "failed": 0}
        ),
        "replay": mocker.patch(
            "pipeline_tasks.replay_failed_records", return_value={"replayed": 0, "remaining": 0}
        ),
    }


def test_full_run_emits_metrics_and_lineage_and_no_alerts(
    patched, isolated_observability, monkeypatch
):
    monkeypatch.setenv("VALIDATION_REJECT_ALERT_RATIO", "0.9")  # 1 of 2 invalid is fine here

    result = run_pipeline_no_airflow.run(run_id="test-run")

    assert result["extracted"] == 2 and result["validated"] == 1
    assert _alerts(isolated_observability) == []

    events = _lineage(isolated_observability)
    jobs = [(e["job"]["name"], e["eventType"]) for e in events]
    assert jobs == [
        ("fakestore_pipeline.extract", "START"),
        ("fakestore_pipeline.extract", "COMPLETE"),
        ("fakestore_pipeline.validate", "START"),
        ("fakestore_pipeline.validate", "COMPLETE"),
        ("fakestore_pipeline.send_to_kafka", "START"),
        ("fakestore_pipeline.send_to_kafka", "COMPLETE"),
        ("fakestore_pipeline.replay_failed", "START"),
        ("fakestore_pipeline.replay_failed", "COMPLETE"),
    ]
    assert {e["run"]["facets"]["etl_run"]["pipeline_run_id"] for e in events} == {"test-run"}

    metrics = _jsonl(isolated_observability / "metrics.jsonl")
    tasks = {m["labels"].get("task") for m in metrics if m["name"] == "task_success"}
    assert tasks == {"extract", "validate", "send_to_kafka", "replay_failed"}
    run_ok = [m for m in metrics if m["name"] == "pipeline_run_success"]
    assert run_ok[-1]["value"] == 1 and run_ok[-1]["run_id"] == "test-run"


def test_failed_step_alerts_and_stops_the_run(patched, isolated_observability):
    patched["extract"].side_effect = ConnectionError("FakeStore API unreachable")

    with pytest.raises(ConnectionError):
        run_pipeline_no_airflow.run(run_id="bad-run")

    [alert] = _alerts(isolated_observability)
    assert alert["title"] == "Pipeline task failed: extract"
    assert alert["severity"] == "critical"
    assert alert["run_id"] == "bad-run"
    patched["send"].assert_not_called()
    events = _lineage(isolated_observability)
    assert events[-1]["eventType"] == "FAIL"
    run_ok = [
        m
        for m in _jsonl(isolated_observability / "metrics.jsonl")
        if m["name"] == "pipeline_run_success"
    ]
    assert run_ok[-1]["value"] == 0


def test_runner_main_exits_1_on_failure(patched, mocker):
    patched["extract"].side_effect = ConnectionError("down")
    mocker.patch("run_pipeline_no_airflow.setup_logging")
    mocker.patch("sys.argv", ["run_pipeline_no_airflow.py"])

    import runpy

    with pytest.raises(SystemExit) as exc:
        runpy.run_module("run_pipeline_no_airflow", run_name="__main__")
    assert exc.value.code == 1


def test_extract_fails_on_an_empty_api_response(patched):
    patched["extract"].return_value = []
    with pytest.raises(ValueError, match="no products"):
        pipeline_tasks.extract()


def test_validate_warns_on_a_high_reject_ratio(isolated_observability, raw_products):
    pipeline_tasks.validate(raw_products)  # 1 of 2 rejected = 50% > 20%

    [alert] = _alerts(isolated_observability)
    assert alert["title"] == "Data quality: validation rejected too many products"
    assert alert["severity"] == "warning"


def test_validate_fails_when_nothing_is_valid(raw_products):
    with pytest.raises(ValueError, match="all 1 products failed"):
        pipeline_tasks.validate([raw_products[1]])


def test_partial_kafka_failure_sends_an_error_alert(patched, isolated_observability, raw_product):
    patched["send"].return_value = {"success": 1, "failed": 2}

    result = pipeline_tasks.send_to_kafka([raw_product] * 3)

    assert result["failed"] == 2
    [alert] = _alerts(isolated_observability)
    assert alert["severity"] == "error"
    assert "2 of 3" in alert["message"]


def test_total_kafka_failure_fails_the_task(patched, raw_product):
    patched["send"].return_value = {"success": 0, "failed": 1}
    with pytest.raises(RuntimeError, match="all 1 Kafka sends failed"):
        pipeline_tasks.send_to_kafka([raw_product])


def test_airflow_mode_leaves_failure_alerts_to_the_callback(
    patched, isolated_observability, raw_product
):
    patched["send"].return_value = {"success": 0, "failed": 1}
    with pytest.raises(RuntimeError):
        pipeline_tasks.send_to_kafka([raw_product], alert_on_failure=False)
    assert _alerts(isolated_observability) == []


def test_replay_warns_when_records_remain(patched, isolated_observability):
    patched["replay"].return_value = {"replayed": 1, "remaining": 2}

    pipeline_tasks.replay_failed()

    [alert] = _alerts(isolated_observability)
    assert alert["title"] == "Replay queue not empty"
