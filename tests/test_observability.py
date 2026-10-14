"""
test_observability.py
---------------------
Tests for observability/: structured logging with run context, Slack + email
alerts (HTTP and SMTP are patched, nothing is sent), alert dedup, metrics files,
track_task (duration, SLA breach, failure alert) and the Airflow callbacks.

The autouse fixture in conftest.py points LOG_DIR at a temp folder and turns
external alert channels off; tests that exercise a channel turn it back on.
"""

import json
import logging
import smtplib

import pytest
import requests

from observability import alerts, metrics, stream_health
from observability.logging_config import (
    JsonFormatter,
    get_run_context,
    new_run_id,
    run_context,
    setup_logging,
)


def _lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def root_logger_restored():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        if h not in handlers:
            root.removeHandler(h)
            h.close()
    root.setLevel(level)


# --------------------------------------------------------
# Logging
# --------------------------------------------------------


def test_run_context_nests_and_resets():
    assert get_run_context()["run_id"] is None
    with run_context(run_id="r1"):
        with run_context(task="extract"):
            assert get_run_context()["run_id"] == "r1"
            assert get_run_context()["task"] == "extract"
        assert get_run_context()["task"] is None
    assert get_run_context()["run_id"] is None


def test_new_run_id_is_unique_and_sortable():
    a, b = new_run_id(), new_run_id()
    assert a != b
    assert a[:8].isdigit()


def test_setup_logging_writes_text_and_json_with_run_context(
    isolated_observability, root_logger_restored
):
    setup_logging("unit", console=False)
    with run_context(run_id="run-42", task="validate"):
        logging.getLogger("x.y").info("hello %s", "world", extra={"product_id": 7})

    records = _lines(isolated_observability / "unit.jsonl")
    hello = next(r for r in records if r["message"] == "hello world")
    assert hello["run_id"] == "run-42"
    assert hello["task"] == "validate"
    assert hello["component"] == "unit"
    assert hello["product_id"] == 7
    assert "run=run-42 task=validate" in (isolated_observability / "unit.log").read_text("utf-8")


def test_setup_logging_is_idempotent(isolated_observability, root_logger_restored):
    setup_logging("unit", console=False)
    setup_logging("unit", console=False)
    ours = [h for h in logging.getLogger().handlers if getattr(h, "_observability", False)]
    assert len(ours) == 2  # one .log + one .jsonl, not four


def test_setup_logging_respects_log_to_file_false(
    isolated_observability, root_logger_restored, monkeypatch
):
    monkeypatch.setenv("LOG_TO_FILE", "false")
    setup_logging("unit", console=False)
    logging.getLogger("x").warning("not written")
    assert not (isolated_observability / "unit.jsonl").exists()


def test_json_formatter_includes_exception():
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.getLogger("t").makeRecord(
            "t", logging.ERROR, __file__, 1, "failed", (), exc_info=__import__("sys").exc_info()
        )
    payload = json.loads(JsonFormatter().format(record))
    assert payload["level"] == "ERROR"
    assert "ValueError: boom" in payload["exception"]


# --------------------------------------------------------
# Alerts
# --------------------------------------------------------


def test_alert_always_goes_to_the_file_even_with_no_channel(isolated_observability):
    result = alerts.send_alert("Disk full", "it is", severity="critical", details={"host": "a"})

    assert result == {"suppressed": False, "file": True, "slack": None, "email": None, "sns": None}
    [alert] = _lines(isolated_observability / "alerts.jsonl")
    assert alert["title"] == "Disk full"
    assert alert["severity"] == "critical"
    assert alert["details"] == {"host": "a"}


def test_slack_alert_posts_a_webhook_payload(monkeypatch, mocker):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/T/B/X")
    post = mocker.patch("observability.alerts.requests.post")

    with run_context(run_id="r9", task="send_to_kafka"):
        result = alerts.send_alert("Task failed", "KafkaError", details={"failed": 3})

    assert result["slack"] is True
    url = post.call_args.args[0]
    payload = post.call_args.kwargs["json"]
    assert url == "https://hooks.slack.test/T/B/X"
    assert payload["text"] == "[ERROR] Task failed"
    blocks = payload["attachments"][0]["blocks"]
    assert blocks[0]["text"]["text"] == "Task failed"
    assert "r9" in blocks[-1]["elements"][0]["text"]


def test_slack_failure_is_reported_not_raised(monkeypatch, mocker):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")
    mocker.patch("observability.alerts.requests.post", side_effect=requests.ConnectionError("down"))

    assert alerts.send_alert("x")["slack"] is False


def test_email_alert_uses_starttls_and_login(monkeypatch, mocker):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_USER", "bot@test")
    monkeypatch.setenv("SMTP_PASSWORD", "pw")
    monkeypatch.setenv("ALERT_EMAIL_TO", "a@test, b@test")
    smtp_cls = mocker.patch("observability.alerts.smtplib.SMTP")
    smtp = smtp_cls.return_value.__enter__.return_value

    result = alerts.send_alert("Load failed", "Databricks down", severity="critical")

    assert result["email"] is True
    smtp_cls.assert_called_once_with("smtp.test", 587, timeout=15)
    smtp.starttls.assert_called_once()
    smtp.login.assert_called_once_with("bot@test", "pw")
    msg = smtp.send_message.call_args.args[0]
    assert msg["Subject"] == "[CRITICAL][local] Load failed"
    assert msg["To"] == "a@test, b@test"
    assert "Databricks down" in msg.get_content()


def test_email_failure_is_reported_not_raised(monkeypatch, mocker):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("ALERT_EMAIL_TO", "a@test")
    mocker.patch(
        "observability.alerts.smtplib.SMTP", side_effect=smtplib.SMTPConnectError(421, "no")
    )

    assert alerts.send_alert("x")["email"] is False


def test_alerts_below_min_severity_only_reach_the_file(monkeypatch, mocker):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")
    monkeypatch.setenv("ALERT_MIN_SEVERITY", "error")
    post = mocker.patch("observability.alerts.requests.post")

    result = alerts.send_alert("slow", severity="warning")

    assert result["file"] is True and result["slack"] is None
    post.assert_not_called()


def test_dedup_key_suppresses_repeats_within_the_cooldown(isolated_observability):
    first = alerts.send_alert("lagging", dedup_key="lag")
    second = alerts.send_alert("lagging", dedup_key="lag")
    other = alerts.send_alert("lagging", dedup_key="other")

    assert first["suppressed"] is False
    assert second["suppressed"] is True
    assert other["suppressed"] is False
    assert len(_lines(isolated_observability / "alerts.jsonl")) == 2


class _TI:
    dag_id = "fakestore_pipeline"
    task_id = "send_to_kafka"
    run_id = "scheduled__2026-10-14"
    try_number = 2
    max_tries = 1
    log_url = "http://localhost:8081/log"


def test_airflow_failure_callback_sends_a_critical_alert(mocker):
    send = mocker.patch("observability.alerts.send_alert")

    alerts.airflow_failure_callback({"ti": _TI(), "exception": RuntimeError("broker down")})

    title, message = send.call_args.args
    assert title == "Airflow task failed: fakestore_pipeline.send_to_kafka"
    assert message == "RuntimeError: broker down"
    assert send.call_args.kwargs["severity"] == "critical"
    assert send.call_args.kwargs["details"]["try_number"] == 2


def test_airflow_retry_callback_sends_a_warning(mocker):
    send = mocker.patch("observability.alerts.send_alert")

    alerts.airflow_retry_callback({"ti": _TI()})

    assert send.call_args.kwargs["severity"] == "warning"
    assert "retrying" in send.call_args.args[0]


# --------------------------------------------------------
# Metrics + track_task
# --------------------------------------------------------


def test_record_writes_jsonl_and_prometheus(isolated_observability):
    with run_context(run_id="r1", component="unit"):
        metrics.record("rows", 5, {"table": "bronze"})
        metrics.record("rows", 7, {"table": "bronze"})

    [a, b] = _lines(isolated_observability / "metrics.jsonl")
    assert (a["value"], b["value"]) == (5.0, 7.0)
    assert b["labels"] == {"table": "bronze"} and b["run_id"] == "r1"
    prom = (isolated_observability / "metrics_unit.prom").read_text("utf-8")
    assert "# TYPE etl_rows gauge" in prom
    assert 'etl_rows{component="unit",table="bronze"} 7.0' in prom  # latest value only


def test_record_pushes_to_pushgateway_when_configured(monkeypatch, mocker):
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://pushgw:9091/")
    put = mocker.patch("observability.metrics.requests.put")

    metrics.record("rows", 1)

    assert put.call_args.args[0] == "http://pushgw:9091/metrics/job/pipeline"
    assert "etl_rows" in put.call_args.kwargs["data"]


def test_track_task_records_success_and_counts(isolated_observability):
    with metrics.track_task("extract", sla_seconds=60) as t:
        t.count("records_out", 20)

    samples = {
        (s["name"], s["labels"].get("kind")): s
        for s in _lines(isolated_observability / "metrics.jsonl")
    }
    assert samples[("task_success", None)]["value"] == 1
    assert samples[("task_records", "records_out")]["value"] == 20
    assert samples[("task_duration_seconds", None)]["task"] == "extract"
    assert not (isolated_observability / "alerts.jsonl").exists()


def test_track_task_alerts_and_reraises_on_failure(isolated_observability):
    with pytest.raises(RuntimeError):
        with metrics.track_task("load"):
            raise RuntimeError("warehouse stopped")

    [alert] = _lines(isolated_observability / "alerts.jsonl")
    assert alert["severity"] == "critical"
    assert alert["title"] == "Pipeline task failed: load"
    assert "warehouse stopped" in alert["message"]
    success = [
        s for s in _lines(isolated_observability / "metrics.jsonl") if s["name"] == "task_success"
    ]
    assert success[0]["value"] == 0


def test_track_task_can_leave_failure_alerts_to_airflow(isolated_observability):
    with pytest.raises(RuntimeError):
        with metrics.track_task("load", alert_on_failure=False):
            raise RuntimeError("x")

    assert not (isolated_observability / "alerts.jsonl").exists()


def test_track_task_sla_breach_sends_a_warning(isolated_observability, monkeypatch):
    monkeypatch.setenv("SLA_SECONDS_SEND_TO_KAFKA", "0")  # env overrides the code value

    with metrics.track_task("send_to_kafka", sla_seconds=999):
        pass

    [alert] = _lines(isolated_observability / "alerts.jsonl")
    assert alert["severity"] == "warning"
    assert alert["title"] == "Pipeline latency SLA breached: send_to_kafka"


# --------------------------------------------------------
# Streaming health checks
# --------------------------------------------------------


def test_stream_batch_within_thresholds_raises_nothing():
    assert stream_health.observe_batch(1, "t", 100, 5, inserted=100, latency_p95=3.0) == []


def test_stream_batch_reject_ratio_and_latency_alerts(isolated_observability):
    raised = stream_health.observe_batch(2, "t", 60, 40, inserted=60, latency_p95=500)

    assert raised == [
        "Streaming data quality: high reject ratio",
        "Streaming latency: events land late in the lakehouse",
    ]


def test_stream_reject_ratio_ignored_for_tiny_batches():
    assert stream_health.observe_batch(3, "t", 1, 2) == []


def test_stream_slow_trigger_alerts():
    raised = stream_health.observe_progress("q", 4, 1000, 50.0, 10.0, trigger_ms=90_000)
    assert raised == ["Streaming latency: micro-batch slower than its trigger budget"]


def test_stream_termination_with_error_is_critical(isolated_observability):
    stream_health.observe_termination("kafka_to_delta", "StreamingQueryException: lost broker")

    [alert] = _lines(isolated_observability / "alerts.jsonl")
    assert alert["severity"] == "critical"


# --------------------------------------------------------
# Secrets Manager -> environment (cloud runtimes)
# --------------------------------------------------------


def test_load_secret_env_does_nothing_locally(monkeypatch):
    from observability.cloud_secrets import load_secret_env

    monkeypatch.delenv("PIPELINE_SECRET_ID", raising=False)
    assert load_secret_env() == []


def test_load_secret_env_fills_missing_keys_only(monkeypatch, mocker):
    from observability.cloud_secrets import load_secret_env

    monkeypatch.setenv("SMTP_HOST", "kept.example")
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    client = mocker.Mock()
    client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {"SLACK_WEBHOOK_URL": "https://hooks.slack.test/x", "SMTP_HOST": "ignored", "EMPTY": ""}
        )
    }

    loaded = load_secret_env("fakestore-etl-dev/pipeline", client=client)

    assert loaded == ["SLACK_WEBHOOK_URL"]
    client.get_secret_value.assert_called_once_with(SecretId="fakestore-etl-dev/pipeline")
    import os

    assert os.environ["SLACK_WEBHOOK_URL"] == "https://hooks.slack.test/x"
    assert os.environ["SMTP_HOST"] == "kept.example"
    monkeypatch.delenv("SLACK_WEBHOOK_URL")


def test_sns_alert_publishes_to_the_topic(monkeypatch, mocker):
    import sys
    import types

    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("ALERT_SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:etl-alerts")
    client = mocker.Mock()
    fake_boto3 = types.SimpleNamespace(client=mocker.Mock(return_value=client))
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    result = alerts.send_alert("Stream stopped", "lost broker", severity="critical")

    assert result["sns"] is True
    fake_boto3.client.assert_called_once_with("sns", region_name="us-east-1")
    kwargs = client.publish.call_args.kwargs
    assert kwargs["TopicArn"].endswith(":etl-alerts")
    assert kwargs["Subject"] == "[CRITICAL][local] Stream stopped"
