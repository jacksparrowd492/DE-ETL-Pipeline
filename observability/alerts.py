"""
alerts.py
---------
Failure and latency alerts, sent to Slack, email and/or Amazon SNS.

    send_alert(title, message, severity="error", details={...})

Channels (each one is used only if it is configured):

    Slack   SLACK_WEBHOOK_URL - a Slack "incoming webhook" URL
    Email   SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASSWORD,
            SMTP_USE_TLS (true: STARTTLS) / SMTP_USE_SSL (false: implicit TLS, port 465),
            ALERT_EMAIL_FROM, ALERT_EMAIL_TO (comma-separated)
    SNS     ALERT_SNS_TOPIC_ARN - cloud deployments: the topic the CloudWatch alarms
            also use (deploy/terraform/aws/monitoring.tf), so email works without SMTP
    File    logs/alerts.jsonl - always written (unless LOG_TO_FILE=false, e.g. in the
            cloud, where the alert's JSON log line goes to CloudWatch instead), so an
            alert is never lost even when no channel is configured or Slack/SMTP is down

Other settings
    ALERTS_ENABLED          true (default) | false - false keeps only the file channel
    ALERT_MIN_SEVERITY      info | warning (default) | error | critical - lower ones
                            go to the file only
    ALERT_COOLDOWN_SECONDS  300 - an alert with the same dedup_key is sent at most
                            once per cooldown per process (e.g. a consumer that
                            fails on every message sends one alert, not thousands)
    ALERT_ENV               label shown in every alert, e.g. local / dev / prod

Sending never raises: a broken alert channel must not turn a warning into a
pipeline failure, or hide the original error.

Airflow: airflow_failure_callback / airflow_retry_callback are wired into the
DAGs' default_args (on_failure_callback / on_retry_callback).
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import socket
import threading
import time
from datetime import datetime, timezone
from email.message import EmailMessage

import requests

from observability.logging_config import env_flag, get_run_context, log_dir

logger = logging.getLogger(__name__)

SEVERITIES = ("info", "warning", "error", "critical")
SLACK_COLORS = {"info": "#3987e5", "warning": "#fab219", "error": "#d03b3b", "critical": "#8b0000"}
SLACK_ICONS = {
    "info": ":information_source:",
    "warning": ":warning:",
    "error": ":x:",
    "critical": ":rotating_light:",
}

_last_sent: dict = {}
_lock = threading.Lock()


def _rank(severity: str) -> int:
    return SEVERITIES.index(severity) if severity in SEVERITIES else SEVERITIES.index("error")


# --------------------------------------------------------
# Channels
# --------------------------------------------------------


def _write_file(alert: dict) -> bool:
    try:
        os.makedirs(log_dir(), exist_ok=True)
        with open(os.path.join(log_dir(), "alerts.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(alert, default=str) + "\n")
        return True
    except OSError:
        logger.exception("Could not write alert to the alerts file")
        return False


def _send_slack(alert: dict) -> bool | None:
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return None

    severity = alert["severity"]
    fields = [
        {"type": "mrkdwn", "text": f"*{k}*\n{v}"}
        for k, v in alert["details"].items()
        if v not in (None, "")
    ][:10]  # Slack allows at most 10 fields per section
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{alert['title']}"[:150]},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": alert["message"][:2900] or " "}},
    ]
    if fields:
        blocks.append({"type": "section", "fields": fields})
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{SLACK_ICONS[severity]} *{severity.upper()}* · {alert['env']} · "
                    f"{alert['component']} · run `{alert['run_id']}` · {alert['host']} · {alert['ts']}",
                }
            ],
        }
    )
    payload = {
        "text": f"[{severity.upper()}] {alert['title']}",  # notification preview text
        "attachments": [{"color": SLACK_COLORS[severity], "blocks": blocks}],
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error("Slack alert failed: %s", e)
        return False


def _send_email(alert: dict) -> bool | None:
    host = os.environ.get("SMTP_HOST")
    recipients = [a.strip() for a in os.environ.get("ALERT_EMAIL_TO", "").split(",") if a.strip()]
    if not host or not recipients:
        return None

    use_ssl = env_flag("SMTP_USE_SSL", False)
    port = int(os.environ.get("SMTP_PORT") or (465 if use_ssl else 587))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASSWORD")

    msg = EmailMessage()
    msg["Subject"] = f"[{alert['severity'].upper()}][{alert['env']}] {alert['title']}"
    msg["From"] = os.environ.get("ALERT_EMAIL_FROM") or user or f"etl-pipeline@{alert['host']}"
    msg["To"] = ", ".join(recipients)
    lines = [alert["message"], ""]
    lines += [f"{k}: {v}" for k, v in alert["details"].items() if v not in (None, "")]
    lines += [
        "",
        f"severity:  {alert['severity']}",
        f"component: {alert['component']}",
        f"run_id:    {alert['run_id']}",
        f"task:      {alert['task']}",
        f"host:      {alert['host']}",
        f"time:      {alert['ts']}",
    ]
    msg.set_content("\n".join(lines))

    try:
        smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with smtp_cls(host, port, timeout=15) as smtp:
            if not use_ssl and env_flag("SMTP_USE_TLS", True):
                smtp.starttls()
            if user and password:
                smtp.login(user, password)
            smtp.send_message(msg)
        return True
    except (smtplib.SMTPException, OSError) as e:
        logger.error("Email alert failed: %s", e)
        return False


def _send_sns(alert: dict) -> bool | None:
    topic = os.environ.get("ALERT_SNS_TOPIC_ARN")
    if not topic:
        return None

    lines = [alert["message"], ""] + [f"{k}: {v}" for k, v in alert["details"].items()]
    lines += [
        "",
        f"component: {alert['component']}",
        f"run_id: {alert['run_id']}",
        f"time: {alert['ts']}",
    ]
    try:
        import boto3  # cloud runtimes only

        boto3.client("sns", region_name=topic.split(":")[3]).publish(
            TopicArn=topic,
            Subject=f"[{alert['severity'].upper()}][{alert['env']}] {alert['title']}"[:100],
            Message="\n".join(lines),
        )
        return True
    except Exception as e:  # noqa: BLE001 - ImportError, botocore errors
        logger.error("SNS alert failed: %s", e)
        return False


# --------------------------------------------------------
# Public API
# --------------------------------------------------------


def send_alert(
    title: str,
    message: str = "",
    severity: str = "error",
    details: dict | None = None,
    dedup_key: str | None = None,
) -> dict:
    """
    Send one alert to every configured channel. Returns what happened per
    channel: True = sent, False = failed, None = not configured / not sent,
    plus "suppressed" when the dedup cooldown swallowed it.
    """

    severity = severity if severity in SEVERITIES else "error"
    ctx = get_run_context()
    alert = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "severity": severity,
        "title": title,
        "message": message,
        "details": {k: str(v) for k, v in (details or {}).items()},
        "env": os.environ.get("ALERT_ENV", "local"),
        "component": ctx["component"],
        "run_id": ctx["run_id"] or "-",
        "task": ctx["task"] or "-",
        "host": socket.gethostname(),
        "dedup_key": dedup_key,
    }

    if dedup_key:
        cooldown = float(os.environ.get("ALERT_COOLDOWN_SECONDS", "300"))
        now = time.monotonic()
        with _lock:
            last = _last_sent.get(dedup_key)
            if last is not None and now - last < cooldown:
                logger.info("Alert suppressed (cooldown) | %s", title)
                return {"suppressed": True, "file": None, "slack": None, "email": None, "sns": None}
            _last_sent[dedup_key] = now

    log_level = logging.WARNING if _rank(severity) <= _rank("warning") else logging.ERROR
    # the whole alert as structured fields, for JSON logs / CloudWatch metric filters
    logger.log(log_level, "ALERT [%s] %s | %s", severity, title, message, extra={"alert": alert})

    to_file = _write_file(alert) if env_flag("LOG_TO_FILE", True) else None
    result = {"suppressed": False, "file": to_file, "slack": None, "email": None, "sns": None}

    min_severity = os.environ.get("ALERT_MIN_SEVERITY", "warning").lower()
    if env_flag("ALERTS_ENABLED", True) and _rank(severity) >= _rank(min_severity):
        result["slack"] = _send_slack(alert)
        result["email"] = _send_email(alert)
        result["sns"] = _send_sns(alert)

    return result


def reset_cooldowns() -> None:
    """Forget every dedup_key (used by tests)."""
    with _lock:
        _last_sent.clear()


# --------------------------------------------------------
# Airflow callbacks
# --------------------------------------------------------


def _airflow_details(context: dict) -> dict:
    ti = context.get("ti") or context.get("task_instance")
    dag_run = context.get("dag_run")
    return {
        "dag_id": getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None) or context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "logical_date": getattr(dag_run, "logical_date", None) or context.get("logical_date"),
        "log_url": getattr(ti, "log_url", None),
    }


def airflow_failure_callback(context: dict) -> None:
    """on_failure_callback: the task failed and has no retries left."""
    details = _airflow_details(context)
    exception = context.get("exception")
    send_alert(
        f"Airflow task failed: {details['dag_id']}.{details['task_id']}",
        f"{type(exception).__name__}: {exception}" if exception else "Task failed.",
        severity="critical",
        details=details,
    )


def airflow_retry_callback(context: dict) -> None:
    """on_retry_callback: the task failed but Airflow will retry it."""
    details = _airflow_details(context)
    exception = context.get("exception")
    send_alert(
        f"Airflow task retrying: {details['dag_id']}.{details['task_id']}",
        f"{type(exception).__name__}: {exception}" if exception else "Task will be retried.",
        severity="warning",
        details=details,
    )
