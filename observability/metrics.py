"""
metrics.py
----------
Pipeline metrics, with no metrics server needed to collect them.

    record(name, value, labels={...}, unit="s")

Each call
  1. appends one JSON line to logs/metrics.jsonl (ts, name, value, labels, run
     context). The Streamlit "Monitoring" page and CloudWatch Logs metric
     filters read this file.
  2. updates logs/metrics_<component>.prom, a Prometheus text-format file with
     the latest value of every series this process recorded. Point
     node_exporter's textfile collector at logs/ to scrape it.
  3. if PUSHGATEWAY_URL is set, pushes the same series to a Prometheus
     Pushgateway (for short-lived batch runs that finish before a scrape).
  4. if METRICS_TO_LOG=true, logs the sample as a structured log line, so on a
     cloud runtime (LOG_FORMAT=json, LOG_TO_FILE=false) CloudWatch Logs metric
     filters can build metrics and alarms from stdout alone.
LOG_TO_FILE=false skips 1 and 2.

    with track_task("extract", sla_seconds=30) as t:
        products = extract_products()
        t.count("records_out", len(products))

track_task records task_duration_seconds, task_success and task_records,
sends a warning alert when the task is slower than its latency SLA, and a
critical alert (then re-raises) when it fails. SLA_SECONDS_<TASK> in the
environment overrides the SLA in code, e.g. SLA_SECONDS_EXTRACT=60.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import threading
import time
import traceback
from datetime import datetime, timezone

import requests

from observability.alerts import send_alert
from observability.logging_config import env_flag, get_run_context, log_dir, run_context

logger = logging.getLogger(__name__)

METRIC_PREFIX = "etl_"
_latest: dict = {}  # (name, sorted label items) -> (value, unit)
_lock = threading.Lock()


# --------------------------------------------------------
# Recording
# --------------------------------------------------------


def _prom_name(name: str) -> str:
    return METRIC_PREFIX + re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _prom_escape(value) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _prom_text(series: dict) -> str:
    lines = []
    seen = set()
    for (name, labels), (value, _unit) in sorted(series.items(), key=lambda kv: kv[0]):
        metric = _prom_name(name)
        if metric not in seen:
            lines.append(f"# TYPE {metric} gauge")
            seen.add(metric)
        label_str = ",".join(f'{k}="{_prom_escape(v)}"' for k, v in labels)
        lines.append(
            f"{metric}{{{label_str}}} {float(value)}" if label_str else f"{metric} {float(value)}"
        )
    return "\n".join(lines) + "\n"


def _write_prometheus(component: str, text: str) -> None:
    path = os.path.join(log_dir(), f"metrics_{component}.prom")
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)  # atomic: a scrape never sees a half-written file


def _push_gateway(component: str, text: str) -> None:
    url = os.environ.get("PUSHGATEWAY_URL")
    if not url:
        return
    try:
        requests.put(f"{url.rstrip('/')}/metrics/job/{component}", data=text, timeout=5)
    except requests.RequestException as e:
        logger.warning("Pushgateway push failed: %s", e)


def record(name: str, value: float, labels: dict | None = None, unit: str | None = None) -> dict:
    """Record one metric sample. Never raises: metrics must not break the pipeline."""

    ctx = get_run_context()
    labels = {k: str(v) for k, v in (labels or {}).items()}
    sample = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "name": name,
        "value": float(value),
        "unit": unit,
        "labels": labels,
        **ctx,
    }

    try:
        if env_flag("METRICS_TO_LOG", False):
            # one JSON log line per sample (LOG_FORMAT=json): CloudWatch Logs metric
            # filters turn {"metric": {"name": ..., "value": ...}} into metrics
            logger.info("metric %s=%s", name, sample["value"], extra={"metric": sample})

        series_labels = tuple(sorted({"component": ctx["component"], **labels}.items()))
        with _lock:
            _latest[(name, series_labels)] = (float(value), unit)
            text = _prom_text(
                {k: v for k, v in _latest.items() if dict(k[1])["component"] == ctx["component"]}
            )

        if env_flag("LOG_TO_FILE", True):
            os.makedirs(log_dir(), exist_ok=True)
            with open(os.path.join(log_dir(), "metrics.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps(sample, default=str) + "\n")
            _write_prometheus(ctx["component"], text)
        _push_gateway(ctx["component"], text)
    except Exception:  # noqa: BLE001 - see docstring
        logger.exception("Could not record metric %s", name)

    return sample


def reset() -> None:
    """Forget the in-process series (used by tests)."""
    with _lock:
        _latest.clear()


# --------------------------------------------------------
# Task tracking
# --------------------------------------------------------


class TaskStats:
    def __init__(self, task: str):
        self.task = task
        self.counts: dict = {}
        self.duration: float | None = None

    def count(self, kind: str, n: int) -> None:
        """Record how many records the task handled, e.g. count("records_out", 20)."""
        self.counts[kind] = self.counts.get(kind, 0) + int(n)


def sla_for(task: str, default: float | None) -> float | None:
    env = os.environ.get(f"SLA_SECONDS_{re.sub(r'[^A-Z0-9]', '_', task.upper())}")
    return float(env) if env else default


@contextlib.contextmanager
def track_task(
    task: str,
    sla_seconds: float | None = None,
    alert_on_failure: bool = True,
    labels: dict | None = None,
):
    """
    Time a pipeline step and emit its metrics. alert_on_failure=False leaves
    failure alerting to the caller (Airflow's on_failure_callback, which knows
    whether a retry is still coming).
    """

    sla = sla_for(task, sla_seconds)
    stats = TaskStats(task)
    labels = {"task": task, **(labels or {})}

    with run_context(task=task):
        logger.info("Task %s started", task)
        start = time.perf_counter()
        try:
            yield stats
        except BaseException as e:
            stats.duration = time.perf_counter() - start
            record("task_duration_seconds", stats.duration, labels, unit="s")
            record("task_success", 0, labels)
            logger.exception("Task %s failed after %.2fs", task, stats.duration)
            if alert_on_failure and not isinstance(e, KeyboardInterrupt):
                tb = traceback.extract_tb(e.__traceback__)
                where = f"{tb[-1].filename}:{tb[-1].lineno}" if tb else "-"
                send_alert(
                    f"Pipeline task failed: {task}",
                    f"{type(e).__name__}: {e}",
                    severity="critical",
                    details={
                        "task": task,
                        "duration_s": f"{stats.duration:.2f}",
                        "raised_at": where,
                    },
                )
            raise

        stats.duration = time.perf_counter() - start
        record("task_duration_seconds", stats.duration, labels, unit="s")
        record("task_success", 1, labels)
        for kind, n in stats.counts.items():
            record("task_records", n, {**labels, "kind": kind})

        logger.info(
            "Task %s succeeded in %.2fs %s",
            task,
            stats.duration,
            " ".join(f"{k}={v}" for k, v in stats.counts.items()),
        )

        if sla is not None and stats.duration > sla:
            record("task_sla_breach", 1, labels)
            send_alert(
                f"Pipeline latency SLA breached: {task}",
                f"{task} took {stats.duration:.1f}s, SLA is {sla:.0f}s.",
                severity="warning",
                details={"task": task, "duration_s": f"{stats.duration:.2f}", "sla_s": sla},
                dedup_key=f"sla:{task}",
            )
