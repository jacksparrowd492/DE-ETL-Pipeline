"""
logging_config.py
-----------------
One logging setup for the whole pipeline. Entry points (run_local.py,
run_pipeline_no_airflow.py, the Kafka consumer, the DAG tasks, the Spark job,
the maintenance CLIs) call setup_logging(component) once; library modules
only do `logger = logging.getLogger(__name__)` and never configure handlers.

Handlers added to the root logger:

    console                      human-readable text (or JSON with LOG_FORMAT=json,
                                 e.g. when stdout is shipped to CloudWatch Logs)
    logs/<component>.log         same text, rotated at 10 MB x 5
    logs/<component>.jsonl       one JSON object per record, rotated at 10 MB x 5

Every record carries the current run_id / task / component, set with
run_context(), so the log lines of one pipeline run (or one Airflow task try)
can be pulled out of any of these files with a single filter. One file pair per
component, so separate processes never rotate the same file.

Environment
    LOG_LEVEL     DEBUG | INFO (default) | WARNING | ERROR
    LOG_DIR       default <project>/logs
    LOG_FORMAT    text (default) | json   - console format only; files are fixed
    LOG_TO_FILE   true (default) | false  - e.g. false on a container/cloud runtime
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import logging.handlers
import os
import uuid
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEXT_FORMAT = "%(asctime)s | %(levelname)s | %(component)s | run=%(run_id)s task=%(task)s | %(name)s | %(message)s"
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5

# Attributes every LogRecord has; anything else on a record came from `extra=`
_STANDARD_ATTRS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}
_CONTEXT_ATTRS = {"run_id", "task", "component"}

_run_id = contextvars.ContextVar("run_id", default=None)
_task = contextvars.ContextVar("task", default=None)
_component = contextvars.ContextVar("component", default=None)
_default_component = "pipeline"


def log_dir() -> str:
    return os.environ.get("LOG_DIR") or os.path.join(PROJECT_ROOT, "logs")


def env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


# --------------------------------------------------------
# Run context (run_id / task / component on every record)
# --------------------------------------------------------


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]


def get_run_context() -> dict:
    return {
        "run_id": _run_id.get(),
        "task": _task.get(),
        "component": _component.get() or _default_component,
    }


@contextlib.contextmanager
def run_context(run_id: str | None = None, task: str | None = None, component: str | None = None):
    """
    Tags every log record, metric and alert emitted inside the block. Values left
    as None keep the enclosing context's value, so a task block nested inside a
    run block keeps the run_id.
    """

    tokens = []
    for var, value in ((_run_id, run_id), (_task, task), (_component, component)):
        if value is not None:
            tokens.append((var, var.set(value)))
    try:
        yield get_run_context()
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = get_run_context()
        for key in _CONTEXT_ATTRS:
            if getattr(record, key, None) is None:
                setattr(record, key, ctx[key] if ctx[key] is not None else "-")
        return True


# --------------------------------------------------------
# JSON formatter
# --------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """One JSON object per line: timestamp, level, logger, message, run context,
    any `extra=` fields, and the formatted exception if there is one."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "component": getattr(record, "component", None),
            "run_id": getattr(record, "run_id", None),
            "task": getattr(record, "task", None),
            "module": record.module,
            "line": record.lineno,
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS and key not in _CONTEXT_ATTRS and key not in payload:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


# --------------------------------------------------------
# Setup
# --------------------------------------------------------


def _ours(handler: logging.Handler) -> bool:
    return getattr(handler, "_observability", False)


def _tag(handler: logging.Handler) -> logging.Handler:
    handler._observability = True
    handler.addFilter(ContextFilter())
    return handler


def _has_foreign_console(root: logging.Logger) -> bool:
    """True if someone else (Airflow, Streamlit, pytest) already logs to a stream."""
    return any(
        isinstance(h, logging.StreamHandler)
        and not isinstance(h, logging.FileHandler)
        and not _ours(h)
        for h in root.handlers
    )


def setup_logging(
    component: str = "pipeline",
    level: str | None = None,
    console: bool = True,
    log_to_file: bool | None = None,
) -> logging.Logger:
    """
    Configure the root logger for `component`. Safe to call more than once: a
    second call replaces this module's handlers instead of stacking duplicates,
    and handlers installed by a host process (Airflow's task log handler,
    pytest's capture) are left alone. No console handler is added if one already
    exists, so lines are never printed twice.
    """

    global _default_component
    _default_component = component

    root = logging.getLogger()
    root.setLevel((level or os.environ.get("LOG_LEVEL") or "INFO").upper())

    for handler in [h for h in root.handlers if _ours(h)]:
        root.removeHandler(handler)
        handler.close()

    text_formatter = logging.Formatter(TEXT_FORMAT)

    if console and not _has_foreign_console(root):
        stream = logging.StreamHandler()
        json_console = os.environ.get("LOG_FORMAT", "text").lower() == "json"
        stream.setFormatter(JsonFormatter() if json_console else text_formatter)
        root.addHandler(_tag(stream))

    if log_to_file is None:
        log_to_file = env_flag("LOG_TO_FILE", True)

    if log_to_file:
        directory = log_dir()
        try:
            os.makedirs(directory, exist_ok=True)
            for suffix, formatter in ((".log", text_formatter), (".jsonl", JsonFormatter())):
                handler = logging.handlers.RotatingFileHandler(
                    os.path.join(directory, f"{component}{suffix}"),
                    maxBytes=MAX_BYTES,
                    backupCount=BACKUP_COUNT,
                    encoding="utf-8",
                    delay=True,
                )
                handler.setFormatter(formatter)
                root.addHandler(_tag(handler))
        except OSError as e:  # read-only file system etc. - keep console logging
            root.warning("File logging disabled, cannot write to %s: %s", directory, e)

    # Chatty third-party loggers: keep their warnings, drop their per-request info
    for noisy in ("kafka", "urllib3", "py4j", "databricks.sql", "thrift_backend"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return root
