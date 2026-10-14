"""
run_pipeline_no_airflow.py
---------------------------
Runs the exact same task sequence as the Airflow DAG (fakestore_pipeline.py):

    extract -> validate -> send_to_kafka -> replay_failed

by calling the identical instrumented steps the DAG calls (pipeline_tasks.py,
which wraps extract_products, validate_products, send_products and
replay_failed_records) directly, in order, instead of through Airflow's
scheduler/XCom.

This exists because Airflow can't run on native Windows (its CLI imports
os.register_at_fork, which doesn't exist there - confirmed by trying), and
this machine's WSL2 has no internet access to install Airflow there either
(a network policy blocks WSL2's adapter). This is a faithful stand-in for
demonstrating the pipeline end-to-end against a real running Kafka broker,
not a simulation - every function here is the real one the DAG would call.

Observability: logs go to the console and logs/pipeline.{log,jsonl}, tagged
with this run's run_id; metrics to logs/metrics.jsonl; a failed step sends a
Slack/email alert (see observability/alerts.py) and the script exits with 1,
so a scheduler (cron, Task Scheduler) sees the failure too.

    python run_pipeline_no_airflow.py
"""

import logging
import sys
import time

import pipeline_tasks
from observability import record, run_context, setup_logging
from observability.logging_config import new_run_id

logger = logging.getLogger(__name__)


def run(run_id: str | None = None) -> dict:
    run_id = run_id or new_run_id()
    start = time.perf_counter()
    success = False

    with run_context(run_id=run_id):
        logger.info("Pipeline run %s started", run_id)
        try:
            products = pipeline_tasks.extract()
            validated = pipeline_tasks.validate(products)
            sent = pipeline_tasks.send_to_kafka(validated)
            replayed = pipeline_tasks.replay_failed()
            success = True
        finally:
            duration = time.perf_counter() - start
            record("pipeline_run_duration_seconds", duration, {"pipeline": "fakestore"}, unit="s")
            record("pipeline_run_success", 1 if success else 0, {"pipeline": "fakestore"})

        logger.info("Pipeline run %s complete in %.1fs.", run_id, duration)

    return {
        "run_id": run_id,
        "extracted": len(products),
        "validated": len(validated),
        "sent": sent,
        "replayed": replayed,
    }


if __name__ == "__main__":
    setup_logging("pipeline")
    try:
        run()
    except Exception:
        # already logged + alerted by the failing step (observability.track_task)
        sys.exit(1)
