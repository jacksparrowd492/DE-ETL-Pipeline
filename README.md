# FakeStore ETL Pipeline

An ETL pipeline that extracts product data from the [FakeStore API](https://fakestoreapi.com/),
validates it, transforms it into the bronze/silver row shape used by
`fakestore_catalog.etl_project`, streams it through Kafka, and loads it into
Databricks (with a local replay path for records that fail to load).

```
extract/  -> validation/  -> kafka1/ (producer/consumer)  -> load/ (Databricks) -> staging/ (warehouse promotion)
                                                                       \-> failed_records/ -> replay/
```

Orchestrated as an Airflow DAG in [fakestore_pipeline.py](fakestore_pipeline.py).

## Phase II: deployment, monitoring and governance

| Requirement | Where | Details |
|---|---|---|
| Logging integrated into the pipeline | [observability/logging_config.py](observability/logging_config.py) | console + rotating `logs/<component>.log` / `.jsonl`, every line tagged with the run id and task |
| Email / Slack alerts for task failures | [observability/alerts.py](observability/alerts.py), DAG callbacks | Slack webhook, SMTP email, Amazon SNS, always `logs/alerts.jsonl`; Airflow `on_failure_callback` / `on_retry_callback` |
| Metrics, latency and failure alerts | [observability/metrics.py](observability/metrics.py), [observability/stream_health.py](observability/stream_health.py) | task duration vs SLA, Kafka → Databricks latency, stream p95 latency / reject ratio / trigger time; JSON-lines + Prometheus |
| Data lineage of the final tables | [governance/lineage.py](governance/lineage.py) → [docs/LINEAGE.md](docs/LINEAGE.md) | table + column lineage for bronze/silver/dim/fact/gold, the Delta tables and Trino views; OpenLineage events per run; CI-checked against the code |
| Cloud deployment | [deploy/](deploy/README.md), [.github/workflows/deploy.yml](.github/workflows/deploy.yml) | AWS: MSK, EMR Serverless, S3 + Glue, Athena (Trino), MWAA, CloudWatch alarms → SNS; Terraform, no Docker |
| Monitoring UI | Streamlit **Monitoring** page | alerts, task durations vs SLA, latency, lineage graph per table |

Guides: [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) (what is logged, alerted and measured,
and how to configure Slack/email) and [deploy/README.md](deploy/README.md) (deployment strategy).
Both orchestrators run the same instrumented steps from [pipeline_tasks.py](pipeline_tasks.py).

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your Databricks + Kafka values
                       # optional: SLACK_WEBHOOK_URL / SMTP_* for alerts (docs/OBSERVABILITY.md)
```

## Running tests

```bash
pip install -r requirements-dev.txt
pytest                                   # run the unit test suite
pytest --cov=transform --cov=validation --cov=extract --cov=kafka1 --cov=load --cov=maintenance        --cov=observability --cov=governance --cov=pipeline_tasks --cov-report=term-missing
```

Tests live in [tests/](tests/):

- `test_transformer.py`, `test_validator.py` — pure unit tests of the
  transformation/validation logic (no I/O, no mocks needed).
- `test_extractor.py`, `test_producer.py`, `test_databricks_loader.py` —
  mock tests: HTTP calls, the Kafka producer, and the Databricks connection
  are all patched, so the suite never hits a real API, broker, or warehouse.
- `test_lakehouse_ops.py`, `test_benchmark.py` — Week 4 table maintenance:
  compaction, snapshot expiry and vacuum run against tiny real Delta tables
  in a temp directory; the Trino benchmark runs against a fake cursor.
- `test_observability.py`, `test_lineage.py`, `test_pipeline_tasks.py`,
  `test_replay.py` — Phase II: logging with run context, Slack/email/SNS alerts
  (HTTP/SMTP patched), alert dedup, metrics files, task SLA and failure alerts,
  stream health checks, the lineage registry against the code, OpenLineage
  events, and the instrumented pipeline steps end to end. A conftest fixture
  points `LOG_DIR` at a temp folder, so tests never write to `logs/`.
- `test_maintenance_dags.py` — DAG integrity checks for the maintenance DAGs.
  Skipped where Airflow cannot be imported (native Windows, CI); run it in
  the WSL Airflow venv (see [maintenance/README.md](maintenance/README.md)).
- `test_unittest_suite.py` — the same transformation/validation logic,
  exercised with a plain `unittest.TestCase` suite (no pytest fixtures), to
  prove the tests aren't tied to the pytest runner:

  ```bash
  python -m unittest discover tests
  ```

## Local end-to-end run (proves the pipeline processes real data)

`fakestore_pipeline.py` is the production DAG — it streams validated
products through Kafka into Databricks, so running it needs those
credentials configured. To see the extract -> validate -> transform logic
process real data end-to-end without any of that infrastructure:

```bash
python run_local.py
```

This calls the live FakeStore API, validates and transforms every product,
and writes the result to `data/output.csv` (gitignored — it's a generated
run artifact, not source, same as `logs/` and `failed_records/`).

## Running the full stack locally (Kafka + Airflow + Databricks + Streamlit)

This needs Kafka running, and either Airflow or a stand-in for it, before
the Databricks-backed pieces (consumer, Streamlit) have anything to read.
Notes below are from actually getting this working on Windows + WSL2 —
keep them in mind if you're setting this up on a similar machine.

**Airflow cannot run on native Windows** — its CLI imports
`os.register_at_fork`, which doesn't exist there, so it crashes immediately
on import. The supported path is WSL2. If your WSL2 has internet access,
set up a venv there, `pip install apache-airflow` (with the [constraints
file](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)
for your Airflow + Python version) and the project's `requirements.txt`,
point `AIRFLOW__CORE__DAGS_FOLDER` at this project directory (reachable
from WSL2 at `/mnt/f/...`), and run `airflow standalone`. If WSL2 has no
internet (common on locked-down/corporate networks — the standard
mirrored-networking fix in `.wslconfig` may not help if a network policy
blocks it outright), use [run_pipeline_no_airflow.py](run_pipeline_no_airflow.py)
instead — it calls the exact same functions the DAG calls
(`extract_products`, `validate_products`, `send_products`,
`replay_failed_records`), in the same order, just without Airflow's
scheduler/XCom.

1. **Kafka** (in WSL2, e.g. `wsl -d Ubuntu`) — this project was tested
   against a Kafka install in KRaft mode (no Zookeeper needed):

   ```bash
   cd /mnt/f/Kafka/kafka_2.13-3.9.2   # or wherever your Kafka install is
   bin/kafka-server-start.sh config/kraft/server.properties
   # first time only, if storage isn't formatted yet:
   #   bin/kafka-storage.sh format -t <uuid> -c config/kraft/server.properties
   bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic validated_products --if-not-exists
   ```

   **WSL2 loopback quirk:** on some machines WSL2 only forwards its
   **IPv6** loopback (`::1`) to Windows, not IPv4 (`127.0.0.1`) — `localhost`
   resolves to both, and kafka-python can hang for 30s per attempt if it
   picks the one that isn't forwarded. If native Windows can't reach Kafka
   at `localhost:9092`, test both explicitly (`127.0.0.1:9092` vs
   `[::1]:9092`) and, if only IPv6 works, set both
   `advertised.listeners=PLAINTEXT://[::1]:9092` in
   `config/kraft/server.properties` and `KAFKA_BOOTSTRAP_SERVER=[::1]:9092`
   in `.env` (bootstrapping can otherwise succeed via one address while the
   post-bootstrap produce connection fails via the advertised one).

2. **Airflow DAG, or the fallback** (native Windows, once Kafka is up):

   ```bash
   python run_pipeline_no_airflow.py
   ```

   Runs extract -> validate -> send_to_kafka -> replay_failed against the
   real broker.

3. **Kafka consumer** (native Windows) — transforms each message and loads
   it into Databricks (bronze/silver, then dim_category/fact_products/
   gold_product_summary):

   ```bash
   python -m kafka1.consumer
   ```

   Run as a module (`-m`), not `python kafka1/consumer.py` — the latter
   doesn't add the project root to `sys.path`, so the sibling `transform`/
   `load`/`staging` package imports fail. This is a long-running consumer
   (`Ctrl+C` to stop once it's caught up); if Databricks returns
   `BAD_REQUEST: Cannot create the resource, please try again later`, your
   SQL warehouse is stopped — start it in the Databricks workspace UI and
   retry.

4. **Streamlit dashboard** (native Windows), reads live from the same
   Databricks tables:

   ```bash
   streamlit run streamlit_app.py
   ```

## Streaming lakehouse (Weeks 2–4)

A second, Databricks-free path: Kafka events -> Spark Structured Streaming ->
local Delta Lake tables -> Trino -> Metabase. Runs natively in WSL, no Docker.

| Folder | Week | What it does |
|---|---|---|
| [streaming/](streaming/README.md) | 2 | Spark streams Kafka events into the Delta tables `user_events` / `user_events_rejected` |
| [serving/](serving/README.md) | 3 | Trino 483 over the Delta tables, Metabase dashboard |
| [maintenance/](maintenance/README.md) | 4 | Airflow DAGs for compaction (bin-packing), snapshot expiry and vacuum; Trino benchmark before vs after |

## Continuous Integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push/PR
to `main` and `develop` as seven jobs, gated in this order:

```
Lint & Format ──┬──> Unit Tests (3.10, 3.12) ──┬──> Coverage Gate (>=70%) ──┐
                └──> Deploy Config ────────────┼────────────────────────────┤
Config Validation ─────────────────────────────┴──> Integration Tests ──────┴──> CI Complete
```

| Job | What it checks |
|---|---|
| **Lint & Format** | `ruff check .` + `ruff format --check .` — see [ruff.toml](ruff.toml) for the (deliberately practical) rule scope. |
| **Config Validation** | [scripts/validate_config.py](scripts/validate_config.py) — `.env.example` covers every key the code reads (`config.py`, `observability/`, `governance/`, the consumer and pipeline steps), `requirements-dev.txt` stays in sync with `requirements.txt`, `pytest.ini`'s `testpaths` exists, and the workflow YAML files parse. Then `python -m governance.lineage validate` (the lineage registry matches the loaders' columns and DDL) and `docs --check` (docs/LINEAGE.md is regenerated). |
| **Unit Tests** | `pytest -q` on Python 3.10 and 3.12 — the full suite in [tests/](tests/). |
| **Coverage Gate (>=70%)** | Re-runs the suite with `--cov-fail-under=70` across `transform`, `validation`, `extract`, `kafka1`, `load`, `maintenance`, `observability`, `governance`, `pipeline_tasks`. |
| **Deploy Config** | `terraform fmt -check` + `terraform validate` on [deploy/terraform/aws](deploy/terraform/aws), the deploy shell scripts parse, and `deploy/package.py` builds the artifacts. |
| **Integration Tests** | Runs [run_local.py](run_local.py) for real against the live FakeStore API (extract -> validate -> transform, no mocks) and asserts `data/output.csv` was written with rows in it — this is what actually distinguishes it from the mocked unit tests. |
| **CI Complete** | A single gate job depending on the rest — point branch protection at this one job instead of listing every job individually. |

A PR cannot merge while any of these are failing. Run the same checks
locally before pushing:

```bash
pip install -r requirements-dev.txt
ruff check .                      # Lint
ruff format --check .             # Format
python scripts/validate_config.py # Config Validation
python -m governance.lineage validate && python -m governance.lineage docs --check
pytest -q                         # Unit Tests
pytest -q --cov=transform --cov=validation --cov=extract --cov=kafka1 --cov=load --cov=maintenance \
  --cov=observability --cov=governance --cov=pipeline_tasks --cov-fail-under=70
python run_local.py               # Integration Tests
python deploy/package.py --no-deps  # Deploy Config (plus terraform fmt/validate if installed)
```

Deployment is a separate, manual workflow:
[.github/workflows/deploy.yml](.github/workflows/deploy.yml) (**Deploy (AWS)**) — see
[deploy/README.md](deploy/README.md).

## Outputs

- **Transformed dataset:** `data/output.csv` — generated by running
  `python run_local.py` (see [Local end-to-end run](#local-end-to-end-run-proves-the-pipeline-processes-real-data)
  above); gitignored since it's a run artifact, regenerated on every run.
  It's also what the Integration Tests CI job verifies gets produced.
- **Unit test results:** all tests passing — 176 tests via `pytest -q`, 92 %
  coverage (plus the Airflow-only DAG tests, which are skipped outside the WSL
  venv), and the same core logic independently verified via
  `python -m unittest discover tests` (6 tests, no pytest dependency).
- **Observability output:** `logs/<component>.{log,jsonl}`, `logs/alerts.jsonl`,
  `logs/metrics.jsonl`, `logs/metrics_<component>.prom` and
  `logs/lineage/openlineage.jsonl` (gitignored run artifacts), shown on the
  Streamlit **Monitoring** page.
- **Data lineage:** [docs/LINEAGE.md](docs/LINEAGE.md), generated from
  [governance/lineage.py](governance/lineage.py).
- **CI/CD:** GitHub Actions ([.github/workflows/ci.yml](.github/workflows/ci.yml))
  validates every commit and pull request against `main`/`develop` through
  the seven jobs above — lint/format, config + lineage checks, unit tests
  (Python 3.10 and 3.12), a >=70% coverage gate, an integration test and the
  deploy config; [deploy.yml](.github/workflows/deploy.yml) deploys to AWS.

## Git workflow

See [docs/GIT_WORKFLOW.md](docs/GIT_WORKFLOW.md) for the branching strategy
used on this project.
