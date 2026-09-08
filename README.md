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

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your Databricks + Kafka values
```

## Running tests

```bash
pip install -r requirements-dev.txt
pytest                                   # run the unit test suite
pytest --cov=transform --cov=validation --cov=extract --cov=kafka1 --cov=load --cov-report=term-missing
```

Tests live in [tests/](tests/):

- `test_transformer.py`, `test_validator.py` — pure unit tests of the
  transformation/validation logic (no I/O, no mocks needed).
- `test_extractor.py`, `test_producer.py`, `test_databricks_loader.py` —
  mock tests: HTTP calls, the Kafka producer, and the Databricks connection
  are all patched, so the suite never hits a real API, broker, or warehouse.
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

## Continuous Integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push/PR
to `main` and `develop` as six jobs, gated in this order:

```
Lint & Format ──┐
                ├──> Unit Tests (3.10, 3.12) ──┬──> Coverage Gate (>=70%) ──┐
Config Validation ──────────────────────────────┴──> Integration Tests ────┴──> CI Complete
```

| Job | What it checks |
|---|---|
| **Lint & Format** | `ruff check .` + `ruff format --check .` — see [ruff.toml](ruff.toml) for the (deliberately practical) rule scope. |
| **Config Validation** | [scripts/validate_config.py](scripts/validate_config.py) — `.env.example` covers every key `config.py` reads, `requirements-dev.txt` stays in sync with `requirements.txt`, `pytest.ini`'s `testpaths` exists, and the workflow YAML itself parses. |
| **Unit Tests** | `pytest -q` on Python 3.10 and 3.12 — the full suite in [tests/](tests/). |
| **Coverage Gate (>=70%)** | Re-runs the suite with `--cov-fail-under=70` across `transform`, `validation`, `extract`, `kafka1`, `load`. |
| **Integration Tests** | Runs [run_local.py](run_local.py) for real against the live FakeStore API (extract -> validate -> transform, no mocks) and asserts `data/output.csv` was written with rows in it — this is what actually distinguishes it from the mocked unit tests. |
| **CI Complete** | A single gate job depending on the rest — point branch protection at this one job instead of listing every job individually. |

A PR cannot merge while any of these are failing. Run the same checks
locally before pushing:

```bash
pip install -r requirements-dev.txt
ruff check .                      # Lint
ruff format --check .             # Format
python scripts/validate_config.py # Config Validation
pytest -q                         # Unit Tests
pytest -q --cov=transform --cov=validation --cov=extract --cov=kafka1 --cov=load --cov-fail-under=70
python run_local.py               # Integration Tests
```

## Outputs

- **Transformed dataset:** `data/output.csv` — generated by running
  `python run_local.py` (see [Local end-to-end run](#local-end-to-end-run-proves-the-pipeline-processes-real-data)
  above); gitignored since it's a run artifact, regenerated on every run.
  It's also what the Integration Tests CI job verifies gets produced.
- **Unit test results:** all tests passing — 79 tests via `pytest -q`, and
  the same core logic independently verified via
  `python -m unittest discover tests` (6 tests, no pytest dependency).
- **CI/CD:** GitHub Actions ([.github/workflows/ci.yml](.github/workflows/ci.yml))
  validates every commit and pull request against `main`/`develop` through
  the six jobs above — lint/format, config shape, unit tests (Python 3.10
  and 3.12), a >=70% coverage gate, and a live-API integration test.

## Git workflow

See [docs/GIT_WORKFLOW.md](docs/GIT_WORKFLOW.md) for the branching strategy
used on this project.
