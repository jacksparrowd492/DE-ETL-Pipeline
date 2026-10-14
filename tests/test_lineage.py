"""
test_lineage.py
---------------
Tests for governance/lineage.py: the registry matches the code (column lists of
every final table), every column has lineage, the graph queries, the generated
docs are current, and the OpenLineage events are well-formed and reach the file
and the HTTP backend.
"""

import json
import uuid

import pytest

from governance import lineage


def _events(log_dir):
    path = log_dir / "lineage" / "openlineage.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_registry_matches_the_code():
    assert lineage.validate() == []


def test_validate_catches_a_column_without_lineage(monkeypatch):
    cols = dict(lineage.COLUMN_LINEAGE["dbx.fact_products"])
    del cols["rating_category"]
    monkeypatch.setitem(lineage.COLUMN_LINEAGE, "dbx.fact_products", cols)

    errors = lineage.validate()

    assert any(
        "dbx.fact_products: columns without lineage: ['rating_category']" in e for e in errors
    )


def test_validate_catches_registry_drift_from_the_loader(monkeypatch):
    import load.databricks_loader as loader

    monkeypatch.setattr(loader, "BRONZE_COLUMNS", loader.BRONZE_COLUMNS + ("new_col",))

    assert any("dbx.bronze_products: registry columns" in e for e in lineage.validate())


def test_validate_catches_a_source_that_is_not_a_job_input(monkeypatch):
    cols = dict(lineage.COLUMN_LINEAGE["dbx.gold_product_summary"])
    cols["average_price"] = lineage.Col((("api.products", "price"),), "AVG")
    monkeypatch.setitem(lineage.COLUMN_LINEAGE, "dbx.gold_product_summary", cols)

    assert any("api.products is not an input" in e for e in lineage.validate())


def test_ddl_parser_reads_column_names():
    src = 'CREATE TABLE IF NOT EXISTS {T} (\n    a INT,\n    b_c STRING COMMENT "x"\n)\nUSING DELTA'
    assert lineage._ddl_columns(src, "{T} (") == ["a", "b_c"]


def test_gold_traces_back_to_the_api():
    up = lineage.upstream("dbx.gold_product_summary")
    assert up[0] == "kafka.validated_products"
    assert "dbx.silver_products" in up
    assert up[-1] == "api.products"


def test_streaming_events_reach_every_dashboard_on_trino():
    down = lineage.downstream("kafka.user_events")
    assert {"delta.user_events", "trino.v_events_by_type", "dash.metabase", "dash.powerbi"} <= set(
        down
    )
    # Streamlit reads user events only through the product activity view
    assert {"trino.v_product_activity", "dash.streamlit"} <= set(down)
    assert "trino.products" not in down


def test_product_activity_joins_events_to_the_product_catalog():
    up = lineage.upstream("trino.v_product_activity")
    assert {"trino.user_events", "kafka.user_events", "trino.products", "api.products"} <= set(up)
    name = lineage.COLUMN_LINEAGE["trino.v_product_activity"]["product_name"]
    assert ("trino.products", "product_name") in name.sources
    assert lineage.COLUMN_LINEAGE["delta.user_events"]["product_id"].sources == (
        ("kafka.user_events", "product_id"),
    )


def test_maintenance_jobs_do_not_create_false_lineage():
    # compaction reads and writes user_events: user_events must not become its own ancestor
    assert "delta.user_events" not in lineage.upstream("delta.user_events")


def test_generated_docs_are_up_to_date():
    with open(lineage.DOCS_FILE, encoding="utf-8") as f:
        assert f.read().replace("\r\n", "\n") == lineage.render_docs()


def test_docs_do_not_depend_on_the_local_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dbc-secret.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_CATALOG", "other_catalog")
    text = lineage.render_docs()
    assert "dbc-secret" not in text
    assert "other_catalog" not in text


def test_dataset_names_resolve_from_the_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CATALOG", "prod")
    monkeypatch.setenv("DATABRICKS_SCHEMA", "sales")
    assert lineage.DATASETS["dbx.fact_products"].resolved_name() == "prod.sales.fact_products"
    assert lineage.DATASETS["dbx.fact_products"].resolved_name(use_env=False) == (
        "fakestore_catalog.etl_project.fact_products"
    )


def test_mermaid_and_dot_cover_every_job():
    text, graph = lineage.mermaid(include_maintenance=True), lineage.dot(include_maintenance=True)
    for job in lineage.JOBS:
        assert job in text
        assert f'"job:{job}"' in graph


def test_complete_event_has_schema_and_column_lineage():
    event = lineage.run_event("kafka_consumer.promote_to_warehouse", "COMPLETE", run_id="r1")

    assert event["eventType"] == "COMPLETE"
    assert event["job"] == {
        "namespace": lineage.JOB_NAMESPACE,
        "name": "kafka_consumer.promote_to_warehouse",
        "facets": event["job"]["facets"],
    }
    uuid.UUID(event["run"]["runId"])  # a valid UUID
    fact = next(o for o in event["outputs"] if o["name"].endswith("fact_products"))
    assert [f["name"] for f in fact["facets"]["schema"]["fields"]][:2] == [
        "product_id",
        "category_id",
    ]
    category = fact["facets"]["columnLineage"]["fields"]["category_id"]["inputFields"]
    assert {(i["name"].split(".")[-1], i["field"]) for i in category} == {
        ("dim_category", "category_id"),
        ("validated_products", "category"),
    }


def test_start_event_has_no_column_lineage():
    event = lineage.run_event("fakestore_pipeline.extract", "START", run_id="r1")
    assert all("columnLineage" not in o["facets"] for o in event["outputs"])


def test_run_id_is_stable_per_job_and_run():
    a = lineage.run_event("fakestore_pipeline.extract", "START", run_id="r1")["run"]["runId"]
    b = lineage.run_event("fakestore_pipeline.extract", "COMPLETE", run_id="r1")["run"]["runId"]
    c = lineage.run_event("fakestore_pipeline.validate", "START", run_id="r1")["run"]["runId"]
    assert a == b != c


def test_lineage_run_emits_start_and_complete(isolated_observability):
    with lineage.lineage_run("fakestore_pipeline.validate", run_id="r2"):
        pass

    events = _events(isolated_observability)
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    assert events[0]["run"]["runId"] == events[1]["run"]["runId"]


def test_lineage_run_emits_fail_with_the_error(isolated_observability):
    with pytest.raises(ValueError):
        with lineage.lineage_run("fakestore_pipeline.extract", run_id="r3"):
            raise ValueError("api down")

    fail = _events(isolated_observability)[-1]
    assert fail["eventType"] == "FAIL"
    assert fail["run"]["facets"]["errorMessage"]["message"] == "ValueError: api down"


def test_emit_posts_to_the_openlineage_backend(monkeypatch, mocker):
    monkeypatch.setenv("OPENLINEAGE_URL", "http://marquez:5000/")
    monkeypatch.setenv("OPENLINEAGE_API_KEY", "k")
    post = mocker.patch("governance.lineage.requests.post")

    assert lineage.emit(lineage.run_event("serving.views", "COMPLETE", run_id="r")) is True
    assert post.call_args.args[0] == "http://marquez:5000/api/v1/lineage"
    assert post.call_args.kwargs["headers"]["Authorization"] == "Bearer k"


def test_cli_validate_and_docs_check(capsys):
    assert lineage.main(["validate"]) == 0
    assert lineage.main(["docs", "--check"]) == 0
    assert "up to date" in capsys.readouterr().out
