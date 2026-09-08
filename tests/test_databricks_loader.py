"""
test_databricks_loader.py
--------------------------
Mock tests for load/databricks_loader.py. get_connection() is patched so no
test ever opens a real Databricks connection.
"""

import json

import pytest

import load.databricks_loader as databricks_loader
from transform.transformer import transform_product


@pytest.fixture
def transformed_product(raw_product):
    return transform_product(raw_product)


def _fake_conn_and_cursor(mocker):
    fake_cursor = mocker.Mock()
    fake_conn = mocker.Mock()
    fake_conn.cursor.return_value = fake_cursor
    return fake_conn, fake_cursor


# --------------------------------------------------------
# load_to_staging - happy path
# --------------------------------------------------------


def test_load_to_staging_inserts_into_bronze_and_silver(mocker, transformed_product):
    fake_conn, fake_cursor = _fake_conn_and_cursor(mocker)
    mocker.patch("load.databricks_loader.get_connection", return_value=fake_conn)

    databricks_loader.load_to_staging(transformed_product)

    assert fake_cursor.execute.call_count == 2  # bronze insert + silver insert
    fake_conn.commit.assert_called_once()
    fake_cursor.close.assert_called_once()
    fake_conn.close.assert_called_once()


def test_load_to_staging_bronze_insert_matches_column_count(mocker, transformed_product):
    fake_conn, fake_cursor = _fake_conn_and_cursor(mocker)
    mocker.patch("load.databricks_loader.get_connection", return_value=fake_conn)

    databricks_loader.load_to_staging(transformed_product)

    bronze_call = fake_cursor.execute.call_args_list[0]
    _, bound_values = bronze_call.args
    assert len(bound_values) == len(databricks_loader.BRONZE_COLUMNS)


# --------------------------------------------------------
# load_to_staging - failure path
# --------------------------------------------------------


def test_load_to_staging_saves_failed_record_and_reraises(mocker, transformed_product, tmp_path):
    fake_conn, fake_cursor = _fake_conn_and_cursor(mocker)
    fake_cursor.execute.side_effect = RuntimeError("insert failed")
    mocker.patch("load.databricks_loader.get_connection", return_value=fake_conn)

    failed_file = tmp_path / "failed_products.json"
    mocker.patch("load.databricks_loader.FAILED_DIRECTORY", str(tmp_path))
    mocker.patch("load.databricks_loader.FAILED_FILE", str(failed_file))

    with pytest.raises(RuntimeError):
        databricks_loader.load_to_staging(transformed_product)

    assert failed_file.exists()
    saved = json.loads(failed_file.read_text())
    assert saved[0]["product_id"] == transformed_product["product_id"]

    # Connection cleanup still happens even though the insert failed.
    fake_cursor.close.assert_called_once()
    fake_conn.close.assert_called_once()


# --------------------------------------------------------
# save_failed_record
# --------------------------------------------------------


def test_save_failed_record_appends_to_existing_file(mocker, transformed_product, tmp_path):
    failed_file = tmp_path / "failed_products.json"
    failed_file.write_text(json.dumps([{"product_id": 999}]))

    mocker.patch("load.databricks_loader.FAILED_DIRECTORY", str(tmp_path))
    mocker.patch("load.databricks_loader.FAILED_FILE", str(failed_file))

    databricks_loader.save_failed_record(transformed_product)

    saved = json.loads(failed_file.read_text())
    assert len(saved) == 2
    assert saved[0]["product_id"] == 999
    assert saved[1]["product_id"] == transformed_product["product_id"]
