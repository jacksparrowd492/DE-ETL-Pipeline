"""
test_product_tracking.py
------------------------
Product tracking in the streaming lakehouse:
  - streaming/producer.py      shopping events carry a product_id, carts stay consistent
  - streaming/load_products.py FakeStore catalog -> Delta table lakehouse.products
  - serving/product_activity.py per-product funnel for the Streamlit Products page
No Kafka, Spark or Trino needed.
"""

import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from serving import product_activity

ROOT = Path(__file__).resolve().parents[1]


def _load(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def producer():
    module = _load("streaming/producer.py", "streaming_producer")
    module._carts.clear()
    return module


@pytest.fixture
def load_products():
    return _load("streaming/load_products.py", "load_products")


# --------------------------------------------------------
# streaming/producer.py
# --------------------------------------------------------


def test_shopping_events_carry_a_known_product_id(producer):
    events = [producer.valid_event() for _ in range(500)]

    for e in events:
        if e["event"] in producer.PRODUCT_EVENTS:
            assert e["product_id"] in producer.PRODUCT_IDS
        else:
            assert "product_id" not in e  # login / logout
    assert {e["event"] for e in events} == set(producer.EVENTS)


def test_remove_and_purchase_take_a_product_from_the_users_cart(producer):
    added = producer.pick_product("user_001", "add_to_cart")
    assert producer._carts["user_001"] == [added]

    assert producer.pick_product("user_001", "purchase") == added
    assert producer._carts["user_001"] == []
    # empty cart: still a valid product ("buy now")
    assert producer.pick_product("user_001", "remove_from_cart") in producer.PRODUCT_IDS


def test_product_popularity_is_skewed_and_stable(producer):
    again = _load("streaming/producer.py", "streaming_producer_2")
    assert producer.PRODUCT_WEIGHTS == again.PRODUCT_WEIGHTS
    assert max(producer.PRODUCT_WEIGHTS) > 5 * min(producer.PRODUCT_WEIGHTS)


# --------------------------------------------------------
# streaming/load_products.py
# --------------------------------------------------------


def test_to_rows_cleans_like_the_batch_pipeline(load_products, raw_product):
    duplicate = {**raw_product, "title": "  Newer title  "}

    rows = load_products.to_rows([raw_product, duplicate])

    assert len(rows) == 1  # last record wins per id
    row = rows[0]
    assert tuple(row) == load_products.COLUMNS
    assert row["product_name"] == "Newer title"
    assert row["product_id"] == int(raw_product["id"])


def test_write_products_creates_a_delta_table(load_products, tmp_path):
    from deltalake import DeltaTable

    raw, source = load_products.fetch_products(offline=True)
    rows = load_products.to_rows(raw)
    path = str(tmp_path / "products")

    load_products.write_products(rows, path)
    load_products.write_products(rows[:3], path)  # re-run overwrites

    table = DeltaTable(path)
    df = table.to_pandas()
    assert source == "fixture"
    assert list(df.columns) == list(load_products.COLUMNS)
    assert sorted(df["product_id"]) == [r["product_id"] for r in rows[:3]]
    assert table.version() == 1


# --------------------------------------------------------
# serving/product_activity.py
# --------------------------------------------------------


def _activity(rows):
    return pd.DataFrame(rows, columns=product_activity.ACTIVITY_COLUMNS)


def test_product_funnel_counts_converts_and_ranks():
    d1, d2 = date(2026, 10, 13), date(2026, 10, 14)
    activity = _activity(
        [
            (1, "Backpack", "Men's Clothing", 100.0, "page_view", d1, 10, 4),
            (1, "Backpack", "Men's Clothing", 100.0, "add_to_cart", d1, 4, 3),
            (1, "Backpack", "Men's Clothing", 100.0, "purchase", d2, 3, 3),
            (2, "Ring", "Jewelry", 50.0, "page_view", d1, 7, 2),
            (2, "Ring", "Jewelry", 50.0, "page_view", d2, 5, 2),
            (2, "Ring", "Jewelry", 50.0, "purchase", d2, 1, 1),
            (9, None, None, None, "click", d2, 2, 1),  # not in lakehouse.products
        ]
    )

    funnel = product_activity.product_funnel(activity)

    assert list(funnel.columns) == product_activity.FUNNEL_COLUMNS
    assert list(funnel["product_id"]) == [1, 2, 9]  # most purchased first
    backpack = funnel.iloc[0]
    assert (backpack["page_view"], backpack["add_to_cart"], backpack["purchase"]) == (10, 4, 3)
    assert backpack["cart_to_purchase_pct"] == 75
    assert backpack["revenue"] == 300.0
    ring = funnel.iloc[1]
    assert ring["page_view"] == 12  # summed over days
    assert pd.isna(ring["cart_to_purchase_pct"])  # no add-to-cart -> no rate
    unknown = funnel.iloc[2]
    assert (unknown["product_name"], unknown["product_category"]) == ("Product 9", "Unknown")


def test_product_funnel_prefers_the_dashboards_catalog():
    activity = _activity([(1, "Old name", "Old cat", 10.0, "purchase", date(2026, 10, 14), 2, 1)])
    silver = pd.DataFrame(
        {
            "product_id": [1, 5],
            "product_name": ["New name", "Other"],
            "product_category": ["Electronics", "Jewelry"],
            "price": [20.0, 1.0],
        }
    )

    row = product_activity.product_funnel(activity, silver).iloc[0]

    assert (row["product_name"], row["product_category"], row["revenue"]) == (
        "New name",
        "Electronics",
        40.0,
    )


def test_product_funnel_of_no_activity_is_empty():
    funnel = product_activity.product_funnel(_activity([]))
    assert funnel.empty
    assert list(funnel.columns) == product_activity.FUNNEL_COLUMNS


def test_load_activity_queries_the_view(mocker):
    cursor = mocker.Mock()
    cursor.fetchall.return_value = [
        [3, "Ring", "Jewelry", "9.99", "purchase", "2026-10-14", 2, 1],
    ]
    conn = mocker.Mock()
    conn.cursor.return_value = cursor
    connect = mocker.patch("trino.dbapi.connect", return_value=conn)

    df = product_activity.load_activity("localhost", 8080, "streamlit")

    connect.assert_called_once_with(
        host="localhost", port=8080, user="streamlit", catalog="delta", schema="lakehouse"
    )
    assert product_activity.VIEW in cursor.execute.call_args.args[0]
    assert df.loc[0, "price"] == 9.99
    assert df.loc[0, "event_date"] == date(2026, 10, 14)
    conn.close.assert_called_once()
