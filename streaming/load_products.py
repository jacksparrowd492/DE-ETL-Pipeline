"""
load_products.py
----------------
Load the FakeStore product catalog into the lakehouse as a small Delta table,
lakehouse.products, so user events (which carry a product_id) can be joined to
product names, categories and prices in Trino, Metabase and Streamlit.

    FakeStore API (or the offline fixture)
        -> transform_product()   same cleaning as the batch pipeline, so names and
                                 categories match silver_products in Databricks
        -> Delta table  <lakehouse>/warehouse/lakehouse.db/products  (overwrite)

Written with delta-rs (the `deltalake` package), so it needs no Spark and can run
while the streaming job holds the embedded Hive Metastore. Trino reads the folder
after serving/register_tables.sh registers it; re-run this script any time to
refresh the catalog (Trino sees the new version on the next query).

Usage
-----
    python streaming/load_products.py              # FakeStore API, fixture if offline
    python streaming/load_products.py --offline    # scripts/fixtures/fakestore_products.json only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from transform.transformer import transform_product  # noqa: E402

logger = logging.getLogger("load_products")

HERE = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT") or os.path.join(HERE, "_lakehouse")
PRODUCTS_PATH = f"{LAKEHOUSE_ROOT.replace(chr(92), '/')}/warehouse/lakehouse.db/products"
FIXTURE = os.path.join(ROOT, "scripts", "fixtures", "fakestore_products.json")

# Table columns, in order (governance/lineage.py checks its registry against this)
COLUMNS = (
    "product_id",
    "product_name",
    "product_category",
    "price",
    "price_category",
    "rating_rate",
    "rating_count",
    "rating_category",
    "image_url",
    "loaded_at",
)


def fetch_products(offline: bool = False) -> tuple[list[dict], str]:
    """Raw products and where they came from; falls back to the fixture when the API fails."""

    if not offline:
        try:
            from extract.extractor import extract_products

            return extract_products(), "FakeStore API"
        except Exception as e:  # noqa: BLE001 - any failure -> offline fixture
            logger.warning("FakeStore API unavailable (%s), using the fixture", e)

    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f), "fixture"


def to_rows(raw_products: list[dict]) -> list[dict]:
    """One row per product, cleaned like the batch pipeline; last record wins per id."""

    rows = {}
    for raw in raw_products:
        p = transform_product(raw)
        rows[p["product_id"]] = {
            "product_id": p["product_id"],
            "product_name": p["product_name"],
            "product_category": p["product_category"],
            "price": p["price"],
            "price_category": p["price_category"],
            "rating_rate": p["rating_rate"],
            "rating_count": p["rating_count"],
            "rating_category": p["rating_category"],
            "image_url": p["image_url"],
            "loaded_at": p["etl_load_timestamp"],
        }
    return [rows[k] for k in sorted(rows)]


def write_products(rows: list[dict], path: str = PRODUCTS_PATH) -> None:
    """Overwrite the Delta table at `path` with `rows` (creates it on first run)."""

    import pyarrow as pa
    from deltalake import write_deltalake

    schema = pa.schema(
        [
            ("product_id", pa.int32()),
            ("product_name", pa.string()),
            ("product_category", pa.string()),
            ("price", pa.float64()),
            ("price_category", pa.string()),
            ("rating_rate", pa.float64()),
            ("rating_count", pa.int32()),
            ("rating_category", pa.string()),
            ("image_url", pa.string()),
            ("loaded_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    assert tuple(schema.names) == COLUMNS
    table = pa.Table.from_pylist(rows, schema=schema)

    if "://" not in path:
        os.makedirs(path, exist_ok=True)
    try:
        write_deltalake(path, table, mode="overwrite", schema_mode="overwrite")
    except TypeError:  # deltalake < 0.15 spells it overwrite_schema
        write_deltalake(path, table, mode="overwrite", overwrite_schema=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="FakeStore products -> lakehouse.products (Delta)")
    parser.add_argument("--offline", action="store_true", help="Use the fixture, skip the API")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    raw, source = fetch_products(args.offline)
    rows = to_rows(raw)
    write_products(rows)
    print(f"Wrote {len(rows)} products from the {source} to {PRODUCTS_PATH}")
    print("Next: bash serving/register_tables.sh  (registers lakehouse.products in Trino)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
