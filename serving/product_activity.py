"""
product_activity.py
-------------------
Per-product user activity for the Streamlit Product Activity page.

    Kafka user-events (product_id) -> Delta lakehouse.user_events
        -> Trino view delta.lakehouse.v_product_activity   (serving/views.sql)
        -> load_activity()     one row per product x event type x day
        -> product_funnel()    one row per product: funnel counts, conversion, revenue

Kept out of streamlit_app.py so the numbers can be unit-tested without
Streamlit, Trino or Databricks.
"""

from __future__ import annotations

import pandas as pd

VIEW = "delta.lakehouse.v_product_activity"
FUNNEL_EVENTS = ["page_view", "click", "add_to_cart", "remove_from_cart", "purchase"]
ACTIVITY_COLUMNS = [
    "product_id",
    "product_name",
    "product_category",
    "price",
    "event",
    "event_date",
    "events",
    "users",
]
FUNNEL_COLUMNS = [
    "product_id",
    "product_name",
    "product_category",
    "price",
    *FUNNEL_EVENTS,
    "cart_to_purchase_pct",
    "revenue",
]


def load_activity(host: str, port: int, user: str) -> pd.DataFrame:
    """Read the whole view (small: products x 5 event types x days) from Trino."""

    import trino  # imported here: only this page needs the Trino client

    conn = trino.dbapi.connect(host=host, port=port, user=user, catalog="delta", schema="lakehouse")
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {', '.join(ACTIVITY_COLUMNS)} FROM {VIEW}")
        rows = cur.fetchall()
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=ACTIVITY_COLUMNS)
    df["product_id"] = pd.to_numeric(df["product_id"]).astype("int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["events"] = pd.to_numeric(df["events"]).astype("int64")
    df["users"] = pd.to_numeric(df["users"]).astype("int64")
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    return df


def product_funnel(activity: pd.DataFrame, products: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    One row per product with events per funnel step, cart-to-purchase % and
    revenue (purchases x price), most purchased first.

    Name, category and price come from `products` (silver_products, the
    dashboard's catalog) when it has the product, else from the view
    (lakehouse.products); unknown products become "Product <id>".
    """

    if activity.empty:
        return pd.DataFrame(columns=FUNNEL_COLUMNS)

    counts = (
        activity.pivot_table(
            index="product_id", columns="event", values="events", aggfunc="sum", fill_value=0
        )
        .reindex(columns=FUNNEL_EVENTS, fill_value=0)
        .astype("int64")
    )
    counts.columns.name = None

    info = activity.groupby("product_id")[["product_name", "product_category", "price"]].first()
    if products is not None and not products.empty:
        catalog = (
            products.drop_duplicates("product_id")
            .set_index("product_id")[["product_name", "product_category", "price"]]
            .reindex(info.index)
        )
        info = catalog.combine_first(info)

    out = info.join(counts).reset_index()
    out["product_name"] = out["product_name"].fillna("Product " + out["product_id"].astype(str))
    out["product_category"] = out["product_category"].fillna("Unknown")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    carts = out["add_to_cart"].where(out["add_to_cart"] > 0)
    out["cart_to_purchase_pct"] = (100 * out["purchase"] / carts).round(0)
    out["revenue"] = (out["purchase"] * out["price"]).round(2)
    return (
        out[FUNNEL_COLUMNS]
        .sort_values(["purchase", "page_view", "product_id"], ascending=[False, False, True])
        .reset_index(drop=True)
    )
