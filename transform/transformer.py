"""
transformer.py
--------------
Transforms raw FakeStore products into the bronze/silver row shape used by
fakestore_catalog.etl_project, applying the same price/rating bucketing and
category naming already used by the tables in that schema (inferred from the
live data: price/value buckets split at $50 / $100, rating buckets split at
3.0 / 4.0 / 4.5, categories normalized to Title Case with "jewelery" -> "Jewelry").
"""

from datetime import datetime, timezone

# --------------------------------------------------------
# Category normalization
# --------------------------------------------------------

# Matches dim_category exactly as already loaded in Databricks.
CATEGORY_DISPLAY_MAP = {
    "electronics": "Electronics",
    "jewelery": "Jewelry",
    "jewelry": "Jewelry",
    "men's clothing": "Men's Clothing",
    "women's clothing": "Women's Clothing",
}


def normalize_category(raw_category: str) -> str:
    """
    Maps a raw category string to its display name in dim_category.
    Known FakeStore categories map exactly (including the "jewelery" -> "Jewelry"
    rename already baked into the catalog). Unknown categories fall back to a
    word-capitalized form that (unlike str.title()) doesn't mangle apostrophes,
    e.g. "kids' toys" -> "Kids' Toys" rather than "Kids' Toys".
    """
    key = raw_category.strip().lower()

    if key in CATEGORY_DISPLAY_MAP:
        return CATEGORY_DISPLAY_MAP[key]

    return " ".join(word[:1].upper() + word[1:] for word in key.split())


# --------------------------------------------------------
# Bucketing rules (inferred from existing bronze_products data)
# --------------------------------------------------------


def classify_price(price: float) -> str:
    if price < 50:
        return "Budget"
    if price < 100:
        return "Standard"
    return "Premium"


def classify_value_segment(price: float) -> str:
    if price < 50:
        return "Low Value"
    if price < 100:
        return "Medium Value"
    return "High Value"


def classify_rating(rating_rate: float) -> str:
    if rating_rate < 3.0:
        return "Poor"
    if rating_rate < 4.0:
        return "Average"
    if rating_rate < 4.5:
        return "Good"
    return "Excellent"


# --------------------------------------------------------
# Transform a Product
# --------------------------------------------------------


def transform_product(product: dict, data_source: str = "Fake Store API") -> dict:
    """
    Transforms a raw product (FakeStore API shape: id/title/price/category/
    description/image/rating) into the bronze/silver column shape used by
    fakestore_catalog.etl_project.

    `product["rating"]` may be a nested {"rate": .., "count": ..} dict (as
    returned by the API) or already-flattened rating_rate/rating_count keys.
    """

    rating = product.get("rating") or {}
    rating_rate = float(rating.get("rate", product.get("rating_rate", 0.0)) or 0.0)
    rating_count = int(rating.get("count", product.get("rating_count", 0)) or 0)

    price = float(product["price"])
    now = datetime.now(timezone.utc)

    return {
        "product_id": int(product["id"]),
        "product_name": product["title"].strip(),
        "product_category": normalize_category(product["category"]),
        "price": price,
        "price_category": classify_price(price),
        "rating_rate": rating_rate,
        "rating_count": rating_count,
        "product_description": product.get("description", ""),
        "image_url": product.get("image", ""),
        "data_source": data_source,
        "etl_load_timestamp": now,
        "rating_category": classify_rating(rating_rate),
        "product_value_segment": classify_value_segment(price),
    }
