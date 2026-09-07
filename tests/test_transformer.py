"""
test_transformer.py
--------------------
Unit tests for transform/transformer.py — pure transformation logic, no
mocking needed. This is the core deliverable for Exercise 6: unit tests for
the pipeline's transformation logic.
"""

from datetime import datetime, timezone

import pytest

from transform.transformer import (
    classify_price,
    classify_rating,
    classify_value_segment,
    normalize_category,
    transform_product,
)


# --------------------------------------------------------
# normalize_category
# --------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("electronics", "Electronics"),
        ("jewelery", "Jewelry"),  # the FakeStore typo -> corrected display name
        ("jewelry", "Jewelry"),
        ("men's clothing", "Men's Clothing"),
        ("women's clothing", "Women's Clothing"),
        (" Electronics ", "Electronics"),  # whitespace + case are normalized
        ("ELECTRONICS", "Electronics"),
    ],
)
def test_normalize_category_known(raw, expected):
    assert normalize_category(raw) == expected


def test_normalize_category_unknown_is_word_capitalized():
    # Falls back to per-word capitalization for unmapped categories.
    assert normalize_category("kids' toys") == "Kids' Toys"


def test_normalize_category_unknown_single_word():
    assert normalize_category("garden") == "Garden"


# --------------------------------------------------------
# classify_price
# --------------------------------------------------------

@pytest.mark.parametrize(
    "price, expected",
    [
        (0, "Budget"),
        (49.99, "Budget"),
        (50, "Standard"),       # lower boundary is inclusive on the Standard side
        (99.99, "Standard"),
        (100, "Premium"),       # lower boundary is inclusive on the Premium side
        (500, "Premium"),
    ],
)
def test_classify_price(price, expected):
    assert classify_price(price) == expected


# --------------------------------------------------------
# classify_value_segment
# --------------------------------------------------------

@pytest.mark.parametrize(
    "price, expected",
    [
        (0, "Low Value"),
        (49.99, "Low Value"),
        (50, "Medium Value"),
        (99.99, "Medium Value"),
        (100, "High Value"),
    ],
)
def test_classify_value_segment(price, expected):
    assert classify_value_segment(price) == expected


# --------------------------------------------------------
# classify_rating
# --------------------------------------------------------

@pytest.mark.parametrize(
    "rating, expected",
    [
        (0, "Poor"),
        (2.99, "Poor"),
        (3.0, "Average"),
        (3.99, "Average"),
        (4.0, "Good"),
        (4.49, "Good"),
        (4.5, "Excellent"),
        (5.0, "Excellent"),
    ],
)
def test_classify_rating(rating, expected):
    assert classify_rating(rating) == expected


# --------------------------------------------------------
# transform_product
# --------------------------------------------------------

def test_transform_product_maps_all_fields(raw_product):
    result = transform_product(raw_product)

    assert result["product_id"] == 1
    assert result["product_name"] == "Fjallraven Backpack"  # stripped
    assert result["product_category"] == "Men's Clothing"
    assert result["price"] == 109.95
    assert result["price_category"] == "Premium"
    assert result["rating_rate"] == 4.6
    assert result["rating_count"] == 259
    assert result["rating_category"] == "Excellent"
    assert result["product_value_segment"] == "High Value"
    assert result["product_description"] == raw_product["description"]
    assert result["image_url"] == raw_product["image"]
    assert result["data_source"] == "Fake Store API"


def test_transform_product_expected_keys(raw_product):
    result = transform_product(raw_product)

    assert set(result.keys()) == {
        "product_id", "product_name", "product_category", "price",
        "price_category", "rating_rate", "rating_count",
        "product_description", "image_url", "data_source",
        "etl_load_timestamp", "rating_category", "product_value_segment",
    }


def test_transform_product_etl_timestamp_is_utc_now(raw_product):
    before = datetime.now(timezone.utc)
    result = transform_product(raw_product)
    after = datetime.now(timezone.utc)

    assert before <= result["etl_load_timestamp"] <= after
    assert result["etl_load_timestamp"].tzinfo is not None


def test_transform_product_custom_data_source(raw_product):
    result = transform_product(raw_product, data_source="Replay")
    assert result["data_source"] == "Replay"


def test_transform_product_accepts_flattened_rating_keys():
    # Some upstream stages (e.g. after Kafka round-trips) may already have
    # flattened rating_rate/rating_count instead of a nested "rating" dict.
    product = {
        "id": 5,
        "title": "Mug",
        "price": 12.5,
        "category": "electronics",
        "rating_rate": 4.2,
        "rating_count": 10,
    }

    result = transform_product(product)

    assert result["rating_rate"] == 4.2
    assert result["rating_count"] == 10


def test_transform_product_missing_rating_defaults_to_zero():
    product = {
        "id": 6,
        "title": "Unrated Item",
        "price": 15.0,
        "category": "electronics",
    }

    result = transform_product(product)

    assert result["rating_rate"] == 0.0
    assert result["rating_count"] == 0
    assert result["rating_category"] == "Poor"


def test_transform_product_missing_description_and_image_default_empty():
    product = {"id": 7, "title": "No Extras", "price": 1.0, "category": "electronics"}
    result = transform_product(product)

    assert result["product_description"] == ""
    assert result["image_url"] == ""
