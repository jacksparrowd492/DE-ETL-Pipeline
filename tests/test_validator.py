"""
test_validator.py
------------------
Unit tests for validation/validator.py.
"""

import pytest

from validation.validator import is_valid_product, validate_products


# --------------------------------------------------------
# is_valid_product
# --------------------------------------------------------

def test_is_valid_product_accepts_well_formed_product(raw_product):
    assert is_valid_product(raw_product) is True


@pytest.mark.parametrize("field", ["id", "title", "price", "category"])
def test_is_valid_product_rejects_missing_field(raw_product, field):
    del raw_product[field]
    assert is_valid_product(raw_product) is False


@pytest.mark.parametrize("field", ["title", "category"])
def test_is_valid_product_rejects_empty_string_field(raw_product, field):
    raw_product[field] = ""
    assert is_valid_product(raw_product) is False


def test_is_valid_product_rejects_none_price(raw_product):
    raw_product["price"] = None
    assert is_valid_product(raw_product) is False


def test_is_valid_product_rejects_non_numeric_price(raw_product):
    raw_product["price"] = "not-a-price"
    assert is_valid_product(raw_product) is False


def test_is_valid_product_accepts_numeric_string_price(raw_product):
    # float("19.99") succeeds, so a numeric string price is currently valid.
    raw_product["price"] = "19.99"
    assert is_valid_product(raw_product) is True


def test_is_valid_product_accepts_zero_price(raw_product):
    raw_product["price"] = 0
    assert is_valid_product(raw_product) is True


# --------------------------------------------------------
# validate_products
# --------------------------------------------------------

def test_validate_products_filters_out_invalid_records(raw_products):
    result = validate_products(raw_products)

    assert len(result) == 1
    assert result[0]["id"] == raw_products[0]["id"]


def test_validate_products_keeps_order_of_valid_records(raw_product):
    second = dict(raw_product, id=2, title="Second Product")
    result = validate_products([raw_product, second])

    assert [p["id"] for p in result] == [1, 2]


def test_validate_products_empty_list_returns_empty_list():
    assert validate_products([]) == []


def test_validate_products_all_invalid_returns_empty_list():
    invalid = [{"id": 1, "title": "", "price": 1, "category": "x"}]
    assert validate_products(invalid) == []
