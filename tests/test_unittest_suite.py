"""
test_unittest_suite.py
-----------------------
Plain unittest.TestCase suite for the transformation/validation logic,
written without pytest fixtures or pytest-mock so it can be run
independently of pytest, using only the Python standard library:

    python -m unittest discover tests

It exercises the same production code as tests/test_transformer.py and
tests/test_validator.py (transform.transformer, validation.validator) —
this file exists to prove the unit tests aren't tied to the pytest runner,
not to duplicate coverage. The pytest-based suite remains the primary,
more thorough set of tests.
"""

import unittest

from transform.transformer import classify_price, classify_rating, normalize_category, transform_product
from validation.validator import is_valid_product, validate_products


class TransformerTestCase(unittest.TestCase):

    def test_normalize_category_maps_jewelery_typo_to_jewelry(self):
        self.assertEqual(normalize_category("jewelery"), "Jewelry")

    def test_classify_price_boundaries(self):
        self.assertEqual(classify_price(49.99), "Budget")
        self.assertEqual(classify_price(50), "Standard")
        self.assertEqual(classify_price(100), "Premium")

    def test_classify_rating_boundaries(self):
        self.assertEqual(classify_rating(2.99), "Poor")
        self.assertEqual(classify_rating(4.5), "Excellent")

    def test_transform_product_produces_expected_shape(self):
        product = {
            "id": 1,
            "title": " Backpack ",
            "price": 109.95,
            "category": "men's clothing",
            "rating": {"rate": 4.6, "count": 259},
        }
        result = transform_product(product)

        self.assertEqual(result["product_id"], 1)
        self.assertEqual(result["product_name"], "Backpack")
        self.assertEqual(result["product_category"], "Men's Clothing")
        self.assertEqual(result["price_category"], "Premium")
        self.assertEqual(result["rating_category"], "Excellent")


class ValidatorTestCase(unittest.TestCase):

    def test_is_valid_product_rejects_missing_field(self):
        product = {"id": 1, "title": "Item", "price": 9.99}  # no category
        self.assertFalse(is_valid_product(product))

    def test_validate_products_filters_invalid_records(self):
        products = [
            {"id": 1, "title": "Valid", "price": 9.99, "category": "electronics"},
            {"id": 2, "title": "", "price": 9.99, "category": "electronics"},
        ]
        result = validate_products(products)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)


if __name__ == "__main__":
    unittest.main()
