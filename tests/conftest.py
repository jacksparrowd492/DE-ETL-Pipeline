"""
conftest.py
-----------
Shared pytest fixtures. `pythonpath = .` in pytest.ini already puts the
project root on sys.path, so plain imports like `from transform.transformer
import ...` work from any test module.
"""

import pytest


@pytest.fixture
def raw_product():
    """One raw product exactly as the FakeStore API returns it."""
    return {
        "id": 1,
        "title": "  Fjallraven Backpack  ",
        "price": 109.95,
        "category": "men's clothing",
        "description": "A backpack for everyday use.",
        "image": "https://fakestoreapi.com/img/backpack.jpg",
        "rating": {"rate": 4.6, "count": 259},
    }


@pytest.fixture
def raw_products(raw_product):
    """A small batch: one valid product, one missing a required field."""
    invalid = {
        "id": 2,
        "title": "",
        "price": 19.99,
        "category": "electronics",
    }
    return [raw_product, invalid]
