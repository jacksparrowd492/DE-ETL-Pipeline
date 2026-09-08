"""
validator.py
------------
Validates raw product records before they are sent to Kafka.
"""

import logging

# --------------------------------------------------------
# Configure Logger
# --------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ["id", "title", "price", "category"]


# --------------------------------------------------------
# Validate a Single Product
# --------------------------------------------------------


def is_valid_product(product: dict) -> bool:
    """
    Checks that a product has all required fields and sane values.
    """

    for field in REQUIRED_FIELDS:
        if field not in product or product[field] in (None, ""):
            logger.warning("Product missing/empty field '%s' | id=%s", field, product.get("id"))
            return False

    try:
        float(product["price"])
    except (TypeError, ValueError):
        logger.warning("Product %s has invalid price: %s", product.get("id"), product.get("price"))
        return False

    return True


# --------------------------------------------------------
# Validate a Batch of Products
# --------------------------------------------------------


def validate_products(products: list) -> list:
    """
    Filters a list of raw products down to only the valid ones.
    """

    valid = [p for p in products if is_valid_product(p)]

    logger.info(
        "Validation complete | Valid=%s | Invalid=%s", len(valid), len(products) - len(valid)
    )

    return valid


# --------------------------------------------------------
# Test
# --------------------------------------------------------

if __name__ == "__main__":
    sample = [
        {"id": 1, "title": "Laptop", "price": 599.99, "category": "electronics"},
        {"id": 2, "title": "", "price": 20, "category": "misc"},
    ]
    print(validate_products(sample))
