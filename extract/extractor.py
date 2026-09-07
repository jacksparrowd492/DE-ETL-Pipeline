"""
extractor.py
------------
Extracts raw product data from the FakeStore API.
"""

import logging

import requests

from config import config

# --------------------------------------------------------
# Configure Logger
# --------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------
# Extract Products
# --------------------------------------------------------

def extract_products():
    """
    Fetches all products from the FakeStore API.

    Returns:
        list[dict]: Raw product records as returned by the API.
    """

    try:
        response = requests.get(config.FAKESTORE_API_URL, timeout=10)
        response.raise_for_status()

        products = response.json()

        logger.info(
            "Extracted %s products from FakeStore API.",
            len(products)
        )

        return products

    except requests.exceptions.RequestException as e:
        logger.exception("Failed to extract products: %s", e)
        raise


# --------------------------------------------------------
# Test
# --------------------------------------------------------

if __name__ == "__main__":
    data = extract_products()
    print(f"Extracted {len(data)} products")