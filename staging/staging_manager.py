"""
staging_manager.py
------------------
Promotes a product from silver_products into the star-schema warehouse
tables in fakestore_catalog.etl_project:

    dim_category            - category dictionary (category_id -> name)
    fact_products            - one row per product, FK'd to dim_category
    gold_product_summary     - per-category aggregate, refreshed incrementally
"""

import logging

from Data_Connection import get_connection
from config import table

# --------------------------------------------------------
# Configure Logger
# --------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger(__name__)

DIM_CATEGORY_TABLE = table("dim_category")
FACT_PRODUCTS_TABLE = table("fact_products")
GOLD_SUMMARY_TABLE = table("gold_product_summary")
SILVER_TABLE = table("silver_products")


# --------------------------------------------------------
# Create Warehouse Tables
# --------------------------------------------------------


def create_products_table():
    """
    Creates dim_category, fact_products and gold_product_summary in
    fakestore_catalog.etl_project if they do not already exist.
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DIM_CATEGORY_TABLE} (
            category_id INT,
            product_category STRING
        )
        """)

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {FACT_PRODUCTS_TABLE} (
            product_id INT,
            category_id INT,
            price DOUBLE,
            rating_rate DOUBLE,
            rating_count INT,
            price_category STRING,
            product_value_segment STRING,
            rating_category STRING
        )
        """)

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_SUMMARY_TABLE} (
            product_category STRING,
            total_products BIGINT,
            average_price DOUBLE,
            minimum_price DOUBLE,
            maximum_price DOUBLE,
            average_rating DOUBLE,
            total_rating_count BIGINT,
            gold_load_timestamp TIMESTAMP
        )
        """)

        conn.commit()

        logger.info("dim_category / fact_products / gold_product_summary are ready.")

    except Exception as e:
        logger.exception("Error creating warehouse tables: %s", e)
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --------------------------------------------------------
# dim_category — get existing id or create a new one
# --------------------------------------------------------


def get_or_create_category_id(cursor, conn, product_category: str) -> int:
    cursor.execute(
        f"SELECT category_id FROM {DIM_CATEGORY_TABLE} WHERE product_category = ?",
        (product_category,),
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    cursor.execute(f"SELECT COALESCE(MAX(category_id), 0) + 1 FROM {DIM_CATEGORY_TABLE}")
    new_id = int(cursor.fetchone()[0])

    cursor.execute(
        f"INSERT INTO {DIM_CATEGORY_TABLE} (category_id, product_category) VALUES (?, ?)",
        (new_id, product_category),
    )
    conn.commit()

    logger.info("Created new category '%s' with category_id=%s", product_category, new_id)
    return new_id


# --------------------------------------------------------
# fact_products — upsert one product
# --------------------------------------------------------

FACT_COLUMNS = (
    "product_id",
    "category_id",
    "price",
    "rating_rate",
    "rating_count",
    "price_category",
    "product_value_segment",
    "rating_category",
)


def upsert_fact_product(product: dict, category_id: int):
    """
    Inserts (or updates, if the product_id already exists) one row in
    fact_products.
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            MERGE INTO {FACT_PRODUCTS_TABLE} AS target
            USING (
                SELECT
                    ? AS product_id, ? AS category_id, ? AS price,
                    ? AS rating_rate, ? AS rating_count, ? AS price_category,
                    ? AS product_value_segment, ? AS rating_category
            ) AS source
            ON target.product_id = source.product_id
            WHEN MATCHED THEN UPDATE SET
                category_id = source.category_id,
                price = source.price,
                rating_rate = source.rating_rate,
                rating_count = source.rating_count,
                price_category = source.price_category,
                product_value_segment = source.product_value_segment,
                rating_category = source.rating_category
            WHEN NOT MATCHED THEN INSERT ({", ".join(FACT_COLUMNS)})
                VALUES (
                    source.product_id, source.category_id, source.price,
                    source.rating_rate, source.rating_count, source.price_category,
                    source.product_value_segment, source.rating_category
                )
            """,
            (
                product["product_id"],
                category_id,
                product["price"],
                product["rating_rate"],
                product["rating_count"],
                product["price_category"],
                product["product_value_segment"],
                product["rating_category"],
            ),
        )

        conn.commit()

        logger.info("Upserted Product %s into fact_products.", product["product_id"])

    except Exception as e:
        logger.exception(
            "fact_products upsert failed for Product %s: %s", product.get("product_id"), e
        )
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --------------------------------------------------------
# gold_product_summary — refresh one category's aggregate row
# --------------------------------------------------------


def refresh_gold_summary(product_category: str):
    """
    Recomputes gold_product_summary for a single category from silver_products
    and MERGEs the result in (insert if new, update if it already exists).
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            MERGE INTO {GOLD_SUMMARY_TABLE} AS target
            USING (
                SELECT
                    product_category,
                    COUNT(*) AS total_products,
                    AVG(price) AS average_price,
                    MIN(price) AS minimum_price,
                    MAX(price) AS maximum_price,
                    AVG(rating_rate) AS average_rating,
                    SUM(rating_count) AS total_rating_count
                FROM {SILVER_TABLE}
                WHERE product_category = ?
                GROUP BY product_category
            ) AS source
            ON target.product_category = source.product_category
            WHEN MATCHED THEN UPDATE SET
                total_products = source.total_products,
                average_price = source.average_price,
                minimum_price = source.minimum_price,
                maximum_price = source.maximum_price,
                average_rating = source.average_rating,
                total_rating_count = source.total_rating_count,
                gold_load_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                product_category, total_products, average_price, minimum_price,
                maximum_price, average_rating, total_rating_count, gold_load_timestamp
            )
            VALUES (
                source.product_category, source.total_products, source.average_price,
                source.minimum_price, source.maximum_price, source.average_rating,
                source.total_rating_count, current_timestamp()
            )
            """,
            (product_category,),
        )

        conn.commit()

        logger.info("gold_product_summary refreshed for category '%s'.", product_category)

    except Exception as e:
        logger.exception("gold_product_summary refresh failed for '%s': %s", product_category, e)
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --------------------------------------------------------
# Promote one product: silver -> dim_category -> fact -> gold
# --------------------------------------------------------


def promote_to_warehouse(product: dict):
    """
    Given an already-transformed product (see transform.transformer), upserts
    it into dim_category + fact_products and refreshes its gold summary row.
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        category_id = get_or_create_category_id(cursor, conn, product["product_category"])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    upsert_fact_product(product, category_id)
    refresh_gold_summary(product["product_category"])


# --------------------------------------------------------
# Backwards-compatible alias (old flat-table pipeline name)
# --------------------------------------------------------


def merge_staging_to_products():
    """
    Deprecated name kept for callers written against the old flat
    staging_products -> products design. New code should call
    promote_to_warehouse(product) directly, since promotion now needs the
    specific product being promoted (dim/fact/gold are keyed per-product).
    """
    logger.warning(
        "merge_staging_to_products() is deprecated - call "
        "promote_to_warehouse(product) with the transformed product instead."
    )
