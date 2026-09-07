"""
streamlit_app.py
-----------------
Interactive Streamlit dashboard for the FakeStore ETL pipeline, backed by
fakestore_catalog.etl_project in Databricks Unity Catalog:

    bronze_products        raw + classified, one row per ingested product
    silver_products        cleaned/validated copy of bronze (dashboard reads this)
    dim_category            category dictionary (category_id -> name)
    fact_products            star-schema fact table, FK'd to dim_category
    gold_product_summary     per-category aggregate, refreshed incrementally

Add Product panel (sidebar)
    validate -> transform (classify price/rating tiers, normalize category) ->
    insert into bronze_products -> insert into silver_products -> upsert
    dim_category/fact_products -> refresh that category's gold_product_summary
    row. Uses the exact same functions the Kafka consumer uses, so a product
    saved here is identical to one produced by the Airflow/Kafka pipeline.
    Optionally also publishes the raw product to Kafka (best-effort, never
    blocks the Databricks write).

Dashboard (main area)
    KPIs + charts + a browsable table, read live from Databricks. Cached for
    30s and force-refreshed right after every write.

Run:
    streamlit run streamlit_app.py
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config, table
from Data_Connection import get_connection
from validation.validator import is_valid_product
from transform.transformer import transform_product
from load.databricks_loader import (
    FAILED_FILE,
    create_staging_table,
    load_to_staging,
)
from staging.staging_manager import create_products_table, promote_to_warehouse

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("streamlit_app")

# Kafka is optional for this dashboard — if the broker isn't reachable we
# simply disable the checkbox instead of crashing the whole app (the
# producer module opens a live connection at import time).
try:
    from kafka1.producer import send_product as kafka_send_product
    KAFKA_AVAILABLE = True
except Exception as e:  # noqa: BLE001 - deliberately broad, this is optional infra
    KAFKA_AVAILABLE = False
    kafka_send_product = None
    logger.warning("Kafka producer unavailable, disabling Kafka publish option: %s", e)


# ---------------------------------------------------------------------------
# Page config + palette (validated categorical palette — dataviz skill)
# ---------------------------------------------------------------------------

st.set_page_config(page_title="FakeStore ETL Dashboard", page_icon="🛒", layout="wide")


def inject_custom_css():
    """Flat, professional dark theme layered on top of Streamlit's default
    components via their stable data-testid hooks. No behavior changes.

    Deliberately NOT here: animated gradient backdrops, glass/blur panels and
    3D hover tilt — they read as gimmicky rather than professional and were
    tuned for a light background, so they're dropped in favor of solid dark
    surfaces, hairline borders and restrained hover states."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@600;700&family=Inter:wght@400;500;600&display=swap');

        :root {
            --accent-blue: #3987e5;
            --accent-orange: #d95926;
            --accent-violet: #9085e9;
            --bg-page: #0d0d0d;
            --bg-surface: #1a1a19;
            --border: rgba(255,255,255,0.10);
            --text-primary: #ffffff;
            --text-secondary: #c3c2b7;
            --text-muted: #898781;
            --shadow-soft: 0 4px 16px rgba(0,0,0,0.35);
        }

        html, body, [class*="css"] { font-family: 'Inter', sans-serif; color: var(--text-primary); }

        /* Flat dark page background */
        [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
            background: var(--bg-page);
        }

        /* Sidebar — flat dark surface, hairline divider */
        [data-testid="stSidebar"] {
            background: var(--bg-surface);
            border-right: 1px solid var(--border);
        }

        /* Gradient title, single subtle entrance (no repeating animation) */
        h1 {
            font-family: 'Poppins', sans-serif;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-violet) 60%, var(--accent-orange));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        /* KPI metric cards — flat surface, hairline border, no glass/tilt */
        [data-testid="stMetric"] {
            background: var(--bg-surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 18px 16px 14px;
            box-shadow: var(--shadow-soft);
            position: relative;
            overflow: hidden;
        }
        [data-testid="stMetric"]::before {
            content: "";
            position: absolute;
            inset: 0 0 auto 0;
            height: 3px;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-orange));
        }
        [data-testid="stMetricValue"] { color: var(--text-primary); }
        [data-testid="stMetricLabel"] { color: var(--text-secondary); }

        /* Buttons — flat accent fill, restrained hover */
        .stButton > button, .stFormSubmitButton > button {
            border-radius: 10px !important;
            border: none !important;
            background: var(--accent-blue) !important;
            color: white !important;
            transition: filter 0.15s ease !important;
            font-weight: 600 !important;
        }
        .stButton > button:hover, .stFormSubmitButton > button:hover {
            filter: brightness(1.12);
        }

        /* Tabs — sliding underline */
        [data-testid="stTabs"] button[role="tab"] {
            transition: color 0.2s ease;
            font-weight: 600;
            color: var(--text-secondary);
        }
        [data-testid="stTabs"] button[role="tab"]:hover {
            color: var(--accent-blue) !important;
        }
        [data-testid="stTabs"] [data-baseweb="tab-highlight"] {
            background-color: var(--accent-blue) !important;
            height: 3px !important;
            border-radius: 3px;
            transition: left 0.3s cubic-bezier(.2,.8,.2,1), width 0.3s cubic-bezier(.2,.8,.2,1);
        }

        /* Plotly chart cards — flat surface, hairline border */
        [data-testid="stPlotlyChart"] {
            background: var(--bg-surface);
            border-radius: 12px;
            padding: 8px;
            border: 1px solid var(--border);
        }

        /* DataFrame */
        [data-testid="stDataFrame"] {
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid var(--border);
        }

        /* Expander */
        [data-testid="stExpander"] {
            border-radius: 12px !important;
            border: 1px solid var(--border) !important;
            background: var(--bg-surface);
        }

        /* Dividers */
        hr { border: none; height: 1px; background: var(--border); }

        /* Scrollbar */
        ::-webkit-scrollbar { width: 10px; height: 10px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 8px; }

        /* Inputs — soft focus ring */
        .stTextInput input, .stNumberInput input, .stTextArea textarea,
        .stSelectbox div[data-baseweb="select"] > div {
            border-radius: 8px !important;
            transition: box-shadow 0.2s ease, border-color 0.2s ease;
        }
        .stTextInput input:focus, .stNumberInput input:focus, .stTextArea textarea:focus {
            box-shadow: 0 0 0 3px rgba(57,135,229,0.30) !important;
            border-color: var(--accent-blue) !important;
        }

        /* Section headings (st.subheader) — brand font, tighter tracking */
        h2, h3 { font-family: 'Poppins', sans-serif; letter-spacing: -0.01em; }

        /* Pipeline flow diagram */
        .pipeline-flow {
            display: flex;
            align-items: stretch;
            flex-wrap: wrap;
            gap: 2px;
            background: var(--bg-surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px 16px 16px;
            box-shadow: var(--shadow-soft);
        }
        .stage {
            flex: 1 1 118px;
            min-width: 110px;
            text-align: center;
            padding: 6px 8px;
            border-radius: 10px;
            transition: background 0.15s ease;
        }
        .stage:hover { background: rgba(255,255,255,0.05); }
        .stage-icon { font-size: 21px; line-height: 1.3; }
        .stage-title {
            font-family: 'Poppins', sans-serif;
            font-weight: 600;
            font-size: 12.5px;
            color: var(--text-primary);
            margin-top: 2px;
        }
        .stage-sub {
            font-size: 11px;
            color: var(--text-muted);
            margin-top: 2px;
            line-height: 1.35;
        }
        .stage-arrow {
            flex: 0 0 auto;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--text-muted);
            font-size: 15px;
            padding: 0 2px;
        }
        .pipeline-branch-row {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-top: 10px;
            padding: 8px 12px;
            border-top: 1px dashed var(--border);
            font-size: 12px;
            color: var(--text-secondary);
        }
        .status-dot { font-weight: 700; }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_custom_css()

# Dark-mode categorical steps (validated: node scripts/validate_palette.js
# "<hexes>" --mode dark -> ALL CHECKS PASS against the #1a1a19 chart surface).
CATEGORY_PALETTE = [
    "#3987e5",  # blue
    "#d95926",  # orange
    "#199e70",  # aqua
    "#c98500",  # yellow
    "#d55181",  # magenta
    "#008300",  # green
    "#9085e9",  # violet
    "#e66767",  # red
]
SEQUENTIAL_BLUE = "#3987e5"
CHART_SURFACE = "#1a1a19"  # dark chart surface

# Shared dark-mode chart ink so every axis/legend/gridline reads clearly
# against CHART_SURFACE without repeating the same block on every figure.
CHART_TEXT_PRIMARY = "#ffffff"
CHART_TEXT_SECONDARY = "#c3c2b7"
CHART_TEXT_MUTED = "#898781"
CHART_GRIDLINE = "#2c2c2a"
CHART_AXIS_LINE = "#383835"


def apply_dark_axes(fig):
    """Apply the shared dark-mode chart chrome (ink, gridlines, legend) on
    top of a Plotly Express figure, leaving each chart's own titles,
    ordering and margins untouched."""
    axis_style = dict(
        gridcolor=CHART_GRIDLINE,
        linecolor=CHART_AXIS_LINE,
        tickfont_color=CHART_TEXT_MUTED,
        title_font_color=CHART_TEXT_SECONDARY,
    )
    fig.update_layout(
        font_color=CHART_TEXT_PRIMARY,
        legend_font_color=CHART_TEXT_PRIMARY,
        xaxis=axis_style,
        yaxis=axis_style,
    )
    return fig


# Ordinal ramps (one hue, light -> dark) for the price / rating tiers.
# Blue steps taken from the validated dark sequential ramp (darkest step no
# darker than step 600 / #184f95, the dark-mode ordinal contrast floor).
# Rating reuses the dark orange categorical hue at increasing opacity so it
# reads as a distinct measure from price.
PRICE_TIER_ORDER = ["Budget", "Standard", "Premium"]
PRICE_TIER_COLORS = {"Budget": "#184f95", "Standard": "#3987e5", "Premium": "#9ec5f4"}

RATING_TIER_ORDER = ["Poor", "Average", "Good", "Excellent"]
RATING_TIER_COLORS = {
    "Poor": "rgba(217,89,38,0.45)",
    "Average": "rgba(217,89,38,0.62)",
    "Good": "rgba(217,89,38,0.8)",
    "Excellent": "rgba(217,89,38,1.0)",
}

DEFAULT_CATEGORIES = ["Electronics", "Jewelry", "Men's Clothing", "Women's Clothing"]

BRONZE_TABLE = table("bronze_products")
SILVER_TABLE = table("silver_products")
DIM_CATEGORY_TABLE = table("dim_category")
GOLD_TABLE = table("gold_product_summary")


def category_color_map(categories):
    """Fixed category -> hue assignment so the same category keeps the same
    color across every chart on the page."""
    cats = sorted(set(categories))
    return {c: CATEGORY_PALETTE[i % len(CATEGORY_PALETTE)] for i, c in enumerate(cats)}


# Status palette (fixed, never themed — see dataviz skill) used only for the
# replay-queue state below the pipeline flow diagram.
STATUS_GOOD = "#0ca30c"
STATUS_WARNING = "#fab219"


def render_pipeline_flow(bronze_count, total_products, gold_rows, failed_count, kafka_topic, kafka_available):
    """Renders the extract -> validate -> Kafka -> transform -> load ->
    warehouse -> dashboard architecture as a connected flow diagram,
    annotated with a few live counts already loaded elsewhere on the page.
    Purely presentational — no data access of its own."""

    def stage(icon, title, sub):
        return f'''<div class="stage">
            <div class="stage-icon">{icon}</div>
            <div class="stage-title">{title}</div>
            <div class="stage-sub">{sub}</div>
        </div>'''

    arrow = '<div class="stage-arrow">&#8594;</div>'
    kafka_sub = f"topic: {kafka_topic}" if kafka_available else "broker unreachable"
    bronze_sub = f"{bronze_count:,} rows" if bronze_count is not None else "—"

    stages = [
        stage("&#127760;", "Extract", "FakeStore API"),
        stage("&#9989;", "Validate", "Required fields"),
        stage("&#128231;", "Kafka", kafka_sub),
        stage("&#128295;", "Transform", "Bucket &amp; normalize"),
        stage("&#129352;&#129351;", "Bronze &rarr; Silver", bronze_sub),
        stage("&#11088;", "Warehouse", f"{gold_rows} categories"),
        stage("&#128202;", "Dashboard", f"{total_products:,} products &middot; you are here"),
    ]

    if failed_count == 0:
        status_html = f'<span class="status-dot" style="color:{STATUS_GOOD}">&#9679;</span> All writes landed — nothing pending'
    else:
        status_html = (
            f'<span class="status-dot" style="color:{STATUS_WARNING}">&#9679;</span> '
            f'{failed_count} record(s) failed to load and are queued for replay — retry from the sidebar'
        )

    st.markdown(
        f'''<div class="pipeline-flow">{arrow.join(stages)}</div>
        <div class="pipeline-branch-row">&#8618; On a failed Databricks write: saved to <code>failed_records/</code> &nbsp;{status_html}</div>''',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# One-time setup — make sure Databricks tables exist
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def init_tables():
    create_staging_table()   # bronze + silver
    create_products_table()  # dim_category + fact_products + gold_product_summary
    return True


# ---------------------------------------------------------------------------
# Data access (cached, short TTL so the dashboard stays live)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30, show_spinner="Loading products from Databricks...")
def load_silver() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {SILVER_TABLE}", conn)
    finally:
        conn.close()
    if not df.empty:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["rating_rate"] = pd.to_numeric(df["rating_rate"], errors="coerce")
        df["rating_count"] = pd.to_numeric(df["rating_count"], errors="coerce")
        df["etl_load_timestamp"] = pd.to_datetime(df["etl_load_timestamp"], errors="coerce")
        df["silver_load_timestamp"] = pd.to_datetime(df["silver_load_timestamp"], errors="coerce")
    return df


@st.cache_data(ttl=30, show_spinner=False)
def load_gold() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {GOLD_TABLE} ORDER BY product_category", conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=30, show_spinner=False)
def load_bronze_count() -> int:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {BRONZE_TABLE}")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def failed_records_count() -> int:
    if not os.path.exists(FAILED_FILE):
        return 0
    try:
        with open(FAILED_FILE) as f:
            return len(json.load(f))
    except (json.JSONDecodeError, OSError):
        return 0


def next_product_id(df: pd.DataFrame) -> int:
    if df is not None and not df.empty:
        return int(df["product_id"].max()) + 1
    return int(datetime.now().timestamp())


def clear_dashboard_cache():
    load_silver.clear()
    load_gold.clear()
    load_bronze_count.clear()


# ---------------------------------------------------------------------------
# Write path — Add Product -> validate -> transform -> Databricks
# ---------------------------------------------------------------------------

def add_product(raw_product: dict, publish_to_kafka: bool = False):
    """
    Runs one product through validate -> transform -> bronze/silver -> dim/
    fact/gold, synchronously, the same chain the Kafka consumer runs, so it
    shows up in the dashboard immediately.
    """
    if not is_valid_product(raw_product):
        return False, "Validation failed — title, price and category are required and price must be numeric."

    transformed = transform_product(raw_product, data_source="Streamlit Manual Entry")

    try:
        load_to_staging(transformed)       # bronze + silver (also saves to failed_records/ on failure)
        promote_to_warehouse(transformed)  # dim_category + fact_products + gold_product_summary
    except Exception as e:  # noqa: BLE001
        return False, f"Databricks write failed — saved locally for replay. ({e})"

    if publish_to_kafka and KAFKA_AVAILABLE:
        try:
            kafka_send_product(raw_product)
        except Exception as e:  # noqa: BLE001
            logger.warning("Kafka publish failed (non-blocking): %s", e)

    return True, transformed


def retry_failed_records():
    """Re-attempts every locally-saved failed record directly against Databricks."""
    if not os.path.exists(FAILED_FILE):
        return 0, 0
    with open(FAILED_FILE) as f:
        failed = json.load(f)
    if not failed:
        return 0, 0

    remaining = []
    succeeded = 0
    for product in failed:
        ts = product.get("etl_load_timestamp")
        if isinstance(ts, str):
            try:
                product["etl_load_timestamp"] = datetime.fromisoformat(ts)
            except ValueError:
                product["etl_load_timestamp"] = datetime.now(timezone.utc)
        try:
            load_to_staging(product)
            promote_to_warehouse(product)
            succeeded += 1
        except Exception:  # noqa: BLE001
            remaining.append(product)

    with open(FAILED_FILE, "w") as f:
        json.dump(remaining, f, indent=4, default=str)

    return succeeded, len(remaining)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

try:
    init_tables()
except Exception as e:  # noqa: BLE001
    st.warning(f"Could not verify/create Databricks tables: {e}")

st.title("🛒 FakeStore ETL — Live Dashboard")
st.caption(
    f"Add a product on the left — it's validated, transformed and written straight "
    f"into `{config.DATABRICKS_CATALOG}.{config.DATABRICKS_SCHEMA}` "
    f"(bronze → silver → dim/fact → gold) through the same pipeline code as the Kafka consumer."
)

# ---------------- Sidebar: Add Product + pipeline controls ----------------
with st.sidebar:
    st.header("➕ Add a Product")
    with st.form("add_product_form", clear_on_submit=True):
        title = st.text_input("Title*")
        price = st.number_input("Price* ($)", min_value=0.0, step=0.5, format="%.2f")
        category_choice = st.selectbox("Category*", DEFAULT_CATEGORIES + ["Other..."])
        custom_category = ""
        if category_choice == "Other...":
            custom_category = st.text_input("Custom category")
        rating_rate = st.slider("Rating", 0.0, 5.0, 4.0, step=0.1)
        rating_count = st.number_input("Rating count", min_value=0, step=1, value=0)
        description = st.text_area("Description")
        image = st.text_input("Image URL", value="https://i.imgur.com/placeholder.png")
        publish_kafka = st.checkbox(
            "Also publish to Kafka",
            value=False,
            disabled=not KAFKA_AVAILABLE,
            help=None if KAFKA_AVAILABLE else "Kafka broker not reachable right now.",
        )
        submitted = st.form_submit_button("💾 Save to Databricks", use_container_width=True)

    if submitted:
        category = (custom_category or category_choice).strip()
        if not title.strip() or not category or category == "Other...":
            st.error("Please fill in a title and a category.")
        else:
            try:
                current_df = load_silver()
            except Exception:  # noqa: BLE001
                current_df = None

            raw = {
                "id": next_product_id(current_df),
                "title": title.strip(),
                "price": price,
                "category": category,
                "description": description,
                "image": image or "",
                "rating": {"rate": rating_rate, "count": int(rating_count)},
            }
            ok, result = add_product(raw, publish_to_kafka=publish_kafka)
            if ok:
                st.success(f"Product #{result['product_id']} saved to Databricks ✅")
                clear_dashboard_cache()
                st.rerun()
            else:
                st.error(result)

    st.divider()
    st.header("⚙️ Pipeline")
    col_a, col_b = st.columns(2)
    if col_a.button("🔄 Refresh", use_container_width=True):
        clear_dashboard_cache()
        st.rerun()
    if col_b.button("🔁 Retry failed", use_container_width=True):
        succeeded, remaining = retry_failed_records()
        st.info(f"Replayed {succeeded} record(s) · {remaining} still failing.")
        clear_dashboard_cache()
        st.rerun()

# ---------------- Load data ----------------
try:
    products_df = load_silver()
    load_error = None
except Exception as e:  # noqa: BLE001
    products_df = pd.DataFrame()
    load_error = str(e)

if load_error:
    st.error(f"Could not read from Databricks: {load_error}")
    st.stop()

try:
    gold_df = load_gold()
except Exception:  # noqa: BLE001
    gold_df = pd.DataFrame()

try:
    bronze_count = load_bronze_count()
except Exception:  # noqa: BLE001
    bronze_count = None

failed_count = failed_records_count()

# ---------------- Pipeline flow diagram ----------------
st.subheader("Pipeline Workflow")
render_pipeline_flow(
    bronze_count=bronze_count,
    total_products=len(products_df),
    gold_rows=len(gold_df),
    failed_count=failed_count,
    kafka_topic=config.KAFKA_TOPIC,
    kafka_available=KAFKA_AVAILABLE,
)

st.divider()

# ---------------- KPI row ----------------
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Products", len(products_df))
k2.metric("Inventory Value", f"${products_df['price'].sum():,.2f}" if not products_df.empty else "$0.00")
k3.metric("Avg. Price", f"${products_df['price'].mean():,.2f}" if not products_df.empty else "$0.00")
k4.metric("Avg. Rating", f"{products_df['rating_rate'].mean():.2f} ★" if not products_df.empty else "—")
k5.metric("Pending Replay", failed_count)

if products_df.empty:
    st.info("No products in Databricks yet — add one from the sidebar to get started.")
    st.stop()

color_map = category_color_map(products_df["product_category"])

st.divider()

tab_overview, tab_price_rating, tab_gold, tab_catalog = st.tabs(
    ["📊 Overview", "💰 Price & Rating", "🥇 Gold Summary", "📋 Catalog"]
)

# ---------------- Overview tab ----------------
with tab_overview:
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Products by Category")
        counts = products_df["product_category"].value_counts().reset_index()
        counts.columns = ["product_category", "count"]
        fig = px.bar(
            counts, x="count", y="product_category", orientation="h",
            color="product_category", color_discrete_map=color_map, text="count",
        )
        fig.update_traces(textposition="outside", marker_line_width=0)
        fig.update_layout(
            showlegend=False, yaxis_title="Product Category", xaxis_title="Number of Products",
            plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
            yaxis=dict(categoryorder="total ascending"),
            margin=dict(t=10),
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Category Share")
        fig = px.pie(
            products_df, names="product_category", color="product_category",
            color_discrete_map=color_map, hole=0.55,
        )
        fig.update_traces(
            textinfo="percent+label",
            hovertemplate="Category: %{label}<br>Products: %{value} (%{percent})<extra></extra>",
        )
        fig.update_layout(
            paper_bgcolor=CHART_SURFACE, margin=dict(t=10),
            legend_title_text="Product Category",
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Products Ingested Over Time")
    ts_col = "etl_load_timestamp"
    if products_df[ts_col].notna().any():
        by_day = (
            products_df.dropna(subset=[ts_col])
            .assign(day=lambda d: d[ts_col].dt.date)
            .groupby("day").size().reset_index(name="count")
        )
        fig = px.line(by_day, x="day", y="count", markers=True, color_discrete_sequence=[SEQUENTIAL_BLUE])
        fig.update_traces(line_width=2, marker_size=8)
        fig.update_layout(
            plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
            xaxis_title="Date", yaxis_title="Products Ingested", margin=dict(t=10),
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No ingestion timestamps available yet.")

# ---------------- Price & Rating tab ----------------
with tab_price_rating:
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Price Distribution")
        fig = px.histogram(products_df, x="price", nbins=20, color_discrete_sequence=[SEQUENTIAL_BLUE])
        fig.update_layout(
            bargap=0.05, plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
            xaxis_title="Price ($)", yaxis_title="Number of Products", margin=dict(t=10),
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        st.subheader("Average Price by Category")
        avg_price = products_df.groupby("product_category")["price"].mean().reset_index()
        fig = px.bar(
            avg_price, x="product_category", y="price", color="product_category",
            color_discrete_map=color_map, text_auto=".2f",
        )
        fig.update_traces(marker_line_width=0)
        fig.update_layout(
            showlegend=False, plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
            xaxis_title="Product Category", yaxis_title="Avg. Price ($)", margin=dict(t=10),
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)

    c5, c6 = st.columns(2)

    with c5:
        st.subheader("Price Tier Breakdown")
        tier_counts = (
            products_df["price_category"].value_counts()
            .reindex(PRICE_TIER_ORDER).fillna(0).reset_index()
        )
        tier_counts.columns = ["price_category", "count"]
        fig = px.bar(
            tier_counts, x="price_category", y="count", color="price_category",
            color_discrete_map=PRICE_TIER_COLORS, category_orders={"price_category": PRICE_TIER_ORDER},
            text="count",
        )
        fig.update_traces(textposition="outside", marker_line_width=0)
        fig.update_layout(
            showlegend=False, plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
            xaxis_title="Price Tier", yaxis_title="Number of Products", margin=dict(t=10),
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)

    with c6:
        st.subheader("Rating Tier Breakdown")
        rating_counts = (
            products_df["rating_category"].value_counts()
            .reindex(RATING_TIER_ORDER).fillna(0).reset_index()
        )
        rating_counts.columns = ["rating_category", "count"]
        fig = px.bar(
            rating_counts, x="rating_category", y="count", color="rating_category",
            color_discrete_map=RATING_TIER_COLORS, category_orders={"rating_category": RATING_TIER_ORDER},
            text="count",
        )
        fig.update_traces(textposition="outside", marker_line_width=0)
        fig.update_layout(
            showlegend=False, plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
            xaxis_title="Rating Tier", yaxis_title="Number of Products", margin=dict(t=10),
        )
        apply_dark_axes(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Price vs. Rating")
    fig = px.scatter(
        products_df, x="price", y="rating_rate", color="rating_category",
        color_discrete_map=RATING_TIER_COLORS, category_orders={"rating_category": RATING_TIER_ORDER},
        hover_data=["product_name", "product_category"],
    )
    fig.update_traces(marker=dict(size=9, line=dict(width=1, color=CHART_SURFACE)))
    fig.update_layout(
        plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
        xaxis_title="Price ($)", yaxis_title="Rating (out of 5)", margin=dict(t=10),
        legend_title_text="Rating Tier",
    )
    apply_dark_axes(fig)
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Gold Summary tab ----------------
with tab_gold:
    if gold_df.empty:
        st.caption("gold_product_summary has no rows yet.")
    else:
        g1, g2 = st.columns(2)

        with g1:
            st.subheader("Total Products by Category (Gold)")
            fig = px.bar(
                gold_df, x="product_category", y="total_products", color="product_category",
                color_discrete_map=color_map, text="total_products",
            )
            fig.update_traces(textposition="outside", marker_line_width=0)
            fig.update_layout(
                showlegend=False, plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
                xaxis_title="Product Category", yaxis_title="Total Products", margin=dict(t=10),
            )
            apply_dark_axes(fig)
            st.plotly_chart(fig, use_container_width=True)

        with g2:
            st.subheader("Average Rating by Category (Gold)")
            fig = px.bar(
                gold_df, x="product_category", y="average_rating", color="product_category",
                color_discrete_map=color_map, text_auto=".2f",
            )
            fig.update_traces(marker_line_width=0)
            fig.update_layout(
                showlegend=False, plot_bgcolor=CHART_SURFACE, paper_bgcolor=CHART_SURFACE,
                xaxis_title="Product Category", yaxis_title="Avg. Rating (out of 5)", margin=dict(t=10),
            )
            apply_dark_axes(fig)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("gold_product_summary")
        st.dataframe(gold_df, use_container_width=True, hide_index=True)

# ---------------- Catalog tab ----------------
with tab_catalog:
    st.subheader("📋 Product Catalog (silver_products)")
    fc1, fc2 = st.columns(2)
    filter_cats = fc1.multiselect("Filter by category", sorted(products_df["product_category"].unique()))
    filter_tiers = fc2.multiselect("Filter by price tier", PRICE_TIER_ORDER)

    table_df = products_df
    if filter_cats:
        table_df = table_df[table_df["product_category"].isin(filter_cats)]
    if filter_tiers:
        table_df = table_df[table_df["price_category"].isin(filter_tiers)]

    st.dataframe(
        table_df.sort_values("silver_load_timestamp", ascending=False)[
            ["product_id", "product_name", "price", "price_category", "product_category",
             "rating_rate", "rating_count", "rating_category", "data_source", "etl_load_timestamp"]
        ],
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("⚙️ Pipeline status"):
        s1, s2 = st.columns(2)
        s1.metric("Rows in bronze_products", bronze_count if bronze_count is not None else "—")
        s2.metric("Failed records awaiting replay", failed_count)
        st.caption(
            "Every add writes bronze_products → silver_products → dim_category/"
            "fact_products → gold_product_summary (scoped to the affected category). "
            "The catalog view above reads from silver_products."
        )
