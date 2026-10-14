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

Pages (top navigation)
    Overview      KPIs, category mix, ingestion volume, pipeline health
    Analytics     price / rating distributions and tiers
    Products      per-product user activity (views, cart, purchases, revenue) from
                  the streaming lakehouse: Trino view delta.lakehouse.v_product_activity
    Gold Layer    per-category aggregates from gold_product_summary
    Catalog       searchable, sortable silver_products browser + CSV export
    Add Product   validate -> transform -> bronze/silver -> dim/fact -> gold,
                  using the exact same functions the Kafka consumer uses, so a
                  product saved here is identical to one produced by the
                  Airflow/Kafka pipeline. Optionally also publishes the raw
                  product to Kafka (best-effort, never blocks the write).
    Pipeline      stage-by-stage flow, layer row counts, replay queue, config
    Monitoring    alerts, task duration vs SLA, pipeline latency, alert channels and
                  the lineage graph + column lineage of every final table (reads the
                  local logs/*.jsonl files, so it works even when Databricks is down)

The sidebar filters (category, price/rating tier, price range) apply to
Overview, Analytics, Products and Catalog. Reads are cached for 30s and force-refreshed
right after every write.

Run:
    streamlit run streamlit_app.py
"""

import html
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config, table
from observability import setup_logging
from Data_Connection import get_connection
from validation.validator import is_valid_product
from transform.transformer import transform_product
from load.databricks_loader import (
    BRONZE_COLUMNS,
    FAILED_FILE,
    SILVER_COLUMNS,
    create_staging_table,
    load_to_staging,
)
from staging.staging_manager import FACT_COLUMNS, create_products_table, promote_to_warehouse
from validation.validator import REQUIRED_FIELDS
from governance import lineage
from serving import product_activity
from observability.logging_config import log_dir as obs_log_dir

setup_logging("streamlit")
logger = logging.getLogger("streamlit_app")

# Latency SLA per pipeline task (pipeline_tasks.py), for the Monitoring page.
# Imported defensively: pipeline_tasks pulls in the Kafka client.
try:
    from pipeline_tasks import SLA_SECONDS as PIPELINE_SLA_SECONDS
except Exception:  # noqa: BLE001 - optional, only used for the SLA marker
    PIPELINE_SLA_SECONDS = {}

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


st.set_page_config(
    page_title="FakeStore ETL · Lakehouse Monitor",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Design tokens
# ---------------------------------------------------------------------------

# Surfaces / ink. SURFACE is the card + chart surface every palette below was
# validated against (dataviz validate_palette.js --mode dark --surface #131b2e).
BG_PAGE = "#0b1120"
SURFACE = "#131b2e"
SURFACE_RAISED = "#1a2438"
BORDER = "#243049"
TEXT_PRIMARY = "#f1f5f9"
TEXT_SECONDARY = "#cbd5e1"
TEXT_MUTED = "#94a3b8"
GRIDLINE = "#1f2a40"
AXIS_LINE = "#2b3852"
ACCENT = "#3987e5"

# Categorical hues in fixed order (ALL CHECKS PASS on SURFACE). Categories
# past the 8th fold into a neutral gray rather than cycling hues.
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
OTHER_GRAY = "#64748b"
SEQUENTIAL_BLUE = "#3987e5"

# Ordinal tier ramps — one hue each, monotone lightness, lightest-on-surface
# step clears 2:1 (validated with --ordinal against SURFACE).
PRICE_TIER_ORDER = ["Budget", "Standard", "Premium"]
PRICE_TIER_COLORS = {"Budget": "#184f95", "Standard": "#3987e5", "Premium": "#9ec5f4"}

RATING_TIER_ORDER = ["Poor", "Average", "Good", "Excellent"]
RATING_TIER_COLORS = {
    "Poor": "#8a3a1a",
    "Average": "#b14a20",
    "Good": "#d95926",
    "Excellent": "#f08a5d",
}

# Status palette (fixed, never reused as a series color; always paired with
# an icon + label so state is never carried by color alone).
STATUS_GOOD = "#0ca30c"
STATUS_WARNING = "#fab219"
STATUS_CRITICAL = "#d03b3b"

DEFAULT_CATEGORIES = ["Electronics", "Jewelry", "Men's Clothing", "Women's Clothing"]

BRONZE_TABLE = table("bronze_products")
SILVER_TABLE = table("silver_products")
DIM_CATEGORY_TABLE = table("dim_category")
GOLD_TABLE = table("gold_product_summary")


def inject_custom_css():
    """Slate-navy professional theme layered on Streamlit's components via
    their stable data-testid hooks and the st-key-* classes Streamlit adds to
    keyed containers. Purely presentational."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        :root {{
            --bg: {BG_PAGE};
            --surface: {SURFACE};
            --surface-raised: {SURFACE_RAISED};
            --border: {BORDER};
            --text: {TEXT_PRIMARY};
            --text-2: {TEXT_SECONDARY};
            --muted: {TEXT_MUTED};
            --accent: {ACCENT};
            --good: {STATUS_GOOD};
            --warn: {STATUS_WARNING};
            --crit: {STATUS_CRITICAL};
        }}

        html, body, [class*="css"], .stMarkdown, button, input, textarea {{
            font-family: 'Inter', system-ui, sans-serif;
        }}
        [data-testid="stAppViewContainer"] {{ background: var(--bg); }}
        [data-testid="stHeader"] {{ background: rgba(11,17,32,0.85); backdrop-filter: blur(6px); }}
        .block-container {{ padding-top: 3.6rem; padding-bottom: 3rem; max-width: 1480px; }}

        [data-testid="stSidebar"] {{ border-right: 1px solid var(--border); }}
        [data-testid="stSidebar"] .block-container {{ padding-top: 1.2rem; }}

        h1, h2, h3, h4 {{ letter-spacing: -0.015em; color: var(--text); }}

        /* ---------- Cards: any st.container(key="card_...") ---------- */
        [class*="st-key-card"] {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 18px 20px 14px;
        }}
        .card-title {{ font-size: 15px; font-weight: 600; color: var(--text); margin: 0; }}
        .card-sub {{ font-size: 12.5px; color: var(--muted); margin: 2px 0 6px; }}

        /* ---------- Page header ---------- */
        .page-head {{ display: flex; justify-content: space-between; align-items: flex-end;
                      gap: 16px; flex-wrap: wrap; margin: 4px 0 18px; }}
        .page-eyebrow {{ font-size: 12px; font-weight: 600; letter-spacing: .08em;
                         text-transform: uppercase; color: var(--accent); margin-bottom: 4px; }}
        .page-title {{ font-size: 28px; font-weight: 700; color: var(--text); line-height: 1.15; margin: 0; }}
        .page-desc {{ font-size: 14px; color: var(--muted); margin-top: 6px; max-width: 760px; }}
        .pill-row {{ display: flex; gap: 8px; flex-wrap: wrap; }}
        .pill {{ display: inline-flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500;
                 color: var(--text-2); background: var(--surface); border: 1px solid var(--border);
                 padding: 5px 10px; border-radius: 999px; white-space: nowrap; }}
        .pill b {{ color: var(--text); font-weight: 600; }}
        .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}

        /* ---------- KPI tiles ---------- */
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                     gap: 14px; margin-bottom: 18px; }}
        .kpi {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px;
                padding: 16px 18px; }}
        .kpi-label {{ font-size: 12.5px; color: var(--muted); font-weight: 500; display: flex;
                      align-items: center; gap: 6px; }}
        .kpi-value {{ font-size: 26px; font-weight: 700; color: var(--text); margin-top: 6px;
                      font-variant-numeric: tabular-nums; }}
        .kpi-foot {{ font-size: 12px; color: var(--muted); margin-top: 4px; }}

        /* ---------- Filter summary chips ---------- */
        .filter-bar {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
                       font-size: 13px; color: var(--muted); margin: -4px 0 14px; }}
        .chip {{ background: rgba(57,135,229,.12); color: #9ec5f4; border: 1px solid rgba(57,135,229,.35);
                 padding: 2px 9px; border-radius: 999px; font-size: 12px; }}

        /* ---------- Key/value health list ---------- */
        .kv {{ display: flex; justify-content: space-between; align-items: center; padding: 9px 0;
               border-bottom: 1px solid var(--border); font-size: 13.5px; }}
        .kv:last-child {{ border-bottom: none; }}
        .kv-k {{ color: var(--muted); }}
        .kv-v {{ color: var(--text); font-weight: 600; display: flex; align-items: center; gap: 7px;
                 font-variant-numeric: tabular-nums; text-align: right; }}

        /* ---------- Recent products list ---------- */
        .recent {{ display: flex; gap: 12px; align-items: center; padding: 9px 0;
                   border-bottom: 1px solid var(--border); }}
        .recent:last-child {{ border-bottom: none; }}
        .recent-swatch {{ width: 4px; align-self: stretch; border-radius: 4px; }}
        .recent-main {{ flex: 1; min-width: 0; }}
        .recent-name {{ font-size: 13.5px; color: var(--text); font-weight: 500; white-space: nowrap;
                        overflow: hidden; text-overflow: ellipsis; }}
        .recent-meta {{ font-size: 12px; color: var(--muted); }}
        .recent-price {{ font-size: 13.5px; color: var(--text); font-weight: 600;
                         font-variant-numeric: tabular-nums; }}

        /* ---------- Pipeline stepper ---------- */
        .stepper {{ display: flex; overflow-x: auto; padding: 6px 2px 4px; }}
        .step {{ flex: 1 0 128px; position: relative; text-align: center; padding: 0 6px; }}
        .step:not(:last-child)::after {{ content: ""; position: absolute; top: 17px; left: calc(50% + 22px);
                                         right: calc(-50% + 22px); height: 2px; background: var(--border); }}
        .step-num {{ width: 36px; height: 36px; margin: 0 auto 8px; border-radius: 50%;
                     display: flex; align-items: center; justify-content: center; font-weight: 700;
                     font-size: 14px; color: var(--text); background: var(--surface-raised);
                     border: 2px solid var(--accent); }}
        .step-num.warn {{ border-color: var(--warn); }}
        .step-num.here {{ background: var(--accent); }}
        .step-name {{ font-size: 13px; font-weight: 600; color: var(--text); }}
        .step-detail {{ font-size: 11.5px; color: var(--muted); margin-top: 2px; }}
        .step-stat {{ font-size: 12px; color: var(--text-2); margin-top: 6px; font-weight: 600;
                      font-variant-numeric: tabular-nums; }}

        /* ---------- Rules / steps lists on Add Product ---------- */
        .rule {{ display: flex; justify-content: space-between; font-size: 13px; padding: 7px 0;
                 border-bottom: 1px solid var(--border); color: var(--text-2); }}
        .rule:last-child {{ border-bottom: none; }}
        .rule span:last-child {{ color: var(--muted); }}
        .swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 3px;
                   margin-right: 8px; vertical-align: middle; }}
        .flow-li {{ display: flex; gap: 10px; font-size: 13px; color: var(--text-2); padding: 6px 0; }}
        .flow-li .n {{ flex: 0 0 22px; height: 22px; border-radius: 50%; background: var(--surface-raised);
                       border: 1px solid var(--border); color: var(--text); font-size: 11.5px;
                       display: flex; align-items: center; justify-content: center; font-weight: 600; }}

        /* ---------- Widgets ---------- */
        .stButton > button, .stFormSubmitButton > button, .stDownloadButton > button {{
            border-radius: 10px !important; font-weight: 600 !important;
        }}
        .stButton > button[kind="primary"], .stFormSubmitButton > button[kind="primary"] {{
            background: var(--accent) !important; border: none !important;
        }}
        [data-testid="stDataFrame"] {{ border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }}
        [data-testid="stPlotlyChart"] {{ margin-top: 4px; }}
        hr {{ border-color: var(--border) !important; }}
        code {{ color: #9ec5f4 !important; background: rgba(57,135,229,.10) !important; }}

        .sb-brand {{ display: flex; gap: 10px; align-items: center; margin-bottom: 6px; }}
        .sb-logo {{ width: 34px; height: 34px; border-radius: 9px; background: var(--accent);
                    display: flex; align-items: center; justify-content: center; font-size: 18px; }}
        .sb-name {{ font-weight: 700; color: var(--text); font-size: 15px; line-height: 1.1; }}
        .sb-tag {{ font-size: 11.5px; color: var(--muted); }}
        .sb-section {{ font-size: 11px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase;
                       color: var(--muted); margin: 14px 0 4px; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_custom_css()


# ---------------------------------------------------------------------------
# Presentation helpers
# ---------------------------------------------------------------------------


def esc(value) -> str:
    return html.escape(str(value))


def fmt_money(value) -> str:
    return f"${value:,.2f}" if pd.notna(value) else "—"


def time_ago(ts) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    seconds = int((pd.Timestamp.now(tz="UTC") - ts).total_seconds())
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def status_badge(state: str, label: str) -> str:
    """Status = icon + label + fixed status color, never color alone."""
    color, icon = {
        "good": (STATUS_GOOD, "&#10003;"),
        "warning": (STATUS_WARNING, "&#9888;"),
        "critical": (STATUS_CRITICAL, "&#10005;"),
    }[state]
    return f'<span style="color:{color};font-weight:700">{icon}</span> {esc(label)}'


def page_header(eyebrow: str, title: str, description: str, pills_html: str = ""):
    st.markdown(
        f"""<div class="page-head">
            <div>
              <div class="page-eyebrow">{esc(eyebrow)}</div>
              <div class="page-title">{esc(title)}</div>
              <div class="page-desc">{description}</div>
            </div>
            <div class="pill-row">{pills_html}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def card_header(title: str, subtitle: str | None = None):
    sub = f'<div class="card-sub">{esc(subtitle)}</div>' if subtitle else ""
    st.markdown(f'<div class="card-title">{esc(title)}</div>{sub}', unsafe_allow_html=True)


def kpi_grid(tiles):
    """tiles: list of (label, value, foot_html)."""
    cells = "".join(
        f'<div class="kpi"><div class="kpi-label">{esc(label)}</div>'
        f'<div class="kpi-value">{esc(value)}</div><div class="kpi-foot">{foot}</div></div>'
        for label, value, foot in tiles
    )
    st.markdown(f'<div class="kpi-grid">{cells}</div>', unsafe_allow_html=True)


def category_color_map(categories):
    """Fixed category -> hue assignment (sorted name order, never cycled) so
    the same category keeps the same color on every chart and every filter."""
    cats = sorted(set(categories))
    return {
        c: CATEGORY_PALETTE[i] if i < len(CATEGORY_PALETTE) else OTHER_GRAY
        for i, c in enumerate(cats)
    }


def style_fig(fig, x_title=None, y_title=None, height=320, grid="y", legend=False):
    """Shared chart chrome: card surface, recessive grid on the value axis
    only, muted ticks, 4px rounded bar ends, dark hover labels."""
    fig.update_layout(
        height=height,
        margin=dict(l=4, r=12, t=8, b=4),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font=dict(family="Inter, system-ui, sans-serif", color=TEXT_SECONDARY, size=12),
        showlegend=legend,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, x=0, title_text="", font_color=TEXT_SECONDARY
        ),
        bargap=0.28,
        barcornerradius=4,
        hoverlabel=dict(
            bgcolor=BG_PAGE,
            bordercolor=BORDER,
            font_color=TEXT_PRIMARY,
            font_family="Inter, system-ui, sans-serif",
        ),
    )
    axis = dict(
        linecolor=AXIS_LINE,
        zeroline=False,
        tickfont_color=TEXT_MUTED,
        title_font_color=TEXT_SECONDARY,
        gridcolor=GRIDLINE,
    )
    fig.update_xaxes(title_text=x_title, showgrid=grid == "x", **axis)
    fig.update_yaxes(title_text=y_title, showgrid=grid == "y", **axis)
    return fig


def show_chart(fig):
    st.plotly_chart(fig, width="stretch", theme=None, config={"displayModeBar": False})


def empty_state(message: str):
    st.markdown(
        f'<div style="padding:28px 8px;text-align:center;color:{TEXT_MUTED};font-size:13.5px">'
        f"{esc(message)}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# One-time setup — make sure Databricks tables exist
# ---------------------------------------------------------------------------


@st.cache_resource(show_spinner=False)
def init_tables():
    create_staging_table()  # bronze + silver
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
        df["etl_load_timestamp"] = pd.to_datetime(
            df["etl_load_timestamp"], errors="coerce", utc=True
        )
        df["silver_load_timestamp"] = pd.to_datetime(
            df["silver_load_timestamp"], errors="coerce", utc=True
        )
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


def load_failed_records() -> list:
    if not os.path.exists(FAILED_FILE):
        return []
    try:
        with open(FAILED_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def failed_records_count() -> int:
    return len(load_failed_records())


def next_product_id(df: pd.DataFrame) -> int:
    if df is not None and not df.empty:
        return int(df["product_id"].max()) + 1
    return int(datetime.now().timestamp())


def clear_dashboard_cache():
    load_silver.clear()
    load_gold.clear()
    load_bronze_count.clear()
    load_product_activity.clear()


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
        return (
            False,
            "Validation failed — title, price and category are required and price must be numeric.",
        )

    transformed = transform_product(raw_product, data_source="Streamlit Manual Entry")

    try:
        load_to_staging(transformed)  # bronze + silver (also saves to failed_records/ on failure)
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
# Actions (button callbacks) + flash messages that survive st.rerun()
# ---------------------------------------------------------------------------


def flash(message: str, icon: str = "✅"):
    st.session_state["_flash"] = (message, icon)


def action_refresh():
    clear_dashboard_cache()
    flash("Data refreshed from Databricks", "🔄")


def action_retry():
    succeeded, remaining = retry_failed_records()
    clear_dashboard_cache()
    flash(
        f"Replayed {succeeded} record(s) · {remaining} still failing",
        "🔁" if not remaining else "⚠️",
    )


if "_flash" in st.session_state:
    message, icon = st.session_state.pop("_flash")
    st.toast(message, icon=icon)


# ---------------------------------------------------------------------------
# Monitoring & lineage (Phase II) - reads only local files, so it also works
# when Databricks is down (it is shown under the connection-error card too)
# ---------------------------------------------------------------------------

SEVERITY_STATE = {"critical": "critical", "error": "critical", "warning": "warning", "info": "good"}


def read_jsonl(path: str, limit: int = 20000) -> list:
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f.readlines()[-limit:]:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


@st.cache_data(ttl=15, show_spinner=False)
def load_observability() -> dict:
    directory = obs_log_dir()
    alerts = pd.DataFrame(read_jsonl(os.path.join(directory, "alerts.jsonl")))
    metrics = pd.DataFrame(read_jsonl(os.path.join(directory, "metrics.jsonl")))
    lineage_events = read_jsonl(os.path.join(directory, "lineage", "openlineage.jsonl"), 2000)
    for df in (alerts, metrics):
        if not df.empty:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
    if not metrics.empty:
        metrics["task"] = metrics["labels"].map(lambda labels: (labels or {}).get("task"))
    return {"alerts": alerts, "metrics": metrics, "lineage": lineage_events, "dir": directory}


def channel_rows() -> list:
    def state(ok: bool, label_on: str, label_off: str = "Not configured") -> str:
        return status_badge("good", label_on) if ok else status_badge("warning", label_off)

    return [
        ("Slack", state(bool(os.getenv("SLACK_WEBHOOK_URL")), "Webhook set")),
        (
            "Email",
            state(
                bool(os.getenv("SMTP_HOST") and os.getenv("ALERT_EMAIL_TO")),
                f"{len(os.getenv('ALERT_EMAIL_TO', '').split(','))} recipient(s)",
            ),
        ),
        ("Amazon SNS", state(bool(os.getenv("ALERT_SNS_TOPIC_ARN")), "Topic set")),
        ("Alerts file", status_badge("good", "logs/alerts.jsonl (always)")),
        ("OpenLineage backend", state(bool(os.getenv("OPENLINEAGE_URL")), "Sending events")),
        ("Pushgateway", state(bool(os.getenv("PUSHGATEWAY_URL")), "Pushing metrics")),
    ]


def render_monitoring(embedded: bool = False):
    if not embedded:
        page_header(
            "Operations",
            "Monitoring & lineage",
            "Alerts, task metrics and pipeline latency from the local observability files "
            "(<code>logs/*.jsonl</code>), and the lineage of every final table from "
            "<code>governance/lineage.py</code>.",
        )
    data = load_observability()
    alerts, metrics = data["alerts"], data["metrics"]
    day_ago = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)

    recent_alerts = alerts[alerts["ts"] >= day_ago] if not alerts.empty else alerts
    critical = (
        int(recent_alerts["severity"].isin(["critical", "error"]).sum())
        if len(recent_alerts)
        else 0
    )
    runs = (
        metrics[metrics["name"] == "pipeline_run_success"].sort_values("ts")
        if not metrics.empty
        else pd.DataFrame()
    )
    tasks = metrics[metrics["name"] == "task_success"] if not metrics.empty else pd.DataFrame()
    failures = int((tasks[tasks["ts"] >= day_ago]["value"] == 0).sum()) if len(tasks) else 0
    latency = (
        metrics[metrics["name"] == "kafka_end_to_end_latency_seconds"]
        if not metrics.empty
        else pd.DataFrame()
    )

    if len(runs):
        last = runs.iloc[-1]
        run_value = "Succeeded" if last["value"] == 1 else "Failed"
        run_foot = status_badge("good" if last["value"] == 1 else "critical", time_ago(last["ts"]))
    else:
        run_value, run_foot = "—", "No run recorded yet"
    kpi_grid(
        [
            ("Last pipeline run", run_value, run_foot),
            (
                "Alerts (24h)",
                f"{len(recent_alerts)}",
                status_badge("critical", f"{critical} error/critical")
                if critical
                else status_badge("good", "No errors"),
            ),
            (
                "Task failures (24h)",
                f"{failures}",
                status_badge("good", "All tasks passed")
                if not failures
                else status_badge("critical", "See alerts"),
            ),
            (
                "Kafka → Databricks p95",
                f"{latency['value'].quantile(0.95):,.1f}s" if len(latency) else "—",
                f"{len(latency):,} messages measured" if len(latency) else "Consumer not run yet",
            ),
        ]
    )

    left, right = st.columns([3, 2], gap="medium")
    with left, st.container(key="card_mon_alerts"):
        card_header(
            "Recent alerts", "Newest first · every alert also went to the configured channels"
        )
        if alerts.empty:
            empty_state("No alerts recorded. Failures and SLA breaches will show up here.")
        else:
            shown = alerts.sort_values("ts", ascending=False).head(50)
            st.dataframe(
                shown.assign(
                    severity=shown["severity"].str.upper(),
                    when=shown["ts"].map(time_ago),
                )[["when", "severity", "title", "message", "component", "task", "run_id"]],
                width="stretch",
                hide_index=True,
                height=320,
            )
    with right, st.container(key="card_mon_channels"):
        card_header("Alert channels", "Configured from .env (values are never shown)")
        st.markdown(
            "".join(
                f'<div class="kv"><span class="kv-k">{esc(k)}</span><span class="kv-v">{v}</span></div>'
                for k, v in channel_rows()
            ),
            unsafe_allow_html=True,
        )

    dur_col, lat_col = st.columns(2, gap="medium")
    with dur_col, st.container(key="card_mon_durations"):
        card_header("Task duration vs SLA", "Latest run of each task, seconds")
        durations = (
            metrics[metrics["name"] == "task_duration_seconds"]
            if not metrics.empty
            else pd.DataFrame()
        )
        if durations.empty:
            empty_state("No task has run yet: try python run_pipeline_no_airflow.py.")
        else:
            latest = durations.sort_values("ts").groupby("task", as_index=False).last()
            latest["sla"] = latest["task"].map(PIPELINE_SLA_SECONDS)
            latest["state"] = [
                "Over SLA" if pd.notna(s) and v > s else "Within SLA"
                for v, s in zip(latest["value"], latest["sla"])
            ]
            fig = px.bar(
                latest,
                x="value",
                y="task",
                orientation="h",
                color="state",
                color_discrete_map={"Within SLA": ACCENT, "Over SLA": STATUS_CRITICAL},
                hover_data={"sla": True, "value": ":.2f"},
            )
            for _, row in latest[latest["sla"].notna()].iterrows():
                fig.add_shape(
                    type="line",
                    x0=row["sla"],
                    x1=row["sla"],
                    y0=row["task"],
                    y1=row["task"],
                    line=dict(color=TEXT_MUTED, width=10),
                )
            show_chart(style_fig(fig, x_title="seconds", grid="x", legend=True, height=260))
    with lat_col, st.container(key="card_mon_latency"):
        card_header("Pipeline latency", "Kafka → Databricks per message, and the stream's p95")
        series = (
            metrics[
                metrics["name"].isin(
                    ["kafka_end_to_end_latency_seconds", "stream_ingest_latency_p95_seconds"]
                )
            ]
            if not metrics.empty
            else pd.DataFrame()
        )
        if series.empty:
            empty_state("No latency samples yet: run the consumer or the Spark stream.")
        else:
            fig = px.line(
                series.sort_values("ts"),
                x="ts",
                y="value",
                color="name",
                color_discrete_sequence=[ACCENT, STATUS_WARNING],
            )
            show_chart(style_fig(fig, y_title="seconds", legend=True, height=260))

    with st.container(key="card_mon_lineage"):
        card_header(
            "Data lineage",
            "Pick a final table: its upstream sources and downstream consumers are highlighted",
        )
        finals = {d.resolved_name(): k for k, d in lineage.DATASETS.items() if d.managed}
        choice = st.selectbox(
            "Table", list(finals), index=list(finals.values()).index("dbx.gold_product_summary")
        )
        key = finals[choice]
        st.graphviz_chart(lineage.dot(highlight=key), width="stretch")
        dataset = lineage.DATASETS[key]
        types = dict(dataset.fields)
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "column": column,
                        "type": types.get(column, ""),
                        "comes from": ", ".join(
                            f"{lineage.DATASETS[s].resolved_name()}.{f}" for s, f in c.sources
                        )
                        or "generated",
                        "transformation": c.rule,
                        "kind": c.subtype.lower(),
                    }
                    for column, c in lineage.COLUMN_LINEAGE[key].items()
                ]
            ),
            width="stretch",
            hide_index=True,
        )
        st.caption(
            f"Owner: {dataset.owner} · PII: {dataset.pii} · Retention: {dataset.retention} · "
            f"{len(data['lineage'])} OpenLineage events in {data['dir']}"
        )


# ---------------------------------------------------------------------------
# Load data (shared by every page)
# ---------------------------------------------------------------------------

try:
    init_tables()
    init_error = None
except Exception as e:  # noqa: BLE001
    init_error = str(e)

try:
    products_df = load_silver()
    load_error = None
except Exception as e:  # noqa: BLE001
    products_df = pd.DataFrame()
    load_error = str(e)

if load_error:
    page_header(
        "Connection problem",
        "Can't reach Databricks",
        "The dashboard couldn't open a session on the configured SQL warehouse, so "
        "there is nothing to show yet. Check the points below, then retry.",
    )
    with st.container(key="card_conn_error"):
        card_header("Error from the Databricks SQL connector")
        st.code(load_error, language=None)
        st.markdown(
            "- Open **SQL Warehouses** in the Databricks UI and confirm the warehouse is running.\n"
            "- A `Cannot create the resource` error with reason `INACTIVE` means the whole "
            "workspace is deactivated — log in to the workspace to reactivate it.\n"
            "- Make sure `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH` and "
            "`DATABRICKS_TOKEN` in `.env` are current."
        )
        st.button("Retry connection", type="primary", on_click=action_refresh)
    st.markdown(
        '<div class="sb-section" style="margin-top:22px">Monitoring</div>', unsafe_allow_html=True
    )
    render_monitoring(embedded=True)
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
has_data = not products_df.empty
color_map = category_color_map(products_df["product_category"]) if has_data else {}
latest_load = products_df["silver_load_timestamp"].max() if has_data else None


def status_pills() -> str:
    kafka = (
        f'<span class="dot" style="background:{STATUS_GOOD}"></span>Kafka <b>online</b>'
        if KAFKA_AVAILABLE
        else f'<span class="dot" style="background:{STATUS_WARNING}"></span>Kafka <b>offline</b>'
    )
    replay_color = STATUS_GOOD if failed_count == 0 else STATUS_WARNING
    return (
        f'<span class="pill"><span class="dot" style="background:{STATUS_GOOD}"></span>'
        f"Databricks <b>connected</b></span>"
        f'<span class="pill">{kafka}</span>'
        f'<span class="pill"><span class="dot" style="background:{replay_color}"></span>'
        f"Replay queue <b>{failed_count}</b></span>"
        f'<span class="pill">Last load <b>{time_ago(latest_load)}</b></span>'
    )


# ---------------------------------------------------------------------------
# Sidebar — brand, global filters, data actions
# ---------------------------------------------------------------------------

all_categories = sorted(products_df["product_category"].dropna().unique()) if has_data else []
price_min = float(products_df["price"].min()) if has_data else 0.0
price_max = float(products_df["price"].max()) if has_data else 0.0
price_slider_enabled = has_data and price_max > price_min


def reset_filters():
    st.session_state["f_categories"] = []
    st.session_state["f_price_tiers"] = []
    st.session_state["f_rating_tiers"] = []
    if price_slider_enabled:
        st.session_state["f_price_range"] = (price_min, price_max)


with st.sidebar:
    st.markdown(
        f"""<div class="sb-brand"><div class="sb-logo">🛒</div>
            <div><div class="sb-name">FakeStore ETL</div>
            <div class="sb-tag">Lakehouse monitor · <code>{esc(config.DATABRICKS_SCHEMA)}</code></div></div>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sb-section">Filters</div>', unsafe_allow_html=True)
    st.caption("Apply to Overview, Analytics, Products and Catalog.")
    sel_categories = st.multiselect(
        "Category", all_categories, key="f_categories", placeholder="All categories"
    )
    sel_price_tiers = st.multiselect(
        "Price tier", PRICE_TIER_ORDER, key="f_price_tiers", placeholder="All price tiers"
    )
    sel_rating_tiers = st.multiselect(
        "Rating tier", RATING_TIER_ORDER, key="f_rating_tiers", placeholder="All rating tiers"
    )
    if price_slider_enabled:
        # Seed/clamp via session state (not value=) so Reset works without a
        # Streamlit warning and a new max price never leaves the range stale.
        stored = st.session_state.get("f_price_range")
        if not stored or stored[0] < price_min or stored[1] > price_max:
            st.session_state["f_price_range"] = (price_min, price_max)
        sel_price_range = st.slider(
            "Price range ($)",
            min_value=price_min,
            max_value=price_max,
            key="f_price_range",
            format="$%.0f",
        )
    else:
        sel_price_range = (price_min, price_max)
    st.button("Reset filters", on_click=reset_filters, width="stretch")

    st.markdown('<div class="sb-section">Data</div>', unsafe_allow_html=True)
    st.button(
        "Refresh data",
        icon=":material/refresh:",
        on_click=action_refresh,
        type="primary",
        width="stretch",
    )
    st.caption(f"Auto-refreshes every 30s · latest silver load {time_ago(latest_load)}")


filtered_df = products_df
if has_data:
    if sel_categories:
        filtered_df = filtered_df[filtered_df["product_category"].isin(sel_categories)]
    if sel_price_tiers:
        filtered_df = filtered_df[filtered_df["price_category"].isin(sel_price_tiers)]
    if sel_rating_tiers:
        filtered_df = filtered_df[filtered_df["rating_category"].isin(sel_rating_tiers)]
    if price_slider_enabled:
        lo, hi = sel_price_range
        filtered_df = filtered_df[filtered_df["price"].between(lo, hi)]


def filter_summary():
    """One line above the charts saying what's filtered and how much is left."""
    chips = [*sel_categories, *sel_price_tiers, *sel_rating_tiers]
    if price_slider_enabled and tuple(sel_price_range) != (price_min, price_max):
        chips.append(f"${sel_price_range[0]:,.0f}–${sel_price_range[1]:,.0f}")
    if not chips:
        return
    chip_html = "".join(f'<span class="chip">{esc(c)}</span>' for c in chips)
    st.markdown(
        f'<div class="filter-bar">Showing <b style="color:{TEXT_PRIMARY}">{len(filtered_df):,}</b> '
        f"of {len(products_df):,} products · {chip_html}</div>",
        unsafe_allow_html=True,
    )


def require_data(df: pd.DataFrame) -> bool:
    """Render an empty state (with a way forward) when there's nothing to plot."""
    if not has_data:
        with st.container(key="card_empty"):
            empty_state("No products in Databricks yet.")
            st.page_link(PAGE_ADD, label="Add your first product", icon=":material/add_circle:")
        return False
    if df.empty:
        with st.container(key="card_empty_filtered"):
            empty_state("No products match the current filters.")
            st.button("Reset filters", on_click=reset_filters, key="reset_inline")
        return False
    return True


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


def render_overview():
    page_header(
        "Dashboard",
        "Overview",
        "Live view of <code>silver_products</code> in "
        f"<code>{esc(config.DATABRICKS_CATALOG)}.{esc(config.DATABRICKS_SCHEMA)}</code>.",
        status_pills(),
    )
    filter_summary()
    if not require_data(filtered_df):
        return
    df = filtered_df

    day_ago = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)
    added_24h = int((df["etl_load_timestamp"] >= day_ago).sum())
    replay_foot = (
        status_badge("good", "All writes landed")
        if failed_count == 0
        else status_badge("warning", "Retry from Pipeline")
    )
    kpi_grid(
        [
            ("Total products", f"{len(df):,}", f"+{added_24h} in the last 24h"),
            ("Inventory value", fmt_money(df["price"].sum()), "Sum of list prices"),
            (
                "Average price",
                fmt_money(df["price"].mean()),
                f"Median {fmt_money(df['price'].median())}",
            ),
            (
                "Average rating",
                f"{df['rating_rate'].mean():.2f} / 5",
                f"{int(df['rating_count'].sum()):,} reviews",
            ),
            (
                "Categories",
                f"{df['product_category'].nunique()}",
                f"{len(gold_df)} in gold summary",
            ),
            ("Pending replay", f"{failed_count}", replay_foot),
        ]
    )

    left, right = st.columns([3, 2], gap="medium")
    with left, st.container(key="card_by_category"):
        card_header("Products by category", "Count and share of the current selection")
        counts = df["product_category"].value_counts().reset_index()
        counts.columns = ["product_category", "count"]
        counts["label"] = counts.apply(
            lambda r: f"{r['count']:,}  ·  {r['count'] / len(df):.0%}", axis=1
        )
        fig = px.bar(
            counts,
            x="count",
            y="product_category",
            orientation="h",
            text="label",
            color="product_category",
            color_discrete_map=color_map,
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>%{x:,} products<extra></extra>",
        )
        style_fig(fig, height=300, grid="x")
        fig.update_yaxes(categoryorder="total ascending", title_text=None)
        fig.update_xaxes(title_text="Products", range=[0, counts["count"].max() * 1.25])
        show_chart(fig)

    with right, st.container(key="card_health"):
        card_header("Pipeline health", "Current state of every layer")
        kafka_state = (
            status_badge("good", "Online")
            if KAFKA_AVAILABLE
            else status_badge("warning", "Unreachable")
        )
        replay_state = (
            status_badge("good", "Empty")
            if failed_count == 0
            else status_badge("warning", f"{failed_count} pending")
        )
        rows = [
            ("Databricks SQL", status_badge("good", "Connected")),
            ("Kafka broker", kafka_state),
            ("Bronze rows", f"{bronze_count:,}" if bronze_count is not None else "—"),
            ("Silver rows", f"{len(products_df):,}"),
            ("Gold categories", f"{len(gold_df)}"),
            ("Replay queue", replay_state),
            ("Latest silver load", time_ago(latest_load)),
        ]
        st.markdown(
            "".join(
                f'<div class="kv"><span class="kv-k">{k}</span><span class="kv-v">{v}</span></div>'
                for k, v in rows
            ),
            unsafe_allow_html=True,
        )
        st.page_link(PAGE_PIPELINE, label="Open pipeline view", icon=":material/arrow_forward:")

    left, right = st.columns([3, 2], gap="medium")
    with left, st.container(key="card_ingest"):
        card_header("Products ingested per day", "By ETL load date (UTC)")
        by_day = (
            df.dropna(subset=["etl_load_timestamp"])
            .assign(day=lambda d: d["etl_load_timestamp"].dt.strftime("%Y-%m-%d"))
            .groupby("day")
            .size()
            .reset_index(name="count")
        )
        if by_day.empty:
            empty_state("No ingestion timestamps available yet.")
        else:
            fig = px.bar(by_day, x="day", y="count", color_discrete_sequence=[SEQUENTIAL_BLUE])
            fig.update_traces(hovertemplate="<b>%{x}</b><br>%{y:,} products<extra></extra>")
            style_fig(fig, x_title=None, y_title="Products", height=280)
            fig.update_xaxes(type="category")
            show_chart(fig)

    with right, st.container(key="card_recent"):
        card_header("Recently added", "Newest rows in silver_products")
        recent = df.sort_values("silver_load_timestamp", ascending=False).head(6)
        items = "".join(
            f'<div class="recent"><span class="recent-swatch" '
            f'style="background:{color_map.get(r.product_category, OTHER_GRAY)}"></span>'
            f'<div class="recent-main"><div class="recent-name">{esc(r.product_name)}</div>'
            f'<div class="recent-meta">{esc(r.product_category)} · {esc(r.data_source)} · '
            f"{time_ago(r.silver_load_timestamp)}</div></div>"
            f'<div class="recent-price">{fmt_money(r.price)}</div></div>'
            for r in recent.itertuples()
        )
        st.markdown(items, unsafe_allow_html=True)


def render_analytics():
    page_header(
        "Dashboard",
        "Price & rating analytics",
        "How products are distributed across price points, rating tiers and categories.",
    )
    filter_summary()
    if not require_data(filtered_df):
        return
    df = filtered_df

    c1, c2 = st.columns(2, gap="medium")
    with c1, st.container(key="card_price_hist"):
        card_header("Price distribution", "Number of products per price band")
        fig = px.histogram(df, x="price", nbins=24, color_discrete_sequence=[SEQUENTIAL_BLUE])
        fig.update_traces(hovertemplate="Price %{x}<br>%{y:,} products<extra></extra>")
        style_fig(fig, x_title="Price ($)", y_title="Products", height=300)
        fig.update_layout(bargap=0.06)
        show_chart(fig)

    with c2, st.container(key="card_avg_price"):
        card_header("Average price by category", "Mean list price, highest first")
        avg_price = df.groupby("product_category")["price"].mean().reset_index()
        fig = px.bar(
            avg_price,
            x="price",
            y="product_category",
            orientation="h",
            color="product_category",
            color_discrete_map=color_map,
            text_auto="$,.2f",
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Avg. price $%{x:,.2f}<extra></extra>",
        )
        style_fig(fig, x_title="Average price ($)", height=300, grid="x")
        fig.update_yaxes(categoryorder="total ascending", title_text=None)
        fig.update_xaxes(range=[0, avg_price["price"].max() * 1.25])
        show_chart(fig)

    c3, c4 = st.columns(2, gap="medium")
    with c3, st.container(key="card_price_tier"):
        card_header("Price tiers", "Budget < $50 · Standard < $100 · Premium $100+")
        tiers = (
            df["price_category"].value_counts().reindex(PRICE_TIER_ORDER).fillna(0).reset_index()
        )
        tiers.columns = ["tier", "count"]
        fig = px.bar(
            tiers,
            x="tier",
            y="count",
            color="tier",
            color_discrete_map=PRICE_TIER_COLORS,
            text="count",
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>%{y:,} products<extra></extra>",
        )
        style_fig(fig, y_title="Products", height=280)
        fig.update_yaxes(range=[0, max(tiers["count"].max(), 1) * 1.2])
        show_chart(fig)

    with c4, st.container(key="card_rating_tier"):
        card_header("Rating tiers", "Poor < 3.0 · Average < 4.0 · Good < 4.5 · Excellent 4.5+")
        tiers = (
            df["rating_category"].value_counts().reindex(RATING_TIER_ORDER).fillna(0).reset_index()
        )
        tiers.columns = ["tier", "count"]
        fig = px.bar(
            tiers,
            x="tier",
            y="count",
            color="tier",
            color_discrete_map=RATING_TIER_COLORS,
            text="count",
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>%{y:,} products<extra></extra>",
        )
        style_fig(fig, y_title="Products", height=280)
        fig.update_yaxes(range=[0, max(tiers["count"].max(), 1) * 1.2])
        show_chart(fig)

    with st.container(key="card_scatter"):
        card_header("Price vs. rating", "Each dot is one product — hover for details")
        fig = px.scatter(
            df,
            x="price",
            y="rating_rate",
            color_discrete_sequence=[SEQUENTIAL_BLUE],
            custom_data=["product_name", "product_category", "rating_count"],
        )
        fig.update_traces(
            marker=dict(size=9, opacity=0.8, line=dict(width=2, color=SURFACE)),
            hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<br>"
            "$%{x:,.2f} · %{y:.1f} ★ (%{customdata[2]:,} reviews)<extra></extra>",
        )
        style_fig(fig, x_title="Price ($)", y_title="Rating (out of 5)", height=360)
        fig.update_xaxes(showgrid=True)
        fig.update_yaxes(range=[0, 5.2])
        show_chart(fig)


# ---------------------------------------------------------------------------
# Product activity — streamed user events (Trino) joined to the product catalog
# ---------------------------------------------------------------------------


@st.cache_data(ttl=30, show_spinner="Loading product activity from Trino...")
def load_product_activity() -> pd.DataFrame:
    return product_activity.load_activity(config.TRINO_HOST, config.TRINO_PORT, config.TRINO_USER)


def render_product_activity():
    page_header(
        "Dashboard",
        "Product activity",
        "What users do with each product: page views, clicks, cart changes and purchases "
        "streamed from Kafka into the Delta lakehouse, read through Trino "
        f"(<code>{esc(product_activity.VIEW)}</code>). Revenue and conversion come from the "
        "synthetic event producer (sample data).",
    )
    filter_summary()

    try:
        activity = load_product_activity()
    except Exception as e:  # noqa: BLE001 - Trino is optional for the rest of the app
        with st.container(key="card_trino_error"):
            card_header(
                "Can't read product activity from Trino",
                f"{config.TRINO_HOST}:{config.TRINO_PORT} · {product_activity.VIEW}",
            )
            st.code(f"{type(e).__name__}: {e}", language=None)
            st.markdown(
                "- Start Trino and register the tables (WSL): "
                '`bash "/mnt/f/Files/DE LAB/ETL-Pipeline/serving/week3_up.sh"`.\n'
                "- No `lakehouse.products` table or `v_product_activity` view yet: run "
                "`python streaming/load_products.py`, then `bash serving/register_tables.sh`.\n"
                "- `product_id` not found: restart `streaming/spark_delta.py` once (it adds the "
                "column), then restart Trino with `TRINO_ACTION=restart`.\n"
                "- Trino elsewhere: set `TRINO_HOST` / `TRINO_PORT` in `.env`."
            )
            st.button("Retry", type="primary", on_click=action_refresh, key="retry_trino")
        return

    dates = sorted(activity["event_date"].dropna().unique()) if not activity.empty else []
    date_col, _ = st.columns([1, 3])
    day = date_col.selectbox(
        "Event date",
        ["All dates", *[str(d) for d in reversed(dates)]],
        key="pa_event_date",
    )
    if day != "All dates":
        activity = activity[activity["event_date"].astype(str) == day]

    funnel = product_activity.product_funnel(activity, products_df)
    if has_data and len(filtered_df) < len(products_df):  # sidebar filters are active
        funnel = funnel[funnel["product_id"].isin(filtered_df["product_id"])]

    if funnel.empty:
        with st.container(key="card_pa_empty"):
            empty_state(
                "No product events yet. Start the producer and the Spark job "
                "(python streaming\\run_demo.py): shopping events now carry a product_id."
                if activity.empty
                else "No product events match the current filters."
            )
        return

    totals = funnel[product_activity.FUNNEL_EVENTS].sum()
    carts, purchases = int(totals["add_to_cart"]), int(totals["purchase"])
    best = funnel.iloc[0]
    kpi_grid(
        [
            ("Product events", f"{int(totals.sum()):,}", "Views, clicks, cart and purchases"),
            (
                "Products engaged",
                f"{len(funnel):,}",
                f"of {len(products_df):,} in the catalog" if has_data else "",
            ),
            ("Purchases", f"{purchases:,}", f"{carts:,} add-to-cart"),
            (
                "Revenue (sample)",
                fmt_money(funnel["revenue"].sum()),
                "Purchases × list price",
            ),
            (
                "Cart → purchase",
                f"{purchases / carts:.0%}" if carts else "—",
                "Purchases ÷ add-to-cart",
            ),
            (
                "Top seller",
                f"#{int(best['product_id'])}",
                esc(str(best["product_name"])[:34]),
            ),
        ]
    )

    cmap = (
        color_map
        if set(funnel["product_category"]) <= set(color_map)
        else category_color_map(set(color_map) | set(funnel["product_category"]))
    )

    left, right = st.columns([3, 2], gap="medium")
    with left, st.container(key="card_pa_top"):
        card_header("Top products by purchases", "Ten most purchased, colored by category")
        top = funnel.head(10).copy()
        top["label"] = top["product_name"].str.slice(0, 32) + "  #" + top["product_id"].astype(str)
        fig = px.bar(
            top,
            x="purchase",
            y="label",
            orientation="h",
            color="product_category",
            color_discrete_map=cmap,
            text="purchase",
            custom_data=["product_name", "page_view", "add_to_cart", "revenue"],
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{customdata[0]}</b><br>%{x:,} purchases · "
            "%{customdata[1]:,} views · %{customdata[2]:,} add-to-cart<br>"
            "Revenue $%{customdata[3]:,.2f}<extra></extra>",
        )
        style_fig(fig, x_title="Purchases", height=380, grid="x", legend=True)
        fig.update_yaxes(categoryorder="total ascending", title_text=None, automargin=True)
        fig.update_xaxes(range=[0, max(int(top["purchase"].max()), 1) * 1.2], automargin=True)
        show_chart(fig)

    with right, st.container(key="card_pa_revenue"):
        card_header("Revenue by category", "Purchases × list price (sample data)")
        rev = funnel.groupby("product_category", as_index=False)["revenue"].sum()
        fig = px.bar(
            rev,
            x="product_category",
            y="revenue",
            color="product_category",
            color_discrete_map=cmap,
            text_auto="$,.0f",
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<extra></extra>",
        )
        style_fig(fig, y_title="Revenue ($)", height=380)
        fig.update_xaxes(categoryorder="total descending", title_text=None, automargin=True)
        fig.update_yaxes(range=[0, max(float(rev["revenue"].max()), 1) * 1.2], automargin=True)
        show_chart(fig)

    with st.container(key="card_pa_funnel"):
        card_header(
            "Product funnel",
            "Events per product and step · cart → purchase can pass 100% with synthetic data",
        )
        st.dataframe(
            funnel,
            width="stretch",
            height=min(38 + 35 * len(funnel), 560),
            hide_index=True,
            column_config={
                "product_id": st.column_config.NumberColumn("ID", format="%d", width="small"),
                "product_name": st.column_config.TextColumn("Product", width="medium"),
                "product_category": st.column_config.TextColumn("Category", width="small"),
                "price": st.column_config.NumberColumn("Price", format="$%.2f"),
                "page_view": st.column_config.NumberColumn("Views", format="%d"),
                "click": st.column_config.NumberColumn("Clicks", format="%d"),
                "add_to_cart": st.column_config.NumberColumn("Add to cart", format="%d"),
                "remove_from_cart": st.column_config.NumberColumn("Removed", format="%d"),
                "purchase": st.column_config.NumberColumn("Purchases", format="%d"),
                "cart_to_purchase_pct": st.column_config.NumberColumn(
                    "Cart → purchase", format="%.0f%%"
                ),
                "revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
            },
        )


def render_gold():
    page_header(
        "Warehouse",
        "Gold layer",
        "Per-category aggregates from <code>gold_product_summary</code>, refreshed "
        "incrementally on every write. Not affected by the sidebar filters.",
    )
    if gold_df.empty:
        with st.container(key="card_gold_empty"):
            empty_state("gold_product_summary has no rows yet.")
        return

    top_rated = gold_df.loc[gold_df["average_rating"].idxmax()]
    priciest = gold_df.loc[gold_df["average_price"].idxmax()]
    kpi_grid(
        [
            ("Categories", f"{len(gold_df)}", "Rows in gold_product_summary"),
            (
                "Products (gold)",
                f"{int(gold_df['total_products'].sum()):,}",
                "Sum across categories",
            ),
            (
                "Top-rated category",
                str(top_rated["product_category"]),
                f"{top_rated['average_rating']:.2f} ★ average",
            ),
            (
                "Highest avg. price",
                str(priciest["product_category"]),
                f"{fmt_money(priciest['average_price'])} average",
            ),
        ]
    )

    g1, g2 = st.columns(2, gap="medium")
    with g1, st.container(key="card_gold_total"):
        card_header("Products per category")
        fig = px.bar(
            gold_df,
            x="total_products",
            y="product_category",
            orientation="h",
            color="product_category",
            color_discrete_map=color_map,
            text="total_products",
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>%{x:,} products<extra></extra>",
        )
        style_fig(fig, x_title="Products", height=300, grid="x")
        fig.update_yaxes(categoryorder="total ascending", title_text=None)
        fig.update_xaxes(range=[0, gold_df["total_products"].max() * 1.2])
        show_chart(fig)

    with g2, st.container(key="card_gold_rating"):
        card_header("Average rating per category", "Out of 5")
        fig = px.bar(
            gold_df,
            x="average_rating",
            y="product_category",
            orientation="h",
            color="product_category",
            color_discrete_map=color_map,
            text_auto=".2f",
        )
        fig.update_traces(
            textposition="outside",
            textfont_color=TEXT_SECONDARY,
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>%{x:.2f} ★<extra></extra>",
        )
        style_fig(fig, x_title="Average rating", height=300, grid="x")
        fig.update_yaxes(categoryorder="total ascending", title_text=None)
        fig.update_xaxes(range=[0, 5.4])
        show_chart(fig)

    with st.container(key="card_gold_table"):
        card_header("gold_product_summary", "Full table")
        st.dataframe(
            gold_df,
            width="stretch",
            hide_index=True,
            column_config={
                "product_category": st.column_config.TextColumn("Category"),
                "total_products": st.column_config.NumberColumn("Products", format="%d"),
                "average_price": st.column_config.NumberColumn("Avg. price", format="$%.2f"),
                "minimum_price": st.column_config.NumberColumn("Min. price", format="$%.2f"),
                "maximum_price": st.column_config.NumberColumn("Max. price", format="$%.2f"),
                "average_rating": st.column_config.ProgressColumn(
                    "Avg. rating", min_value=0, max_value=5, format="%.2f ★"
                ),
                "total_rating_count": st.column_config.NumberColumn("Reviews", format="%d"),
                "gold_load_timestamp": st.column_config.DatetimeColumn(
                    "Refreshed (UTC)", format="YYYY-MM-DD HH:mm"
                ),
            },
        )


CATALOG_SORTS = {
    "Newest first": ("silver_load_timestamp", False),
    "Price: high to low": ("price", False),
    "Price: low to high": ("price", True),
    "Rating: high to low": ("rating_rate", False),
    "Most reviewed": ("rating_count", False),
}

CATALOG_COLUMNS = [
    "image_url",
    "product_id",
    "product_name",
    "product_category",
    "price",
    "price_category",
    "rating_rate",
    "rating_count",
    "rating_category",
    "data_source",
    "etl_load_timestamp",
]


def render_catalog():
    page_header(
        "Data",
        "Product catalog",
        "Browse <code>silver_products</code>. Select a row to see the full product.",
    )
    filter_summary()
    if not require_data(filtered_df):
        return

    s1, s2 = st.columns([3, 1], gap="small")
    query = s1.text_input(
        "Search",
        placeholder="Search by product name or ID…",
        label_visibility="collapsed",
        icon=":material/search:",
    )
    sort_label = s2.selectbox("Sort", list(CATALOG_SORTS), label_visibility="collapsed")

    df = filtered_df
    if query.strip():
        q = query.strip().lower()
        df = df[
            df["product_name"].str.lower().str.contains(q, regex=False, na=False)
            | df["product_id"].astype(str).str.contains(q, regex=False)
        ]
    sort_col, ascending = CATALOG_SORTS[sort_label]
    df = df.sort_values(sort_col, ascending=ascending, na_position="last")

    head_l, head_r = st.columns([3, 1])
    head_l.caption(f"{len(df):,} product(s)")
    head_r.download_button(
        "Export CSV",
        df.drop(columns=["image_url"], errors="ignore").to_csv(index=False).encode("utf-8"),
        file_name="silver_products.csv",
        mime="text/csv",
        icon=":material/download:",
        width="stretch",
    )

    table_df = df[[c for c in CATALOG_COLUMNS if c in df.columns]].reset_index(drop=True)
    event = st.dataframe(
        table_df,
        width="stretch",
        height=520,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        row_height=44,
        column_config={
            "image_url": st.column_config.ImageColumn("", width="small"),
            "product_id": st.column_config.NumberColumn("ID", format="%d", width="small"),
            "product_name": st.column_config.TextColumn("Product", width="large"),
            "product_category": st.column_config.TextColumn("Category"),
            "price": st.column_config.NumberColumn("Price", format="$%.2f"),
            "price_category": st.column_config.TextColumn("Price tier"),
            "rating_rate": st.column_config.ProgressColumn(
                "Rating", min_value=0, max_value=5, format="%.1f ★"
            ),
            "rating_count": st.column_config.NumberColumn("Reviews", format="%d"),
            "rating_category": st.column_config.TextColumn("Rating tier"),
            "data_source": st.column_config.TextColumn("Source"),
            "etl_load_timestamp": st.column_config.DatetimeColumn(
                "Loaded (UTC)", format="YYYY-MM-DD HH:mm"
            ),
        },
    )

    selected = event.selection.rows if event and event.selection else []
    if selected:
        product = df.iloc[selected[0]]
        with st.container(key="card_product_detail"):
            img_col, info_col = st.columns([1, 4], gap="medium")
            if isinstance(product.get("image_url"), str) and product["image_url"].startswith(
                "http"
            ):
                img_col.image(product["image_url"], width="stretch")
            with info_col:
                card_header(
                    str(product["product_name"]),
                    f"#{product['product_id']} · {product['product_category']} · {product['data_source']}",
                )
                d1, d2, d3, d4 = st.columns(4)
                d1.metric(
                    "Price",
                    fmt_money(product["price"]),
                    product["price_category"],
                    delta_color="off",
                )
                d2.metric(
                    "Rating",
                    f"{product['rating_rate']:.1f} ★",
                    product["rating_category"],
                    delta_color="off",
                )
                d3.metric("Reviews", f"{int(product['rating_count']):,}")
                d4.metric("Loaded", time_ago(product["etl_load_timestamp"]))
                description = product.get("product_description") or "No description."
                st.markdown(
                    f'<div style="color:{TEXT_SECONDARY};font-size:13.5px;line-height:1.55">'
                    f"{esc(description)}</div>",
                    unsafe_allow_html=True,
                )


def render_add_product():
    page_header(
        "Ingest",
        "Add a product",
        "Runs through the same validate → transform → bronze → silver → gold code as the "
        "Kafka consumer, and appears on the dashboard immediately.",
        status_pills(),
    )

    last = st.session_state.get("last_saved")
    if last:
        with st.container(key="card_last_saved"):
            a, b = st.columns([5, 1])
            a.markdown(
                f"{status_badge('good', 'Saved to Databricks')} &nbsp;·&nbsp; "
                f"<b>#{last['product_id']} {esc(last['product_name'])}</b> — "
                f"{esc(last['product_category'])} · {fmt_money(last['price'])} "
                f"({esc(last['price_category'])}) · {last['rating_rate']:.1f} ★ "
                f"({esc(last['rating_category'])})",
                unsafe_allow_html=True,
            )
            if b.button("Dismiss", key="dismiss_saved", width="stretch"):
                st.session_state.pop("last_saved", None)
                st.rerun()

    form_col, info_col = st.columns([3, 2], gap="medium")

    with form_col, st.container(key="card_form"):
        card_header("Product details", "Fields marked * are required")
        with st.form("add_product_form", clear_on_submit=True, border=False):
            title = st.text_input(
                "Title *", placeholder="e.g. Wireless Noise-Cancelling Headphones"
            )
            r1a, r1b = st.columns(2)
            category_choice = r1a.selectbox("Category *", DEFAULT_CATEGORIES + ["Other…"])
            custom_category = r1b.text_input(
                "Custom category", placeholder="Only used when Category is “Other…”"
            )
            r2a, r2b = st.columns(2)
            price = r2a.number_input("Price (USD) *", min_value=0.0, step=0.5, format="%.2f")
            rating_count = r2b.number_input("Number of reviews", min_value=0, step=1, value=0)
            rating_rate = st.slider("Rating", 0.0, 5.0, 4.0, step=0.1, format="%.1f ★")
            description = st.text_area(
                "Description", height=110, placeholder="Short product description (optional)"
            )
            image = st.text_input("Image URL", value="https://i.imgur.com/placeholder.png")
            publish_kafka = st.checkbox(
                "Also publish the raw product to Kafka",
                value=False,
                disabled=not KAFKA_AVAILABLE,
                help=None if KAFKA_AVAILABLE else "Kafka broker not reachable right now.",
            )
            submitted = st.form_submit_button(
                "Save to Databricks", type="primary", icon=":material/save:", width="stretch"
            )

        if submitted:
            category = (custom_category if category_choice == "Other…" else category_choice).strip()
            if not title.strip():
                st.error("Please enter a product title.")
            elif not category:
                st.error("You chose “Other…” — please type a custom category.")
            else:
                raw = {
                    "id": next_product_id(products_df),
                    "title": title.strip(),
                    "price": price,
                    "category": category,
                    "description": description,
                    "image": image or "",
                    "rating": {"rate": rating_rate, "count": int(rating_count)},
                }
                with st.spinner("Writing to bronze → silver → gold…"):
                    ok, result = add_product(raw, publish_to_kafka=publish_kafka)
                if ok:
                    st.session_state["last_saved"] = result
                    flash(f"Product #{result['product_id']} saved to Databricks")
                    clear_dashboard_cache()
                    st.rerun()
                else:
                    st.error(result)

    with info_col:
        with st.container(key="card_rules"):
            card_header("How it will be classified", "Applied automatically by the transformer")
            price_rules = [
                ("Budget", "under $50"),
                ("Standard", "$50 – $99.99"),
                ("Premium", "$100 and up"),
            ]
            rating_rules = [
                ("Poor", "under 3.0"),
                ("Average", "3.0 – 3.9"),
                ("Good", "4.0 – 4.4"),
                ("Excellent", "4.5 and up"),
            ]
            st.markdown(
                "".join(
                    f'<div class="rule"><span><span class="swatch" style="background:'
                    f'{PRICE_TIER_COLORS[t]}"></span>{t}</span><span>{r}</span></div>'
                    for t, r in price_rules
                )
                + '<div style="height:10px"></div>'
                + "".join(
                    f'<div class="rule"><span><span class="swatch" style="background:'
                    f'{RATING_TIER_COLORS[t]}"></span>{t}</span><span>{r}</span></div>'
                    for t, r in rating_rules
                ),
                unsafe_allow_html=True,
            )
        with st.container(key="card_flow"):
            card_header("What happens when you save")
            flow = [
                "Validated — title, price and category are required",
                "Transformed — tiers assigned, category name normalized",
                "Inserted into <code>bronze_products</code> and <code>silver_products</code>",
                "<code>dim_category</code> / <code>fact_products</code> upserted",
                "That category's <code>gold_product_summary</code> row refreshed",
                "On failure: saved to <code>failed_records/</code> for replay",
            ]
            st.markdown(
                "".join(
                    f'<div class="flow-li"><span class="n">{i}</span><span>{s}</span></div>'
                    for i, s in enumerate(flow, 1)
                ),
                unsafe_allow_html=True,
            )


def render_pipeline():
    page_header(
        "Operations",
        "Pipeline",
        "End-to-end flow from the FakeStore API to this dashboard, with live row counts "
        "per layer and the local replay queue for failed Databricks writes.",
        status_pills(),
    )

    steps = [
        ("Extract", "FakeStore API", "source", ""),
        ("Validate", "Required fields", "schema check", ""),
        (
            "Kafka",
            esc(config.KAFKA_TOPIC or "topic"),
            "online" if KAFKA_AVAILABLE else "offline",
            "" if KAFKA_AVAILABLE else "warn",
        ),
        ("Transform", "Tiers & categories", "normalize", ""),
        ("Bronze", "Raw rows", f"{bronze_count:,}" if bronze_count is not None else "—", ""),
        ("Silver", "Clean rows", f"{len(products_df):,}", ""),
        ("Gold", "Category summary", f"{len(gold_df)} categories", ""),
        ("Dashboard", "You are here", "live", "here"),
    ]
    with st.container(key="card_stepper"):
        card_header("Data flow", "Batch and streaming paths converge on the same load code")
        st.markdown(
            '<div class="stepper">'
            + "".join(
                f'<div class="step"><div class="step-num {cls}">{i}</div>'
                f'<div class="step-name">{name}</div><div class="step-detail">{detail}</div>'
                f'<div class="step-stat">{stat}</div></div>'
                for i, (name, detail, stat, cls) in enumerate(steps, 1)
            )
            + "</div>",
            unsafe_allow_html=True,
        )

    kpi_grid(
        [
            (
                "bronze_products",
                f"{bronze_count:,}" if bronze_count is not None else "—",
                "Raw + classified rows",
            ),
            ("silver_products", f"{len(products_df):,}", "Cleaned rows (dashboard source)"),
            ("gold_product_summary", f"{len(gold_df)}", "One row per category"),
            (
                "Replay queue",
                f"{failed_count}",
                status_badge("good", "Nothing pending")
                if failed_count == 0
                else status_badge("warning", "Needs retry"),
            ),
        ]
    )

    q_col, cfg_col = st.columns([3, 2], gap="medium")
    with q_col, st.container(key="card_replay"):
        card_header("Replay queue", "Records whose Databricks write failed, saved locally")
        failed = load_failed_records()
        if not failed:
            st.markdown(
                status_badge("good", "All writes landed — nothing to replay."),
                unsafe_allow_html=True,
            )
        else:
            st.dataframe(
                pd.DataFrame(failed).reindex(
                    columns=["product_id", "product_name", "product_category", "price"]
                ),
                width="stretch",
                hide_index=True,
                column_config={
                    "product_id": st.column_config.NumberColumn("ID", format="%d"),
                    "product_name": "Product",
                    "product_category": "Category",
                    "price": st.column_config.NumberColumn("Price", format="$%.2f"),
                },
            )
        st.button(
            f"Retry {failed_count} failed record(s)",
            icon=":material/replay:",
            on_click=action_retry,
            disabled=failed_count == 0,
            type="primary",
        )

    with cfg_col, st.container(key="card_config"):
        card_header("Configuration", "Read from .env")
        rows = [
            ("Catalog", config.DATABRICKS_CATALOG),
            ("Schema", config.DATABRICKS_SCHEMA),
            ("Workspace", config.DATABRICKS_SERVER_HOSTNAME or "—"),
            ("Warehouse", (config.DATABRICKS_HTTP_PATH or "—").rsplit("/", 1)[-1]),
            ("Kafka bootstrap", config.KAFKA_BOOTSTRAP_SERVER or "—"),
            ("Kafka topic", config.KAFKA_TOPIC or "—"),
        ]
        st.markdown(
            "".join(
                f'<div class="kv"><span class="kv-k">{k}</span>'
                f'<span class="kv-v"><code>{esc(v)}</code></span></div>'
                for k, v in rows
            ),
            unsafe_allow_html=True,
        )
        if init_error:
            st.warning(f"Could not verify/create Databricks tables: {init_error}")


# ---------------------------------------------------------------------------
# How It Works — animated flow + dry-run record trace
# ---------------------------------------------------------------------------

FIXTURE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "scripts", "fixtures", "fakestore_products.json"
)
FACT_PRODUCTS_TABLE = table("fact_products")

HW_SLOT = 0.7  # seconds a packet spends on one wire of the animated diagram

# (title, detail, animation arrival slot). The Kafka topic ends lane A and
# starts lane B, so both of its boxes light up in the same slot.
HW_LANE_INGEST = [
    ("FakeStore API", "extract_products()", 0),
    ("Validate", "required fields", 1),
    ("Kafka producer", "+ batch_id, ingestion_time", 2),
    ("Kafka topic", config.KAFKA_TOPIC or "topic", 3),
]
HW_LANE_PROCESS = [
    ("Kafka topic", "etl_consumer_group", 3),
    ("Transform", "tiers + category names", 4),
    ("Bronze", "INSERT raw row", 5),
    ("Silver", "INSERT + timestamp", 6),
    ("dim / fact", "MERGE star schema", 7),
    ("Gold", "MERGE category row", 8),
    ("Dashboard", "this app", 9),
]
HW_CYCLE = (HW_LANE_PROCESS[-1][2] + 2) * HW_SLOT

TRACE_STAGES = [
    ("extract", "Extract", "extract/extractor.py · extract_products()"),
    ("validate", "Validate", "validation/validator.py · is_valid_product()"),
    ("produce", "Produce to Kafka", "kafka1/producer.py · send_product()"),
    ("consume", "Consume from Kafka", "kafka1/consumer.py · consume()"),
    ("transform", "Transform", "transform/transformer.py · transform_product()"),
    ("bronze", "Load bronze", "load/databricks_loader.py · load_to_staging()"),
    ("silver", "Load silver", "load/databricks_loader.py · load_to_staging()"),
    (
        "warehouse",
        "dim_category + fact_products",
        "staging/staging_manager.py · promote_to_warehouse()",
    ),
    ("gold", "Refresh gold", "staging/staging_manager.py · refresh_gold_summary()"),
    ("dashboard", "Show on dashboard", "streamlit_app.py · load_silver()"),
]

BROKEN_SAMPLES = {
    "Broken record — missing title": {
        "id": 9001,
        "title": "",
        "price": 19.99,
        "category": "electronics",
        "rating": {"rate": 4.1, "count": 12},
    },
    "Broken record — price is not a number": {
        "id": 9002,
        "title": "Mystery Box",
        "price": "twenty",
        "category": "electronics",
        "rating": {"rate": 3.2, "count": 4},
    },
}
CUSTOM_SAMPLE = "Custom JSON…"


def inject_how_it_works_css():
    """Styles for the animated diagram. Packet and glow keyframes share one
    cycle length; each wire/node is offset by its arrival slot, so a single
    packet appears to travel the whole pipeline in order."""
    travel = HW_SLOT / HW_CYCLE * 100
    glow = min(travel * 1.6, 99)
    st.markdown(
        f"""
        <style>
        .hw-lane-label {{ font-size: 11px; font-weight: 600; letter-spacing: .08em;
                          text-transform: uppercase; color: var(--muted); margin: 10px 0 8px; }}
        .hw-lane-label code {{ text-transform: none; letter-spacing: 0; }}
        .hw-lane {{ display: flex; align-items: center; overflow-x: auto; padding: 4px 2px 6px; }}
        .hw-node {{ flex: 0 0 132px; background: var(--surface-raised); border: 1px solid var(--border);
                    border-radius: 10px; padding: 10px 8px; text-align: center;
                    animation: hw-glow {HW_CYCLE:.2f}s linear infinite; }}
        .hw-node-t {{ font-size: 13px; font-weight: 600; color: var(--text); }}
        .hw-node-d {{ font-size: 11px; color: var(--muted); margin-top: 2px; font-family: ui-monospace, monospace; }}
        .hw-wire {{ flex: 1 0 28px; height: 2px; background: var(--border); position: relative; }}
        .hw-pkt {{ position: absolute; top: -4px; left: 0; width: 10px; height: 10px; border-radius: 50%;
                   background: var(--accent); box-shadow: 0 0 10px var(--accent); opacity: 0;
                   animation: hw-move {HW_CYCLE:.2f}s linear infinite; }}
        @keyframes hw-move {{
            0% {{ left: 0; opacity: 1; }}
            {travel:.3f}% {{ left: calc(100% - 10px); opacity: 1; }}
            {travel + 0.01:.3f}% {{ opacity: 0; }}
            100% {{ opacity: 0; }}
        }}
        @keyframes hw-glow {{
            0% {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(57,135,229,.25); }}
            {glow:.3f}% {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(57,135,229,.25); }}
            {glow + 0.01:.3f}% {{ border-color: var(--border); box-shadow: none; }}
            100% {{ border-color: var(--border); box-shadow: none; }}
        }}
        @media (prefers-reduced-motion: reduce) {{
            .hw-node, .hw-pkt {{ animation: none; }}
        }}
        .hw-branch {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; font-size: 12.5px;
                      color: var(--text-2); padding: 8px 12px; margin-top: 8px;
                      border: 1px dashed var(--border); border-radius: 10px; }}
        .hw-chip {{ background: var(--surface-raised); border: 1px solid var(--border); border-radius: 8px;
                    padding: 3px 9px; font-size: 12px; color: var(--text); white-space: nowrap; }}
        .hw-arrow {{ color: var(--muted); }}

        .tr-steps {{ display: flex; gap: 6px; flex-wrap: wrap; margin: 4px 0 14px; }}
        .tr-step {{ font-size: 12px; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border);
                    color: var(--muted); background: var(--surface); white-space: nowrap; }}
        .tr-step.done {{ color: var(--text); border-color: rgba(12,163,12,.55); }}
        .tr-step.failed {{ color: var(--text); border-color: var(--crit); }}
        .tr-step.running {{ color: var(--text); border-color: var(--accent);
                            box-shadow: 0 0 0 3px rgba(57,135,229,.25); }}
        .tr-head {{ display: flex; align-items: center; gap: 12px; margin-bottom: 6px; }}
        .tr-num {{ flex: 0 0 30px; height: 30px; border-radius: 50%; display: flex; align-items: center;
                   justify-content: center; font-weight: 700; font-size: 13px; color: var(--text);
                   background: var(--surface-raised); border: 2px solid var(--accent); }}
        .tr-num.failed {{ border-color: var(--crit); }}
        .tr-num.skipped {{ border-color: var(--border); color: var(--muted); }}
        .tr-name {{ font-size: 15px; font-weight: 600; color: var(--text); }}
        .tr-code {{ font-size: 11.5px; color: var(--muted); font-family: ui-monospace, monospace; }}
        .tr-what {{ font-size: 13px; color: var(--text-2); line-height: 1.55; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def hw_lane_html(nodes) -> str:
    parts = []
    for i, (title, detail, slot) in enumerate(nodes):
        if i:
            # The packet leaves the previous node in the slot before this one arrives.
            parts.append(
                f'<div class="hw-wire"><span class="hw-pkt" '
                f'style="animation-delay:{(slot - 1) * HW_SLOT:.2f}s"></span></div>'
            )
        parts.append(
            f'<div class="hw-node" style="animation-delay:{slot * HW_SLOT:.2f}s">'
            f'<div class="hw-node-t">{esc(title)}</div><div class="hw-node-d">{esc(detail)}</div></div>'
        )
    return f'<div class="hw-lane">{"".join(parts)}</div>'


def hw_chain(*chips) -> str:
    arrow = '<span class="hw-arrow">&rarr;</span>'
    return arrow.join(f'<span class="hw-chip">{c}</span>' for c in chips)


@st.cache_data(show_spinner=False)
def load_trace_samples() -> list:
    try:
        with open(FIXTURE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return [
            {
                "id": 1,
                "title": "Fjallraven - Foldsack No. 1 Backpack, Fits 15 Laptops",
                "price": 109.95,
                "description": "Your perfect pack for everyday use and walks in the forest.",
                "category": "men's clothing",
                "image": "https://fakestoreapi.com/img/81fPKd-2AYL._AC_SL1500_.jpg",
                "rating": {"rate": 3.9, "count": 120},
            }
        ]


@st.cache_data(ttl=30, show_spinner=False)
def load_dim_category() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(f"SELECT * FROM {DIM_CATEGORY_TABLE}", conn)
    finally:
        conn.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_fact_product_ids() -> set:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT product_id FROM {FACT_PRODUCTS_TABLE}")
        return {int(row[0]) for row in cur.fetchall()}
    finally:
        conn.close()


def jsonable(value):
    """Round-trip through JSON so datetimes render in st.json."""
    return json.loads(json.dumps(value, default=str))


def price_rule(price: float) -> str:
    return "under $50" if price < 50 else "$50 – $99.99" if price < 100 else "$100 and up"


def rating_rule(rate: float) -> str:
    if rate < 3.0:
        return "under 3.0"
    if rate < 4.0:
        return "3.0 – 3.9"
    if rate < 4.5:
        return "4.0 – 4.4"
    return "4.5 and up"


def enrich_like_producer(product: dict) -> dict:
    """Same metadata kafka1.producer.enrich_product() stamps before sending.
    Re-implemented here so the dry run never imports a live Kafka client."""
    enriched = product.copy()
    enriched["batch_id"] = str(uuid.uuid4())
    enriched["ingestion_time"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    return enriched


def gold_row_from(rows: pd.DataFrame) -> dict:
    """Mirrors the aggregate refresh_gold_summary() computes over silver."""
    if rows.empty:
        return {}
    return {
        "total_products": int(len(rows)),
        "average_price": float(rows["price"].mean()),
        "minimum_price": float(rows["price"].min()),
        "maximum_price": float(rows["price"].max()),
        "average_rating": float(rows["rating_rate"].mean()),
        "total_rating_count": int(rows["rating_count"].sum()),
    }


def build_trace(raw: dict) -> dict:
    """Runs one record through the real validate/transform code and computes
    what every downstream write WOULD do, without writing anything."""
    trace = {"raw": raw, "failed_at": None, "error": None}

    checks = []
    for field in REQUIRED_FIELDS:
        present = field in raw and raw[field] not in (None, "")
        checks.append((f"`{field}` is present and not empty", present, repr(raw.get(field, "—"))))
    if "price" in raw:
        try:
            float(raw["price"])
            numeric = True
        except (TypeError, ValueError):
            numeric = False
        checks.append(("`price` parses as a number", numeric, repr(raw["price"])))
    trace["checks"] = checks

    if not is_valid_product(raw):
        trace["failed_at"] = "validate"
        return trace

    message = enrich_like_producer(raw)
    wire = json.dumps(message).encode("utf-8")
    trace["message"] = message
    trace["wire_bytes"] = len(wire)
    consumed = json.loads(wire.decode("utf-8"))
    trace["consumed"] = consumed

    try:
        transformed = transform_product(consumed)
    except Exception as e:  # noqa: BLE001 - surfaced in the trace, not raised
        trace["failed_at"] = "transform"
        trace["error"] = str(e)
        return trace
    trace["transformed"] = transformed

    trace["bronze"] = {c: transformed[c] for c in BRONZE_COLUMNS}
    now = datetime.now(timezone.utc)
    trace["silver"] = {
        c: now if c == "silver_load_timestamp" else transformed[c] for c in SILVER_COLUMNS
    }

    category = transformed["product_category"]
    try:
        dim = load_dim_category()
        match = dim[dim["product_category"] == category]
        if not match.empty:
            trace["category_id"], trace["category_new"] = int(match["category_id"].iloc[0]), False
        else:
            next_id = int(dim["category_id"].max()) + 1 if not dim.empty else 1
            trace["category_id"], trace["category_new"] = next_id, True
    except Exception:  # noqa: BLE001
        trace["category_id"], trace["category_new"] = None, None
    try:
        trace["fact_exists"] = transformed["product_id"] in load_fact_product_ids()
    except Exception:  # noqa: BLE001
        trace["fact_exists"] = None
    trace["fact"] = {
        c: (trace["category_id"] if c == "category_id" else transformed[c]) for c in FACT_COLUMNS
    }

    in_category = (
        products_df[products_df["product_category"] == category] if has_data else pd.DataFrame()
    )
    new_row = pd.DataFrame([trace["silver"]])[["price", "rating_rate", "rating_count"]]
    trace["gold_before"] = gold_row_from(in_category)
    trace["gold_after"] = gold_row_from(
        pd.concat([in_category[["price", "rating_rate", "rating_count"]], new_row])
        if not in_category.empty
        else new_row
    )
    trace["category"] = category
    return trace


def trace_statuses(trace: dict) -> dict:
    keys = [k for k, _, _ in TRACE_STAGES]
    if not trace["failed_at"]:
        return {k: "done" for k in keys}
    stop = keys.index(trace["failed_at"])
    return {
        k: "done" if i < stop else "failed" if i == stop else "skipped" for i, k in enumerate(keys)
    }


def trace_steps_html(statuses: dict, running: str | None = None) -> str:
    icons = {"done": "&#10003;", "failed": "&#10005;", "skipped": "&ndash;", "pending": "&middot;"}
    chips = []
    for key, name, _ in TRACE_STAGES:
        state = "running" if key == running else statuses.get(key, "pending")
        icon = "&#9679;" if state == "running" else icons.get(state, "")
        chips.append(f'<span class="tr-step {state}">{icon} {esc(name)}</span>')
    return f'<div class="tr-steps">{"".join(chips)}</div>'


def render_trace_stage(i: int, key: str, name: str, code: str, status: str, trace: dict):
    num_cls = {"failed": "failed", "skipped": "skipped"}.get(status, "")
    badge = {
        "done": status_badge("good", "Done"),
        "failed": status_badge("critical", "Stopped here"),
        "skipped": f'<span style="color:{TEXT_MUTED}">Not reached</span>',
    }[status]
    with st.container(key=f"card_trace_{key}"):
        st.markdown(
            f'<div class="tr-head"><div class="tr-num {num_cls}">{i}</div>'
            f'<div style="flex:1"><div class="tr-name">{esc(name)}</div>'
            f'<div class="tr-code">{esc(code)}</div></div><div style="font-size:13px">{badge}</div></div>',
            unsafe_allow_html=True,
        )
        if status == "skipped":
            return
        what, data = st.columns([2, 3], gap="medium")
        what_md, show = trace_stage_content(key, trace)
        what.markdown(f'<div class="tr-what">{what_md}</div>', unsafe_allow_html=True)
        with data:
            show()


def trace_stage_content(key: str, trace: dict):
    """Returns (explanation_html, callable that renders the stage's data)."""
    t = trace.get("transformed") or {}

    if key == "extract":
        return (
            "The orchestrator (Airflow DAG <code>fakestore_pipeline</code>, daily — or "
            "<code>run_pipeline_no_airflow.py</code>) calls "
            f"<code>GET {esc(config.FAKESTORE_API_URL)}</code>. This is one record as the API "
            "returns it.",
            lambda: st.json(jsonable(trace["raw"]), expanded=True),
        )

    if key == "validate":
        passed = trace["failed_at"] != "validate"
        verdict = (
            status_badge("good", "Valid — continues to Kafka.")
            if passed
            else status_badge(
                "critical",
                "Invalid — validate_products() drops it and logs a warning. It never reaches Kafka.",
            )
        )
        return (
            "Every record needs a non-empty <code>id</code>, <code>title</code>, <code>price</code> "
            f"and <code>category</code>, and the price must parse as a number.<br><br>{verdict}",
            lambda: st.dataframe(
                pd.DataFrame(
                    [
                        {"Check": c, "Result": "✓ pass" if ok else "✕ fail", "Value": v}
                        for c, ok, v in trace["checks"]
                    ]
                ),
                hide_index=True,
                width="stretch",
            ),
        )

    if key == "produce":
        return (
            "The producer stamps ETL metadata (<code>batch_id</code>, <code>ingestion_time</code>), "
            "serializes the record to UTF-8 JSON "
            f"(<b>{trace['wire_bytes']:,} bytes</b>) and sends it to topic "
            f"<code>{esc(config.KAFKA_TOPIC or '—')}</code>. The broker assigns the partition and "
            "offset.",
            lambda: st.json(jsonable(trace["message"]), expanded=False),
        )

    if key == "consume":
        return (
            "The consumer (group <code>etl_consumer_group</code>, reading from the earliest "
            "offset) receives the bytes and <code>json.loads()</code> them back into a dict — "
            "identical to what was sent.",
            lambda: st.json(jsonable(trace["consumed"]), expanded=False),
        )

    if key == "transform":
        if trace["failed_at"] == "transform":
            return (
                f"transform_product() raised an error: <code>{esc(trace['error'])}</code>",
                lambda: None,
            )
        raw = trace["consumed"]
        rating = raw.get("rating") or {}
        rows = [
            ("product_name", repr(raw["title"]), "whitespace trimmed", t["product_name"]),
            (
                "product_category",
                repr(raw["category"]),
                "normalize_category()",
                t["product_category"],
            ),
            ("price_category", f"${t['price']:,.2f}", price_rule(t["price"]), t["price_category"]),
            (
                "product_value_segment",
                f"${t['price']:,.2f}",
                price_rule(t["price"]),
                t["product_value_segment"],
            ),
            (
                "rating_category",
                str(rating.get("rate", "—")),
                rating_rule(t["rating_rate"]),
                t["rating_category"],
            ),
            ("etl_load_timestamp", "—", "now, UTC", str(t["etl_load_timestamp"])[:19]),
        ]

        def show():
            st.dataframe(
                pd.DataFrame(rows, columns=["Output column", "Input", "Rule", "Result"]),
                hide_index=True,
                width="stretch",
            )
            with st.expander("Full transformed record"):
                st.json(jsonable(t))

        return (
            "The raw record is flattened (nested <code>rating</code> becomes "
            "<code>rating_rate</code> / <code>rating_count</code>), the category name is "
            "normalized, and price / rating tiers are derived:",
            show,
        )

    if key == "bronze":
        return (
            f"One <code>INSERT</code> into <code>{esc(BRONZE_TABLE)}</code>: the record plus its "
            "classifications, exactly as transformed.",
            lambda: st.dataframe(pd.DataFrame([trace["bronze"]]), hide_index=True, width="stretch"),
        )

    if key == "silver":
        return (
            f"The same row is inserted into <code>{esc(SILVER_TABLE)}</code> with a "
            "<code>silver_load_timestamp</code>, in the same transaction. If either insert throws, "
            "the product is saved to <code>failed_records/failed_products.json</code> and the "
            "replay task retries it later.",
            lambda: st.dataframe(pd.DataFrame([trace["silver"]]), hide_index=True, width="stretch"),
        )

    if key == "warehouse":
        cid, new = trace.get("category_id"), trace.get("category_new")
        dim_text = (
            "Couldn't read <code>dim_category</code> for this preview."
            if cid is None
            else f"Category <b>{esc(t['product_category'])}</b> is new — it would get "
            f"<code>category_id = {cid}</code>."
            if new
            else f"Category <b>{esc(t['product_category'])}</b> already exists as "
            f"<code>category_id = {cid}</code>."
        )
        exists = trace.get("fact_exists")
        fact_text = (
            "Couldn't read <code>fact_products</code> for this preview."
            if exists is None
            else f"<code>product_id {t['product_id']}</code> is already in "
            "<code>fact_products</code>, so the MERGE <b>updates</b> that row."
            if exists
            else f"<code>product_id {t['product_id']}</code> is new, so the MERGE <b>inserts</b> it."
        )
        return (
            f"{dim_text}<br><br>{fact_text}",
            lambda: st.dataframe(pd.DataFrame([trace["fact"]]), hide_index=True, width="stretch"),
        )

    if key == "gold":
        before, after = trace["gold_before"], trace["gold_after"]
        metrics = [
            ("Products", "total_products", "{:,.0f}"),
            ("Average price", "average_price", "${:,.2f}"),
            ("Min. price", "minimum_price", "${:,.2f}"),
            ("Max. price", "maximum_price", "${:,.2f}"),
            ("Average rating", "average_rating", "{:.3f}"),
            ("Total reviews", "total_rating_count", "{:,.0f}"),
        ]
        rows = [
            {
                "Metric": label,
                "Before": fmt.format(before[k]) if k in before else "—",
                "After": fmt.format(after[k]),
                "Changed": "●" if round(before.get(k, -1), 6) != round(after[k], 6) else "",
            }
            for label, k, fmt in metrics
        ]
        return (
            f"The <b>{esc(trace['category'])}</b> row of <code>gold_product_summary</code> is "
            "recomputed from silver (COUNT, AVG, MIN, MAX, SUM) and MERGEd in. Only this "
            "category's row changes.",
            lambda: st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch"),
        )

    # dashboard
    return (
        "The dashboard reads <code>silver_products</code> through a 30-second cache that is "
        "cleared after every write from this app, so the product shows up on the Overview, "
        "Analytics and Catalog pages.",
        lambda: kpi_grid(
            [
                ("Silver rows now", f"{len(products_df):,}", "before this record"),
                ("After the write", f"{len(products_df) + 1:,}", "+1 row"),
            ]
        ),
    )


def render_how_it_works():
    inject_how_it_works_css()
    page_header(
        "Walkthrough",
        "How the pipeline works",
        "Watch a record travel from the FakeStore API to this dashboard, then trace one "
        "yourself and inspect the data after every stage.",
    )

    with st.container(key="card_hw_diagram"):
        card_header("Live data flow", "One packet = one product moving through the pipeline")
        st.markdown(
            '<div class="hw-lane-label">1 · Ingest — Airflow DAG <code>fakestore_pipeline</code> '
            "or <code>run_pipeline_no_airflow.py</code></div>"
            + hw_lane_html(HW_LANE_INGEST)
            + '<div class="hw-lane-label">2 · Process — <code>kafka1/consumer.py</code> '
            "into Databricks Unity Catalog</div>"
            + hw_lane_html(HW_LANE_PROCESS)
            + '<div class="hw-branch"><b style="color:var(--warn)">&#9888; On a failed write</b>'
            + hw_chain("Bronze / Silver INSERT throws", "failed_records/", "replay task", "Bronze")
            + "</div>"
            + '<div class="hw-branch"><b style="color:var(--accent)">&#10010; Manual entry</b>'
            + hw_chain("Add Product page", "Validate", "Transform", "Bronze")
            + '<span style="color:var(--muted)">skips Kafka · optional publish</span></div>',
            unsafe_allow_html=True,
        )

    # ---------------- Trace a record ----------------
    samples = load_trace_samples()
    sample_labels = {
        f"#{p['id']} · {p['title'][:48]} ({p['category']}, ${p['price']})": p for p in samples
    }
    options = [*sample_labels, *BROKEN_SAMPLES, CUSTOM_SAMPLE]

    with st.container(key="card_hw_controls"):
        card_header(
            "Trace a record",
            "Dry run: uses the real validate and transform code, but sends nothing to Kafka "
            "and writes nothing to Databricks.",
        )
        c1, c2, c3 = st.columns([4, 1.2, 1.2], vertical_alignment="bottom")
        choice = c1.selectbox("Record to trace", options, key="hw_choice")
        animate = c2.toggle("Animate", value=True, key="hw_animate")
        run = c3.button("Run trace", type="primary", icon=":material/play_arrow:", width="stretch")

        custom_text = None
        if choice == CUSTOM_SAMPLE:
            custom_text = st.text_area(
                "Record JSON (FakeStore API shape)",
                value=json.dumps(samples[0], indent=2),
                height=220,
                key="hw_custom",
            )

    if run:
        raw = None
        if choice == CUSTOM_SAMPLE:
            try:
                raw = json.loads(custom_text or "")
                if not isinstance(raw, dict):
                    st.error('The JSON must be a single object, e.g. {"id": 1, "title": …}.')
                    raw = None
            except json.JSONDecodeError as e:
                st.error(f"That isn't valid JSON: {e}")
        else:
            raw = sample_labels.get(choice) or BROKEN_SAMPLES[choice]
        if raw is not None:
            st.session_state["hw_trace"] = {"label": choice, "trace": build_trace(raw)}
        else:
            st.session_state.pop("hw_trace", None)  # don't leave a stale trace under the error

    state = st.session_state.get("hw_trace")
    if not state:
        with st.container(key="card_hw_empty"):
            empty_state("Pick a record and press “Run trace” to follow it stage by stage.")
        render_other_paths()
        return

    trace = state["trace"]
    statuses = trace_statuses(trace)
    st.caption(f"Tracing: {state['label']}")
    steps_slot = st.empty()

    for i, (key, name, code) in enumerate(TRACE_STAGES, 1):
        if run and animate and statuses[key] != "skipped":
            shown = {k: statuses[k] for k, _, _ in TRACE_STAGES[: i - 1]}
            steps_slot.markdown(trace_steps_html(shown, running=key), unsafe_allow_html=True)
            time.sleep(0.45)
        render_trace_stage(i, key, name, code, statuses[key], trace)
    steps_slot.markdown(trace_steps_html(statuses), unsafe_allow_html=True)

    if trace["failed_at"]:
        st.error(
            f"The record stopped at **{dict((k, n) for k, n, _ in TRACE_STAGES)[trace['failed_at']]}** "
            "— later stages were never reached, so nothing about it would reach Databricks."
        )
    else:
        st.success(
            "The record made it all the way through — this is exactly what a real run writes."
        )

    render_other_paths()


def render_other_paths():
    st.markdown(
        '<div class="sb-section" style="margin-top:22px">Other paths</div>', unsafe_allow_html=True
    )
    p1, p2, p3 = st.columns(3, gap="medium")
    with p1, st.container(key="card_hw_manual"):
        card_header("Manual entry")
        st.markdown(
            '<div class="tr-what">'
            + hw_chain("Add Product", "Validate", "Transform", "Bronze → Gold")
            + "<br><br>The <b>Add Product</b> page calls the same functions synchronously, "
            "skipping Kafka, so the product is on the dashboard as soon as you save. "
            "It can also publish the raw record to Kafka.</div>",
            unsafe_allow_html=True,
        )
        st.page_link(PAGE_ADD, label="Add a product", icon=":material/add_circle:")
    with p2, st.container(key="card_hw_replay"):
        card_header("Failure & replay")
        st.markdown(
            '<div class="tr-what">'
            + hw_chain("INSERT fails", "failed_records/", "Retry")
            + "<br><br>A failed bronze/silver write never loses the record: it's appended to "
            "<code>failed_records/failed_products.json</code>. The DAG's "
            "<code>replay_failed</code> task, or <b>Retry</b> on the Pipeline page, "
            "loads it again.</div>",
            unsafe_allow_html=True,
        )
        st.page_link(PAGE_PIPELINE, label="Open the replay queue", icon=":material/replay:")
    with p3, st.container(key="card_hw_stream"):
        card_header("Streaming lakehouse (user events)")
        st.markdown(
            '<div class="tr-what">'
            + hw_chain("streaming/producer.py", "user-events", "Spark", "Delta")
            + "<br><br>A separate local demo: Spark Structured Streaming reads "
            "<code>user-events</code> in micro-batches, validates each event with "
            "<code>from_json</code>, MERGEs valid rows into a Delta table partitioned by "
            "<code>event_date</code> and quarantines rejects. Checkpoint offsets give "
            "exactly-once writes. Shopping events carry a <code>product_id</code>, joined to "
            "<code>lakehouse.products</code> in Trino for the <b>Products</b> page. "
            "Run it with <code>python streaming\\run_demo.py</code>.</div>",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

PAGE_OVERVIEW = st.Page(
    render_overview,
    title="Overview",
    icon=":material/dashboard:",
    url_path="overview",
    default=True,
)
PAGE_ANALYTICS = st.Page(
    render_analytics, title="Analytics", icon=":material/insights:", url_path="analytics"
)
PAGE_PRODUCTS = st.Page(
    render_product_activity,
    title="Products",
    icon=":material/shopping_cart:",
    url_path="product-activity",
)
PAGE_GOLD = st.Page(
    render_gold, title="Gold Layer", icon=":material/workspace_premium:", url_path="gold"
)
PAGE_CATALOG = st.Page(
    render_catalog, title="Catalog", icon=":material/table_view:", url_path="catalog"
)
PAGE_ADD = st.Page(
    render_add_product, title="Add Product", icon=":material/add_circle:", url_path="add-product"
)
PAGE_PIPELINE = st.Page(
    render_pipeline, title="Pipeline", icon=":material/account_tree:", url_path="pipeline"
)
PAGE_MONITORING = st.Page(
    render_monitoring,
    title="Monitoring",
    icon=":material/monitor_heart:",
    url_path="monitoring",
)
PAGE_HOW = st.Page(
    render_how_it_works,
    title="How It Works",
    icon=":material/play_circle:",
    url_path="how-it-works",
)

st.navigation(
    [
        PAGE_OVERVIEW,
        PAGE_ANALYTICS,
        PAGE_PRODUCTS,
        PAGE_GOLD,
        PAGE_CATALOG,
        PAGE_ADD,
        PAGE_PIPELINE,
        PAGE_MONITORING,
        PAGE_HOW,
    ],
    position="top",
).run()
