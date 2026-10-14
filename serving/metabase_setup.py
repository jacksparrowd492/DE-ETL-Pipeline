"""
Week 3 - build the BI layer in Metabase, served by Trino.

  python3 serving/metabase_setup.py            # create (or reuse) everything
  python3 serving/metabase_setup.py --rebuild  # delete and recreate the dashboard

What it does (Metabase REST API, standard library only):
  1. First run: creates the Metabase admin account (you type email/password).
     Later runs: logs in with the same account.
  2. Adds the Trino database connection  (driver: Starburst/Trino,
     host localhost:8080, catalog delta, schema lakehouse).
  3. Creates SQL questions that run on Trino and an interactive dashboard
     "User Events - Trino Lakehouse" with Event Date and Event Type filters,
     including a Product activity section (v_product_activity: events joined
     to lakehouse.products).
  Re-running on an existing Metabase re-syncs the Trino schema, so new
  columns/tables/views show up in Metabase's data browser too.

Credentials are never stored in the project; they are only typed at the prompt.
"""

import argparse
import getpass
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

MB = "http://localhost:3000"
DB_NAME = "Trino Lakehouse"
DASH_NAME = "User Events - Trino Lakehouse"
T = "delta.lakehouse.user_events"
R = "delta.lakehouse.user_events_rejected"
P = "delta.lakehouse.v_product_activity"  # serving/views.sql
GREEN, RED, BLUE = "#3A9D5D", "#D64550", "#2F6FB3"
EVENT_COLORS = {
    "purchase": GREEN,
    "page_view": "#1F4E8C",
    "click": "#4C8ED9",
    "login": "#2F6FB3",
    "logout": "#8DB8EC",
    "add_to_cart": "#5FA2E8",
    "remove_from_cart": "#A9C9F0",
}
FILTERS = "WHERE 1=1 [[AND event_date = {{event_date}}]] [[AND event = {{event}}]]"

session = None
REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "metabase_setup.log")


def api(method, path, body=None):
    req = urllib.request.Request(MB + path, method=method)
    req.add_header("Content-Type", "application/json")
    if session:
        req.add_header("X-Metabase-Session", session)
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data, timeout=120) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        sys.exit(f"!! {method} {path} -> {e.code}: {e.read().decode()[:500]}")


def login():
    global session
    props = api("GET", "/api/session/properties")
    if not props.get("has-user-setup"):
        print("== First run: create the Metabase admin account (local only) ==")
        email = input("Admin email: ").strip()
        pw = getpass.getpass("Admin password (min 8 chars, letters+numbers): ")
        props = api("GET", "/api/session/properties")
        if props.get("has-user-setup"):  # account was created meanwhile (e.g. in the browser)
            session = api("POST", "/api/session", {"username": email, "password": pw})["id"]
            print(">> logged in to Metabase")
            return
        res = api(
            "POST",
            "/api/setup",
            {
                "token": props["setup-token"],
                "user": {
                    "first_name": "Admin",
                    "last_name": "User",
                    "email": email,
                    "password": pw,
                    "site_name": "ETL Lakehouse",
                },
                "prefs": {"site_name": "ETL Lakehouse", "allow_tracking": False},
            },
        )
        session = res["id"]
    else:
        email = input("Metabase email: ").strip()
        pw = getpass.getpass("Metabase password: ")
        session = api("POST", "/api/session", {"username": email, "password": pw})["id"]
    print(">> logged in to Metabase")


def trino_database():
    dbs = api("GET", "/api/database")
    dbs = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
    for d in dbs:
        if d["name"] == DB_NAME:
            print(f">> reusing database '{DB_NAME}' (id {d['id']}), re-syncing its schema")
            api("POST", f"/api/database/{d['id']}/sync_schema")
            return d["id"]
    d = api(
        "POST",
        "/api/database",
        {
            "engine": "starburst",
            "name": DB_NAME,
            "details": {
                "host": "localhost",
                "port": 8080,
                "catalog": "delta",
                "schema": "lakehouse",
                "user": "metabase",
                "ssl": False,
            },
            "is_full_sync": True,
        },
    )
    api("POST", f"/api/database/{d['id']}/sync_schema")
    print(f">> added Trino database (id {d['id']})")
    return d["id"]


def tag(name, display, kind):
    return {"id": str(uuid.uuid4()), "name": name, "display-name": display, "type": kind}


def card(db, name, sql, display, viz=None, filters=True):
    tags = (
        {
            "event_date": tag("event_date", "Event Date", "date"),
            "event": tag("event", "Event Type", "text"),
        }
        if filters
        else {}
    )
    c = api(
        "POST",
        "/api/card",
        {
            "name": name,
            "display": display,
            "visualization_settings": viz or {},
            "dataset_query": {
                "type": "native",
                "database": db,
                "native": {"query": sql, "template-tags": tags},
            },
        },
    )
    res = api("POST", f"/api/card/{c['id']}/query")
    ok = res.get("status") == "completed"
    rows = len(res.get("data", {}).get("rows", [])) if ok else 0
    line = f"{'OK ' if ok else 'ERR'} card {c['id']:>3} {name!r}: " + (
        f"{rows} rows" if ok else str(res.get("error"))[:300]
    )
    print("   " + line)
    with open(REPORT, "a") as fh:
        fh.write(line + "\n")
    return c["id"], filters


def build(db, rebuild):
    for d in api("GET", "/api/dashboard"):
        if d["name"] == DASH_NAME:
            if not rebuild:
                print(f">> dashboard exists: {MB}/dashboard/{d['id']}  (use --rebuild to recreate)")
                return d["id"]
            full = api("GET", f"/api/dashboard/{d['id']}")
            for dc in full.get("dashcards", []):
                if dc.get("card_id"):
                    api("PUT", f"/api/card/{dc['card_id']}", {"archived": True})
            api("DELETE", f"/api/dashboard/{d['id']}")

    def bar(dim, met):
        return {
            "graph.dimensions": [dim],
            "graph.metrics": [met],
            "graph.show_values": True,
            "graph.x_axis.title_text": "",
            "graph.y_axis.title_text": "Events",
        }

    def pie(dim, met):
        return {
            "pie.dimension": dim,
            "pie.metric": met,
            "pie.show_legend": True,
            "pie.percent_visibility": "inside",
        }

    W = FILTERS  # "WHERE 1=1 [[AND event_date = ..]] [[AND event = ..]]"
    lat = "to_unixtime(ingested_at) - to_unixtime(event_time)"
    cards = [  # (card, col, row, size_x, size_y)
        # --- row 0: volume KPIs
        (
            card(db, "Total Events", f"SELECT count(*) AS total_events FROM {T} {W}", "scalar"),
            0,
            0,
            6,
            3,
        ),
        (
            card(
                db,
                "Unique Users",
                f"SELECT count(DISTINCT user_id) AS unique_users FROM {T} {W}",
                "scalar",
            ),
            6,
            0,
            6,
            3,
        ),
        (
            card(
                db,
                "Distinct Event Types",
                f"SELECT count(DISTINCT event) AS event_types FROM {T} {W}",
                "scalar",
            ),
            12,
            0,
            6,
            3,
        ),
        (
            card(
                db,
                "Avg Events per User",
                f"SELECT round(CAST(count(*) AS double) / nullif(count(DISTINCT user_id), 0), 1) AS avg_events_per_user FROM {T} {W}",
                "scalar",
            ),
            18,
            0,
            6,
            3,
        ),
        # --- row 3: business + pipeline-health KPIs
        (
            card(
                db,
                "Purchase Rate (sample data)",
                f"SELECT round(100.0 * count_if(event = 'purchase') / nullif(count_if(event = 'add_to_cart'), 0), 0) AS purchase_rate_pct FROM {T} {W}",
                "scalar",
                {"scalar.suffix": " %"},
            ),
            0,
            3,
            6,
            3,
        ),
        (
            card(
                db,
                "Cart Removal (sample data)",
                f"SELECT round(100.0 * count_if(event = 'remove_from_cart') / nullif(count_if(event = 'add_to_cart'), 0), 0) AS cart_removal_pct FROM {T} {W}",
                "scalar",
                {"scalar.suffix": " %"},
            ),
            6,
            3,
            6,
            3,
        ),
        (
            card(
                db,
                "Data Quality (% valid messages)",
                f"SELECT round(100.0 * v / nullif(v + r, 0), 1) AS valid_pct FROM (SELECT count(*) AS v FROM {T}) CROSS JOIN (SELECT count(*) AS r FROM {R})",
                "scalar",
                {"scalar.suffix": " %"},
                filters=False,
            ),
            12,
            3,
            6,
            3,
        ),
        (
            card(
                db,
                "Ingestion Latency p95 (seconds)",
                f"SELECT round(approx_percentile({lat}, 0.95), 1) AS p95_latency_s FROM {T} {W}",
                "scalar",
                {"scalar.suffix": " s"},
            ),
            18,
            3,
            6,
            3,
        ),
        # --- row 6: customer journey
        (
            card(
                db,
                "Conversion Funnel (page view -> purchase)",
                f"SELECT CASE event WHEN 'page_view' THEN '1. Page view' WHEN 'click' THEN '2. Click' "
                f"WHEN 'add_to_cart' THEN '3. Add to cart' ELSE '4. Purchase' END AS step, count(*) AS events "
                f"FROM {T} {W} AND event IN ('page_view', 'click', 'add_to_cart', 'purchase') GROUP BY 1 ORDER BY 1",
                "funnel",
                {"funnel.dimension": "step", "funnel.metric": "events"},
            ),
            0,
            6,
            12,
            7,
        ),
        (
            card(
                db,
                "Event Mix (share of all events)",
                f"SELECT event, count(*) AS events FROM {T} {W} GROUP BY event ORDER BY events DESC",
                "pie",
                {**pie("event", "events"), "pie.colors": EVENT_COLORS},
            ),
            12,
            6,
            12,
            7,
        ),
        # --- row 13: volume breakdowns
        (
            card(
                db,
                "Events by Event Type",
                f"SELECT event, count(*) AS events FROM {T} {W} GROUP BY event ORDER BY events DESC",
                "row",
                bar("event", "events"),
            ),
            0,
            13,
            12,
            7,
        ),
        (
            card(
                db,
                "Events by Date",
                f"SELECT CAST(event_date AS varchar) AS event_date, count(*) AS events FROM {T} {W} GROUP BY 1 ORDER BY 1",
                "bar",
                bar("event_date", "events"),
            ),
            12,
            13,
            12,
            7,
        ),
        # --- row 20: when + where
        (
            card(
                db,
                "Events by Hour (UTC)",
                f"SELECT lpad(CAST(hour(event_time) AS varchar), 2, '0') || ':00' AS hour_utc, count(*) AS events FROM {T} {W} GROUP BY 1 ORDER BY 1",
                "bar",
                bar("hour_utc", "events"),
            ),
            0,
            20,
            12,
            7,
        ),
        (
            card(
                db,
                "Events by Kafka Partition",
                f"SELECT 'partition ' || CAST(kafka_partition AS varchar) AS kafka_partition_name, count(*) AS events FROM {T} {W} GROUP BY 1 ORDER BY 1",
                "pie",
                pie("kafka_partition_name", "events"),
            ),
            12,
            20,
            12,
            7,
        ),
        # --- row 27: timeline
        (
            card(
                db,
                "Events per Minute by Type",
                f"SELECT date_trunc('minute', event_time) AS minute, event, count(*) AS events FROM {T} {W} GROUP BY 1, 2 ORDER BY 1",
                "line",
                {"graph.dimensions": ["minute", "event"], "graph.metrics": ["events"]},
            ),
            0,
            27,
            24,
            7,
        ),
        # --- row 34: users
        (
            card(
                db,
                "Events by User (top 20)",
                f"SELECT user_id, count(*) AS events FROM {T} {W} GROUP BY user_id ORDER BY events DESC LIMIT 20",
                "bar",
                bar("user_id", "events"),
            ),
            0,
            34,
            24,
            7,
        ),
        (
            card(
                db,
                "User Activity Matrix",
                f"SELECT user_id, count_if(event = 'page_view') AS page_view, count_if(event = 'click') AS click, "
                f"count_if(event = 'add_to_cart') AS add_to_cart, count_if(event = 'remove_from_cart') AS remove_from_cart, "
                f"count_if(event = 'purchase') AS purchase, count(*) AS total FROM {T} {W} GROUP BY user_id ORDER BY total DESC",
                "table",
            ),
            0,
            41,
            14,
            9,
        ),
        # --- data quality
        (
            card(
                db,
                "Rejected Messages by Reason",
                f"SELECT reason, count(*) AS messages FROM {R} GROUP BY reason ORDER BY messages DESC",
                "row",
                {**bar("reason", "messages"), "series_settings": {"messages": {"color": RED}}},
                filters=False,
            ),
            14,
            41,
            10,
            9,
        ),
        # --- row 51: product activity (v_product_activity; events without a product are excluded)
        (
            card(
                db,
                "Products Engaged",
                f"SELECT count(DISTINCT product_id) AS products FROM {P} {W}",
                "scalar",
            ),
            0,
            51,
            6,
            3,
        ),
        (
            card(
                db,
                "Product Purchases",
                f"SELECT coalesce(sum(events), 0) AS purchases FROM {P} {W} AND event = 'purchase'",
                "scalar",
            ),
            6,
            51,
            6,
            3,
        ),
        (
            card(
                db,
                "Revenue (sample data)",
                f"SELECT round(coalesce(sum(events * price), 0), 2) AS revenue FROM {P} {W} AND event = 'purchase'",
                "scalar",
                {"scalar.prefix": "$"},
            ),
            12,
            51,
            6,
            3,
        ),
        (
            card(
                db,
                "Cart to Purchase (sample data)",
                f"SELECT round(100.0 * sum(CASE WHEN event = 'purchase' THEN events ELSE 0 END) "
                f"/ nullif(sum(CASE WHEN event = 'add_to_cart' THEN events ELSE 0 END), 0), 0) AS cart_to_purchase_pct "
                f"FROM {P} {W}",
                "scalar",
                {"scalar.suffix": " %"},
            ),
            18,
            51,
            6,
            3,
        ),
        (
            card(
                db,
                "Top 10 Products by Purchases",
                f"SELECT coalesce(substr(product_name, 1, 40), 'Product ' || CAST(product_id AS varchar)) AS product, "
                f"sum(events) AS purchases FROM {P} {W} AND event = 'purchase' "
                f"GROUP BY product_id, product_name ORDER BY purchases DESC LIMIT 10",
                "row",
                {
                    **bar("product", "purchases"),
                    "graph.y_axis.title_text": "Purchases",
                    "series_settings": {"purchases": {"color": GREEN}},
                },
            ),
            0,
            54,
            12,
            8,
        ),
        (
            card(
                db,
                "Revenue by Category (sample data)",
                f"SELECT coalesce(product_category, 'Unknown') AS category, round(sum(events * price), 2) AS revenue "
                f"FROM {P} {W} AND event = 'purchase' GROUP BY 1 ORDER BY revenue DESC",
                "bar",
                {
                    **bar("category", "revenue"),
                    "graph.y_axis.title_text": "Revenue ($)",
                    "series_settings": {"revenue": {"color": GREEN}},
                },
            ),
            12,
            54,
            12,
            8,
        ),
        (
            card(
                db,
                "Product Funnel",
                f"SELECT product_id, coalesce(product_name, 'Product ' || CAST(product_id AS varchar)) AS product, "
                f"product_category AS category, price, "
                f"sum(CASE WHEN event = 'page_view' THEN events ELSE 0 END) AS page_views, "
                f"sum(CASE WHEN event = 'click' THEN events ELSE 0 END) AS clicks, "
                f"sum(CASE WHEN event = 'add_to_cart' THEN events ELSE 0 END) AS add_to_cart, "
                f"sum(CASE WHEN event = 'remove_from_cart' THEN events ELSE 0 END) AS removed, "
                f"sum(CASE WHEN event = 'purchase' THEN events ELSE 0 END) AS purchases, "
                f"round(100.0 * sum(CASE WHEN event = 'purchase' THEN events ELSE 0 END) "
                f"/ nullif(sum(CASE WHEN event = 'add_to_cart' THEN events ELSE 0 END), 0), 0) AS cart_to_purchase_pct, "
                f"round(sum(CASE WHEN event = 'purchase' THEN events * price ELSE 0 END), 2) AS revenue "
                f"FROM {P} {W} GROUP BY product_id, product_name, product_category, price "
                f"ORDER BY purchases DESC, page_views DESC",
                "table",
            ),
            0,
            62,
            24,
            9,
        ),
    ]

    params = [
        {
            "id": "p_event_date",
            "name": "Event Date",
            "slug": "event_date",
            "type": "date/single",
            "sectionId": "date",
        },
        {
            "id": "p_event",
            "name": "Event Type",
            "slug": "event",
            "type": "string/=",
            "sectionId": "string",
        },
    ]
    dash = api(
        "POST",
        "/api/dashboard",
        {
            "name": DASH_NAME,
            "description": "Real-time pipeline: Kafka -> Spark Streaming -> Delta Lake -> Trino -> Metabase / Power BI. Rates marked (sample data) come from the synthetic producer and can exceed 100%.",
            "parameters": params,
        },
    )
    dashcards = []
    for i, ((cid, filtered), col, row, sx, sy) in enumerate(cards):
        maps = []
        if filtered:
            maps = [
                {
                    "parameter_id": "p_event_date",
                    "card_id": cid,
                    "target": ["variable", ["template-tag", "event_date"]],
                },
                {
                    "parameter_id": "p_event",
                    "card_id": cid,
                    "target": ["variable", ["template-tag", "event"]],
                },
            ]
        dashcards.append(
            {
                "id": -(i + 1),
                "card_id": cid,
                "col": col,
                "row": row,
                "size_x": sx,
                "size_y": sy,
                "parameter_mappings": maps,
                "visualization_settings": {},
            }
        )
    for j, (text, row) in enumerate(
        [("Data quality & pipeline health", 24), ("Product activity", 50)]
    ):
        dashcards.append(
            {
                "id": -(100 + j),
                "card_id": None,
                "col": 0,
                "row": row,
                "size_x": 24,
                "size_y": 1,
                "parameter_mappings": [],
                "visualization_settings": {
                    "virtual_card": {
                        "name": None,
                        "display": "heading",
                        "visualization_settings": {},
                        "dataset_query": {},
                        "archived": False,
                    },
                    "text": text,
                    "dashcard.background": False,
                },
            }
        )
    insight = (
        "**Key insight:** peak activity on **29 Sep 2026** with 6,055 events (92.6% of all events). "
        "Busiest hour is **08:00 UTC** (3,246 events, ~50%), page_view is the most frequent event (972), "
        "and 89.8% of Kafka messages passed validation (745 rejected, mostly missing user_id)."
    )
    dashcards.append(
        {
            "id": -(len(dashcards) + 1),
            "card_id": None,
            "col": 0,
            "row": 71,
            "size_x": 24,
            "size_y": 2,
            "parameter_mappings": [],
            "visualization_settings": {
                "virtual_card": {
                    "name": None,
                    "display": "text",
                    "visualization_settings": {},
                    "dataset_query": {},
                    "archived": False,
                },
                "text": insight,
            },
        }
    )
    api("PUT", f"/api/dashboard/{dash['id']}", {"dashcards": dashcards, "parameters": params})
    print(f">> dashboard ready: {MB}/dashboard/{dash['id']}")
    return dash["id"]


def smoke_test(db):
    res = api(
        "POST",
        "/api/dataset",
        {"database": db, "type": "native", "native": {"query": f"SELECT count(*) FROM {T}"}},
    )
    if res.get("status") != "completed":
        sys.exit(f"!! Trino query from Metabase failed: {res.get('error')}")
    print(f">> Metabase -> Trino OK: {res['data']['rows'][0][0]} events in {T}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true")
    args = ap.parse_args()
    for _ in range(200):
        try:
            urllib.request.urlopen(MB + "/api/health", timeout=5)
            break
        except Exception:
            time.sleep(3)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    open(REPORT, "w").write(time.strftime("%Y-%m-%d %H:%M:%S") + " metabase_setup run\n")
    login()
    db = trino_database()
    smoke_test(db)
    build(db, args.rebuild)
