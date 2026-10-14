# Week 3 – Trino query engine & dashboard serving

```
Kafka ──► Spark Structured Streaming ──► Delta Lake (streaming/_lakehouse/warehouse)
                                              │
                                    Trino 483 (catalog "delta", Delta Lake connector)
                                              │
                    ┌─────────────────────────┴──────────────────────────┐
              Metabase (localhost:3000)                     Power BI Desktop
              "User Events - Trino Lakehouse"               Python source → trino.dbapi
```

## Run everything (WSL Ubuntu)

```bash
bash "/mnt/f/Files/DE LAB/ETL-Pipeline/serving/week3_up.sh"
# after changing Trino config:
TRINO_ACTION=restart bash "/mnt/f/Files/DE LAB/ETL-Pipeline/serving/week3_up.sh"
```

| Step | Script | What it does |
|---|---|---|
| 1 | `start_trino.sh [start|restart|stop|status]` | Starts the coordinator from `F:\trino-server-483` on Java 25 |
| 2 | `register_tables.sh` (+ `register_tables.sql`, `views.sql`) | Registers the Delta tables in Trino and creates serving views |
| 3 | `start_metabase.sh` | Metabase 0.52.8 + Starburst/Trino driver 6.1.0 on Java 21 |
| 4 | `metabase_setup.py [--rebuild]` | Adds the Trino connection and builds the dashboard via the Metabase API |

Logs: `serving/logs/week3_up.log`, `serving/logs/metabase.log`, Trino: `/var/tmp/trino-data/var/log/server.log` (WSL).

## Trino configuration (`F:\trino-server-483\etc`)

* `catalog/delta.properties` – `connector.name=delta-lake`, local file system
  (`fs.native-local.enabled`, `local.location=…/streaming/_lakehouse`), file metastore
  `local:///trino_metastore`, `delta.register-table-procedure.enabled=true`.
  Spark's Hive metastore is embedded Derby (single process), so Trino keeps its own
  catalog and the existing Delta folders are registered with
  `CALL delta.system.register_table(...)`. Spark keeps writing; Trino reads the
  latest Delta snapshot on every query.
* `config.properties` – coordinator + worker in one node, HTTP 8080
  (HTTPS 8443 with a self-signed localhost certificate is also enabled; optional).
* Originals are kept in `etc/_original/`.

Objects exposed: `delta.lakehouse.user_events` (with `product_id`), `delta.lakehouse.user_events_rejected`,
`delta.lakehouse.products` (written by `streaming/load_products.py`; `week3_up.sh` creates it if
missing), views `v_events_by_type`, `v_events_by_user`, `v_events_per_minute` and
`v_product_activity` (events per product, event type and day, joined to `products`).

After the product-tracking upgrade: restart `streaming/spark_delta.py` once (it adds the
`product_id` column), then `TRINO_ACTION=restart bash serving/week3_up.sh --rebuild` so Trino
re-reads the schema, the view is created and the Metabase dashboard gets the product cards.
The Streamlit **Products** page reads the same view (`TRINO_HOST` / `TRINO_PORT` in `.env`).

## Dashboards

Both dashboards show the same story, all numbers come from Trino.

| Section | Chart | Why it matters |
|---|---|---|
| Volume | Total events, unique users, event types, avg events/user | Size of the stream |
| Volume | Events by type, by date, by user | Where traffic comes from |
| Business | **Purchase rate** (purchases ÷ add-to-cart), **cart removal rate** | Conversion health |
| Business | **Conversion funnel** page view → click → add to cart → purchase | Drop-off between steps |
| Business | **Event mix** donut, **activity by hour of day (UTC)** | Behaviour pattern / peak hours |
| Business | **User activity heatmap** (user × event type) | Power users and odd behaviour |
| Pipeline | **Data quality %** (valid ÷ valid + rejected), **rejected messages by reason** | Validation in the Spark job |
| Pipeline | **Ingestion latency p95** (ingested_at − event_time) | Freshness of the lakehouse |
| Pipeline | **Events by Kafka partition** | Partition balance / skew |
| Products (Metabase + Streamlit) | **Products engaged**, **purchases**, **revenue** and **cart → purchase** (sample data) | Which products sell |
| Products (Metabase + Streamlit) | **Top 10 products by purchases**, **revenue by category**, **product funnel** table | Per-product drop-off from view to purchase |

* **Metabase** – http://localhost:3000 → *User Events - Trino Lakehouse* (one page, filters
  **Event Date** and **Event Type** applied inside the SQL that runs on Trino).
  Rebuild after changes: `python3 serving/metabase_setup.py --rebuild`
  (each card's query is checked; results in `serving/logs/metabase_setup.log`).
* **Power BI** – `F:\Files\DE LAB\User_Event_Analytics_Trino.pbix`, two pages:
  *Overview* and *Funnel & Pipeline Health*. Source = Python script using
  `trino.dbapi.connect(host='localhost', port=8080, user='powerbi', catalog='delta', schema='lakehouse')`
  that loads `events` (with derived `event_hour`, `ingest_latency_s`, `funnel_step`) and
  `rejected`. *Refresh* re-queries Trino.

## Checks

```sql
SELECT count(*), count(DISTINCT user_id), count(DISTINCT event) FROM delta.lakehouse.user_events;
-- 6541 | 20 | 7   (2026-09-23: 486, 2026-09-29: 6055)
```
