-- Serving views used by the BI dashboard
CREATE OR REPLACE VIEW delta.lakehouse.v_events_by_type AS
SELECT event, event_date, count(*) AS events, count(DISTINCT user_id) AS users
FROM delta.lakehouse.user_events
GROUP BY event, event_date;

CREATE OR REPLACE VIEW delta.lakehouse.v_events_by_user AS
SELECT user_id, event_date, count(*) AS events
FROM delta.lakehouse.user_events
GROUP BY user_id, event_date;

CREATE OR REPLACE VIEW delta.lakehouse.v_events_per_minute AS
SELECT date_trunc('minute', event_time) AS minute, event, count(*) AS events
FROM delta.lakehouse.user_events
GROUP BY 1, 2;

-- Product activity: events per product, event type and day, with the product's
-- name, category and price from lakehouse.products. Events without a product_id
-- (login/logout, events from before product tracking) are left out. Kept last:
-- it needs lakehouse.products, which register_tables.sh may have skipped.
CREATE OR REPLACE VIEW delta.lakehouse.v_product_activity AS
SELECT e.product_id, p.product_name, p.product_category, p.price, e.event, e.event_date, count(*) AS events, count(DISTINCT e.user_id) AS users
FROM delta.lakehouse.user_events e
LEFT JOIN delta.lakehouse.products p ON p.product_id = e.product_id
WHERE e.product_id IS NOT NULL
GROUP BY e.product_id, p.product_name, p.product_category, p.price, e.event, e.event_date;
