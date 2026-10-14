-- Serving layer in the cloud: Amazon Athena (engine v3 = Trino) over the same Delta
-- folders the stream writes to S3. The cloud counterpart of serving/register_tables.sql
-- and serving/views.sql; the view SQL is identical (Trino dialect).
--
-- Run in the Athena workgroup from `terraform output athena_workgroup`, after the
-- streaming job created the tables. Replace <lakehouse_root> with
-- `terraform output -raw lakehouse_root` (deploy/aws/deploy.sh does it for you).
--
-- Athena reads Delta natively from a Glue table with table_type = DELTA. Spark's own
-- Glue tables (database "lakehouse") are kept for Spark; these live in
-- "lakehouse_serving", like Trino's separate catalog in the local setup.

CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_serving.user_events
LOCATION '<lakehouse_root>/warehouse/lakehouse.db/user_events'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_serving.user_events_rejected
LOCATION '<lakehouse_root>/warehouse/lakehouse.db/user_events_rejected'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE OR REPLACE VIEW lakehouse_serving.v_events_by_type AS
SELECT event, event_date, count(*) AS events, count(DISTINCT user_id) AS users
FROM lakehouse_serving.user_events
GROUP BY event, event_date;

CREATE OR REPLACE VIEW lakehouse_serving.v_events_by_user AS
SELECT user_id, event_date, count(*) AS events
FROM lakehouse_serving.user_events
GROUP BY user_id, event_date;

CREATE OR REPLACE VIEW lakehouse_serving.v_events_per_minute AS
SELECT date_trunc('minute', event_time) AS minute, event, count(*) AS events
FROM lakehouse_serving.user_events
GROUP BY 1, 2;
