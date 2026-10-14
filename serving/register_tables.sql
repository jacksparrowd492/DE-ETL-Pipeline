-- Week 3: expose the Week 2 Delta tables to Trino.
-- Spark's Hive Metastore is embedded Derby (one process only), so Trino keeps
-- its own file metastore and we register the existing Delta folders in it.
-- Paths are relative to local.location in etc/catalog/delta.properties
-- (= ETL-Pipeline/streaming/_lakehouse). Safe to re-run.

CREATE SCHEMA IF NOT EXISTS delta.lakehouse;

CALL delta.system.register_table(
    schema_name    => 'lakehouse',
    table_name     => 'user_events',
    table_location => 'local:///warehouse/lakehouse.db/user_events');

CALL delta.system.register_table(
    schema_name    => 'lakehouse',
    table_name     => 'user_events_rejected',
    table_location => 'local:///warehouse/lakehouse.db/user_events_rejected');


-- Product catalog written by streaming/load_products.py (run that first)
CALL delta.system.register_table(
    schema_name    => 'lakehouse',
    table_name     => 'products',
    table_location => 'local:///warehouse/lakehouse.db/products');
