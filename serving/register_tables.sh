#!/usr/bin/env bash
# Run register_tables.sql against the local Trino with the Trino CLI jar
# (the "trino" file in the project root). Tables that already exist are skipped.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CLI="$HERE/../trino"
JAVA="${JAVA_HOME:-$HOME/jdk-25}/bin/java"; [ -x "$JAVA" ] || JAVA=java
run() { "$JAVA" -jar "$CLI" --server http://localhost:8080 --user admin --execute "$1"; }

LAKEHOUSE="$HERE/../streaming/_lakehouse/warehouse/lakehouse.db"
run "CREATE SCHEMA IF NOT EXISTS delta.lakehouse"
for t in user_events user_events_rejected products; do
  if [ ! -d "$LAKEHOUSE/$t/_delta_log" ]; then
    echo ">> skipped delta.lakehouse.$t: no Delta table at $LAKEHOUSE/$t"
    [ "$t" = products ] && echo "   create it with: python3 streaming/load_products.py"
    continue
  fi
  if run "SHOW TABLES FROM delta.lakehouse" | grep -qx "\"$t\""; then
    echo ">> delta.lakehouse.$t already registered"
  else
    run "CALL delta.system.register_table(schema_name => 'lakehouse', table_name => '$t', table_location => 'local:///warehouse/lakehouse.db/$t')" \
      && echo ">> registered delta.lakehouse.$t"
  fi
done
# serving views (CREATE OR REPLACE is idempotent)
"$JAVA" -jar "$CLI" --server http://localhost:8080 --user admin --catalog delta --schema lakehouse --file "$HERE/views.sql" && echo ">> views created"
echo ">> Check:"
# product tracking (needs the product_id column, added when streaming/spark_delta.py starts)
run "SELECT count(product_id) AS product_events, count(DISTINCT product_id) AS products FROM delta.lakehouse.user_events" \
  || echo "!! user_events has no product_id yet: restart streaming/spark_delta.py, then TRINO_ACTION=restart week3_up.sh"
run "SELECT count(*) AS total_events, count(DISTINCT user_id) AS users, count(DISTINCT event) AS event_types FROM delta.lakehouse.user_events"
