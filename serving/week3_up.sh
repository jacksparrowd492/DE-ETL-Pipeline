#!/usr/bin/env bash
# Week 3 one-shot: Trino (Delta catalog) -> register lakehouse tables -> Metabase dashboard.
# Run from WSL Ubuntu:
#   bash "/mnt/f/Files/DE LAB/ETL-Pipeline/serving/week3_up.sh"
# Output is also saved to serving/logs/week3_up.log
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$HERE/logs"
exec > >(tee "$HERE/logs/week3_up.log") 2>&1

step() { echo; echo "========== $* =========="; }

step "1/4 Trino coordinator"
bash "$HERE/start_trino.sh" "${TRINO_ACTION:-start}" || { echo "!! Trino failed:"; tail -60 /var/tmp/trino-data/var/log/server.log; cp /var/tmp/trino-data/var/log/server.log "$HERE/logs/trino-server.log"; exit 1; }

step "2/4 Register Delta tables + serving views"
# product catalog for the product cards: created once, refresh with streaming/load_products.py
if [ ! -d "$HERE/../streaming/_lakehouse/warehouse/lakehouse.db/products/_delta_log" ]; then
  python3 "$HERE/../streaming/load_products.py" \
    || echo "!! could not create lakehouse.products here (needs: pip install deltalake pyarrow requests python-dotenv); run streaming/load_products.py where those are installed"
fi
bash "$HERE/register_tables.sh" || exit 1

step "3/4 Metabase (BI)"
bash "$HERE/start_metabase.sh" || exit 1

step "4/4 Dashboard"
python3 -u "$HERE/metabase_setup.py" "$@"

echo
echo "Trino UI : http://localhost:8080/ui"
echo "Metabase : http://localhost:3000"
