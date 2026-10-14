#!/usr/bin/env bash
# Forgot the Metabase admin password? Start Metabase with a fresh, empty settings DB.
# Nothing is deleted: the old settings DB is moved to ~/metabase/backup-<time>/.
# Then the dashboard is rebuilt and you choose a NEW admin email/password.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MB_DIR="$HOME/metabase"

echo ">> Stopping Metabase ..."
pkill -f 'metabase.jar' 2>/dev/null || true
for _ in $(seq 1 30); do pgrep -f 'metabase.jar' >/dev/null || break; sleep 1; done

BK="$MB_DIR/backup-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BK"
mv "$MB_DIR"/metabase.db* "$BK"/ 2>/dev/null && echo ">> Old Metabase settings moved to $BK"

bash "$HERE/start_metabase.sh" || exit 1
python3 -u "$HERE/metabase_setup.py"
