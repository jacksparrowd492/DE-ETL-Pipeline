#!/usr/bin/env bash
# Week 3 - BI layer: Metabase connected to Trino (Starburst/Trino driver).
#   bash serving/start_metabase.sh        # first run downloads ~400 MB, then serves http://localhost:3000
# Metabase keeps its own settings DB in ~/metabase (dashboards survive restarts).
set -euo pipefail
SERVING_DIR="$(cd "$(dirname "$0")" && pwd)"
MB_DIR="$HOME/metabase"
mkdir -p "$MB_DIR/plugins"
cd "$MB_DIR"

# The Starburst/Trino driver (6.1.0) is built against Metabase 0.52, so pin a 0.52.x release
if [ ! -f metabase.jar ]; then
  for v in v0.52.8 v0.52.6 v0.52.4 v0.52.2; do
    echo ">> Downloading Metabase $v ..."
    curl -fL -o metabase.jar "https://downloads.metabase.com/$v/metabase.jar" && break
    rm -f metabase.jar
  done
  [ -f metabase.jar ] || { echo "!! could not download Metabase"; exit 1; }
fi
if ! ls plugins/starburst*.metabase-driver.jar >/dev/null 2>&1; then
  echo ">> Downloading Starburst (Trino) Metabase driver ..."
  curl -fL -o plugins/starburst-6.1.0.metabase-driver.jar \
    https://github.com/starburstdata/metabase-driver/releases/download/6.1.0/starburst-6.1.0.metabase-driver.jar
fi

# Metabase 0.52 is certified on Java 21 (Trino uses 25). Pick a Java 21:
#   1) any JDK 21 under /usr/lib/jvm   2) ~/jdk-21   3) download Temurin 21 into ~/jdk-21 (no sudo)
JAVA=""
for j in /usr/lib/jvm/*21*/bin/java "$HOME/jdk-21/bin/java"; do
  [ -x "$j" ] && { JAVA="$j"; break; }
done
if [ -z "$JAVA" ] && java -version 2>&1 | grep -q 'version "21'; then JAVA="$(command -v java)"; fi
if [ -z "$JAVA" ]; then
  echo ">> Downloading Temurin JDK 21 into ~/jdk-21 (for Metabase) ..."
  mkdir -p "$HOME/jdk-21"
  curl -fsSL "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse" \
    | tar -xz -C "$HOME/jdk-21" --strip-components=1
  JAVA="$HOME/jdk-21/bin/java"
fi
if curl -fs http://localhost:3000/api/health 2>/dev/null | grep -q ok; then echo ">> Metabase already running"; exit 0; fi

export MB_DB_FILE="$MB_DIR/metabase.db" MB_PLUGINS_DIR="$MB_DIR/plugins" MB_JETTY_PORT=3000
LOG="$SERVING_DIR/logs/metabase.log"
mkdir -p "$(dirname "$LOG")"
echo ">> Starting Metabase on http://localhost:3000 with $("$JAVA" -version 2>&1 | head -1)  (log: $LOG)"
nohup "$JAVA" -Xmx2g --add-opens java.base/java.nio=ALL-UNNAMED -jar metabase.jar > "$LOG" 2>&1 &
echo $! > metabase.pid
for _ in $(seq 1 200); do
  kill -0 "$(cat metabase.pid)" 2>/dev/null || { echo "!! Metabase exited - last log lines:"; tail -40 "$LOG"; exit 1; }
  curl -fs http://localhost:3000/api/health 2>/dev/null | grep -q ok && { echo ">> Metabase is up: http://localhost:3000"; exit 0; }
  sleep 3
done
echo "!! Metabase not ready after 10 min - see $LOG"; exit 1
