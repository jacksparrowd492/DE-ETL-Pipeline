#!/usr/bin/env bash
# Week 3 - start the Trino coordinator inside WSL (Ubuntu).
#   bash serving/start_trino.sh          # start in background
#   bash serving/start_trino.sh stop     # stop
#   bash serving/start_trino.sh status
# Trino 483 needs Java 25. If it is not on PATH, a JDK 25 is downloaded once
# into ~/jdk-25 (no sudo needed).
set -euo pipefail

TRINO_HOME="${TRINO_HOME:-/mnt/f/trino-server-483}"
JDK_DIR="$HOME/jdk-25"
ACTION="${1:-start}"

java_major() { "$1" -version 2>&1 | awk -F'"' '/version/ {split($2,v,"."); print v[1]; exit}'; }

if [ -x "$JDK_DIR/bin/java" ]; then
  export JAVA_HOME="$JDK_DIR"
elif command -v java >/dev/null && [ "$(java_major java)" -ge 25 ]; then
  export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
else
  echo ">> Downloading Temurin JDK 25 into $JDK_DIR ..."
  mkdir -p "$JDK_DIR"
  curl -fsSL "https://api.adoptium.net/v3/binary/latest/25/ga/linux/x64/jdk/hotspot/normal/eclipse" \
    | tar -xz -C "$JDK_DIR" --strip-components=1
  export JAVA_HOME="$JDK_DIR"
fi
export PATH="$JAVA_HOME/bin:$PATH"
echo ">> Using $(java -version 2>&1 | head -1)"

cd "$TRINO_HOME"
chmod +x bin/launcher bin/linux-amd64/launcher 2>/dev/null || true
# A Trino started earlier with another data dir (old /tmp/trino-data) is invisible to
# "launcher status/stop" but still holds port 8080, so stop any Trino JVM on restart/stop.
if [ "$ACTION" = "restart" ] || [ "$ACTION" = "stop" ]; then
  if pgrep -f io.trino.server.TrinoServer >/dev/null; then
    echo ">> Stopping running Trino process(es): $(pgrep -f io.trino.server.TrinoServer | tr '\n' ' ')"
    pkill -f io.trino.server.TrinoServer || true
    for _ in $(seq 1 30); do pgrep -f io.trino.server.TrinoServer >/dev/null || break; sleep 1; done
  fi
  [ "$ACTION" = "stop" ] && exit 0
  ACTION=start
fi
if [ "$ACTION" = "start" ] && bin/launcher status >/dev/null 2>&1; then
  echo ">> Trino already running"
else
  bin/launcher "$ACTION"
fi

if [ "$ACTION" = "start" ] || [ "$ACTION" = "restart" ]; then
  echo ">> Waiting for Trino on http://localhost:8080 ..."
  for _ in $(seq 1 60); do
    if curl -fs http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false' \
       && curl -fsk https://localhost:8443/v1/info >/dev/null 2>&1; then
      echo ">> Trino is up:  http://localhost:8080/ui   (user: admin, no password)"
      exit 0
    fi
    sleep 2
  done
  echo "!! Trino did not become ready - see /var/tmp/trino-data/var/log/server.log"
  exit 1
fi
