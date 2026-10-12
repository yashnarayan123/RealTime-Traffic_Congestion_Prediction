#!/usr/bin/env bash
# Stop helper: stops THIS project's processes and Docker services WITHOUT deleting data.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Kill by PID file after verifying the process belongs to this project
# (command line mentions ROOT, or its cwd IS ROOT); falls back to the same
# scoped live-process scan so other projects are never touched.
pid_in_project() {
  local pid="$1"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  ps -p "$pid" -o args= 2>/dev/null | grep -q "$ROOT" && return 0
  [ -L "/proc/$pid/cwd" ] && [ "$(readlink "/proc/$pid/cwd")" = "$ROOT" ] && return 0
  return 1
}

stop_pidfile() {
  local name="$1" pattern="$2"
  local pidfile="logs/${name}.pid"
  if [ -f "$pidfile" ]; then
    local pid
    pid="$(cat "$pidfile")"
    if pid_in_project "$pid"; then
      kill "$pid" 2>/dev/null && echo "$name stopped (PID $pid)" || echo "$name PID $pid already exiting"
      rm -f "$pidfile"
      return 0
    else
      echo "$name PID file stale (no live project process); removing."
      rm -f "$pidfile"
    fi
  fi
  # Fallback: only PIDs belonging to this project (args or cwd check).
  local found=0
  for pid in $(pgrep -f "$pattern" 2>/dev/null); do
    if pid_in_project "$pid"; then
      kill "$pid" 2>/dev/null && echo "$name stopped (PID $pid)" || true
      found=1
    fi
  done
  [ "$found" = 0 ] && echo "$name was not running"
}

echo "==> Stopping Spark streaming processor (if running) ..."
# The driver runs inside traffic-spark-master; stop it there first so no
# orphan keeps consuming Kafka after the host-side exec client is killed.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "traffic-spark-master"; then
  # Match the script path (the Spark app NAME is not in the java cmdline).
  if docker exec traffic-spark-master pgrep -f "traffic_stream_processor\.py" >/dev/null 2>&1; then
    docker exec traffic-spark-master pkill -f "traffic_stream_processor\.py" 2>/dev/null \
      && echo "processor driver stopped in traffic-spark-master" \
      || echo "processor driver already exiting"
  else
    echo "processor driver not running in container"
  fi
fi
stop_pidfile "spark_processor" "spark-submit.*traffic_stream_processor"

echo "==> Stopping local dashboard/producer for this project (if running) ..."
stop_pidfile "dashboard" "streamlit run.*dashboard/app.py"
stop_pidfile "producer" "traffic_producer.py"

if command -v docker >/dev/null 2>&1; then
  echo "==> Stopping Docker services (data volumes preserved) ..."
  docker compose stop 2>/dev/null || true
else
  echo "(docker not installed, skipping)"
fi

echo "Data preserved in data/ and kafka_data volume."
echo "To fully clean generated output ONLY (never models/datasets):"
echo "  rm -rf data/processed/traffic_summary/* data/checkpoints/*"
echo "To also remove Kafka volume: docker compose down -v   (erases queued Kafka messages)"
