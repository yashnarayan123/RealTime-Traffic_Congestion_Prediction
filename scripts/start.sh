#!/usr/bin/env bash
# Start helper: avoids duplicate processes, reports useful errors.
# Usage:
#   ./scripts/start.sh --full-stack     # single command: docker + topic + processor + producer + dashboard
#   ./scripts/start.sh --dashboard-only  # dashboard only (default with no args)
#   ./scripts/start.sh [--with-docker] [--producer] [--spark-submit] [--dashboard-only]
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs data/processed/traffic_summary data/checkpoints

WITH_DOCKER=0; WITH_PRODUCER=0; WITH_SPARK=0; DASHBOARD_ONLY=0; FULL_STACK=0
for a in "$@"; do
  case "$a" in
    --full-stack) FULL_STACK=1;;
    --with-docker) WITH_DOCKER=1;;
    --producer) WITH_PRODUCER=1;;
    --spark-submit) WITH_SPARK=1;;
    --dashboard-only) DASHBOARD_ONLY=1;;
    *) echo "Unknown arg: $a"; exit 2;;
  esac
done

if [ "$FULL_STACK" = 1 ]; then
  WITH_DOCKER=1; WITH_PRODUCER=1; WITH_SPARK=1
fi

[ -x ".venv/bin/python" ] || { echo "FAIL: .venv missing. Run ./scripts/setup.sh first."; exit 1; }

# Root cause fix for background exits: Streamlit's first-ever run prints a
# welcome prompt asking for an email on stdin, which blocks (then dies) when
# backgrounded. Pre-seeding credentials.toml makes startup non-interactive.
# This only touches the user's own ~/.streamlit/credentials.toml when the
# email field is absent; it never deletes or overwrites an existing email.
if [ ! -f "$HOME/.streamlit/credentials.toml" ] || ! grep -q 'email' "$HOME/.streamlit/credentials.toml" 2>/dev/null; then
  mkdir -p "$HOME/.streamlit"
  printf '[general]\nemail = ""\n' >> "$HOME/.streamlit/credentials.toml"
  echo "==> Seeded ~/.streamlit/credentials.toml (skips first-run email prompt)."
fi

# Scoped duplicate check: PID file first, then live processes whose command
# line mentions this ROOT or whose working directory IS this ROOT (covers
# relative-path invocations like `streamlit run src/dashboard/app.py`).
# Never matches other projects' processes.
pid_in_project() {
  local pid="$1"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  ps -p "$pid" -o args= 2>/dev/null | grep -q "$ROOT" && return 0
  [ -L "/proc/$pid/cwd" ] && [ "$(readlink "/proc/$pid/cwd")" = "$ROOT" ] && return 0
  return 1
}

dashboard_running() {
  if [ -f logs/dashboard.pid ]; then
    if pid_in_project "$(cat logs/dashboard.pid)"; then
      return 0
    else
      rm -f logs/dashboard.pid
    fi
  fi
  for pid in $(pgrep -f "streamlit run.*dashboard/app.py" 2>/dev/null); do
    if pid_in_project "$pid"; then
      return 0
    fi
  done
  return 1
}

producer_running() {
  if [ -f logs/producer.pid ]; then
    if pid_in_project "$(cat logs/producer.pid)"; then
      return 0
    else
      rm -f logs/producer.pid
    fi
  fi
  for pid in $(pgrep -f "traffic_producer.py" 2>/dev/null); do
    if pid_in_project "$pid"; then
      return 0
    fi
  done
  return 1
}

processor_running() {
  if [ -f logs/spark_processor.pid ]; then
    if pid_in_project "$(cat logs/spark_processor.pid)"; then
      return 0
    else
      rm -f logs/spark_processor.pid
    fi
  fi
  # Container-side check: the driver runs inside traffic-spark-master.
  # (Match the script path: the app NAME is not in the java cmdline.)
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "traffic-spark-master" \
    && docker exec traffic-spark-master pgrep -f "traffic_stream_processor\.py" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

ensure_topic() {
  if docker exec traffic-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "traffic-events"; then
    echo "==> Kafka topic traffic-events exists."
  else
    echo "==> Creating Kafka topic traffic-events ..."
    docker exec traffic-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic traffic-events \
      --partitions 3 --replication-factor 1
  fi
}

wait_kafka_healthy() {
  echo "==> Waiting for Kafka to be healthy ..."
  for _ in $(seq 1 30); do
    if [ "$(docker inspect -f '{{.State.Health.Status}}' traffic-kafka 2>/dev/null)" = "healthy" ]; then
      echo "==> Kafka is healthy."
      return 0
    fi
    sleep 2
  done
  echo "FAIL: Kafka did not become healthy in time. See: docker compose ps; docker logs traffic-kafka"
  return 1
}

start_processor() {
  if processor_running; then
    echo "WARN: Spark processor already running. Not starting a duplicate."
    return 0
  fi
  # The Spark executor runs as UID 185 (spark) inside the container and writes
  # Parquet through the ./data bind mount: the host DIRS must be writable by
  # any UID. Only touch the dirs (never recursive: output files may be owned
  # by the container user and must not fail the startup).
  chmod a+rwX data/processed data/processed/traffic_summary data/checkpoints 2>/dev/null || true
  echo "==> Submitting Spark streaming processor ..."
  nohup docker exec traffic-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/traffic/src/processing/traffic_stream_processor.py \
    < /dev/null > logs/spark_processor.log 2>&1 &
  echo "$!" > logs/spark_processor.pid
  echo "Processor exec-client PID: $(cat logs/spark_processor.pid) (logs/spark_processor.log)"
}

if [ "$WITH_DOCKER" = 1 ]; then
  command -v docker >/dev/null 2>&1 || { echo "FAIL: docker not installed."; exit 1; }
  if [ "$FULL_STACK" = 1 ]; then
    echo "==> Building Spark image (cached after first build) ..."
    docker build -f Dockerfile.spark -t traffic-spark:4.0.1-kafka . || exit 1
  else
    echo "==> Building Spark image (first run downloads jars) ..."
    docker build -f Dockerfile.spark -t traffic-spark:4.0.1-kafka . || exit 1
  fi
  echo "==> Starting kafka + spark-master + spark-worker ..."
  docker compose up -d kafka spark-master spark-worker
  wait_kafka_healthy || exit 1
  ensure_topic
  echo "Kafka host endpoint: localhost:9094 | container endpoint: kafka:9092"
fi

if dashboard_running; then
  echo "WARN: dashboard already running for this project. Not starting a duplicate."
  echo "      Stop it first: ./scripts/stop.sh"
else
  if [ "$DASHBOARD_ONLY" = 1 ] || [ "$FULL_STACK" = 1 ] || { [ "$WITH_PRODUCER" = 0 ] && [ "$WITH_SPARK" = 0 ] && [ "$WITH_DOCKER" = 0 ]; }; then
    echo "==> Starting Streamlit dashboard on http://localhost:8501 ..."
    # < /dev/null is essential: never let the background process block on stdin.
    nohup .venv/bin/python -m streamlit run src/dashboard/app.py \
      --server.port 8501 --server.address 0.0.0.0 --server.headless true \
      < /dev/null > logs/dashboard.log 2>&1 &
    echo "$!" > logs/dashboard.pid
    echo "Dashboard PID: $(cat logs/dashboard.pid) (logs/dashboard.log, logs/dashboard.pid)"
  fi
fi

if [ "$WITH_PRODUCER" = 1 ]; then
  if producer_running; then
    echo "WARN: producer already running for this project. Not starting a duplicate."
  else
    echo "==> Starting Kafka producer ..."
    nohup .venv/bin/python src/producer/traffic_producer.py \
      < /dev/null > logs/producer.log 2>&1 &
    echo "$!" > logs/producer.pid
    echo "Producer PID: $(cat logs/producer.pid) (logs/producer.log, logs/producer.pid)"
  fi
fi

if [ "$WITH_SPARK" = 1 ]; then
  if [ "$FULL_STACK" = 1 ]; then
    start_processor
  else
    echo "==> Submitting Spark processor to spark://localhost:7077 ..."
    echo "    Run on the spark-master container:"
    echo "    docker exec traffic-spark-master /opt/spark/bin/spark-submit \\"
    echo "      --master spark://spark-master:7077 \\"
    echo "      /opt/traffic/src/processing/traffic_stream_processor.py"
  fi
fi

echo "Done. URLs: dashboard http://localhost:8501 | spark-master http://localhost:8080 | kafka localhost:9094"
