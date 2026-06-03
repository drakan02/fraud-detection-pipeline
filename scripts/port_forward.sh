#!/usr/bin/env bash
#
# Dynamically port-forwards ClusterIP services to localhost using ports defined in .env.
# Usage: ./scripts/port_forward.sh
#

set -uo pipefail

# Resolve script directory to load .env from root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

# Load .env file
if [ -f .env ]; then
  echo "Loading ports from .env file..."
  while IFS= read -r line || [ -n "$line" ]; do
    line=$(echo "$line" | xargs)
    if [[ ! -z "$line" && "$line" != \#* && "$line" == *=* ]]; then
      export "$line"
    fi
  done < .env
fi

# Set defaults
GRAFANA_PORT=${GRAFANA_PORT:-30000}
PROMETHEUS_PORT=${PROMETHEUS_PORT:-30090}
FLINK_WEB_PORT=${FLINK_WEB_PORT:-30081}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-30123}
MODEL_SERVER_PORT=${MODEL_SERVER_PORT:-30001}
KAFKA_PORT=${KAFKA_PORT:-30093}

echo "=========================================================="
# Custom formatting for a premium console interface
echo -e "\033[1;36mStarting Kubernetes ClusterIP Port-Forwards to localhost\033[0m"
echo "=========================================================="
echo " Grafana      : http://localhost:$GRAFANA_PORT (admin/fraudadmin)"
echo " Prometheus   : http://localhost:$PROMETHEUS_PORT"
echo " Flink UI     : http://localhost:$FLINK_WEB_PORT"
echo " ClickHouse   : http://localhost:$CLICKHOUSE_PORT (user: default)"
echo " ML Server    : http://localhost:$MODEL_SERVER_PORT"
echo " Kafka        : localhost:$KAFKA_PORT (replayer target)"
echo "=========================================================="

# Terminate any existing port-forward processes to avoid address-in-use errors
echo "Stopping any existing port-forwards..."
pkill -f "port-forward" 2>/dev/null || true
sleep 1

# Start port-forwards in background
kubectl port-forward svc/grafana-service      "$GRAFANA_PORT":3000    > /dev/null 2>&1 &
kubectl port-forward svc/prometheus           "$PROMETHEUS_PORT":9090 > /dev/null 2>&1 &
kubectl port-forward svc/flink-jobmanager-service "$FLINK_WEB_PORT":8081 > /dev/null 2>&1 &
kubectl port-forward svc/clickhouse-service   "$CLICKHOUSE_PORT":8123 > /dev/null 2>&1 &
kubectl port-forward svc/ml-server-service    "$MODEL_SERVER_PORT":8001 > /dev/null 2>&1 &
kubectl port-forward svc/kafka-service        "$KAFKA_PORT":9093 > /dev/null 2>&1 &

echo -e "\033[1;32mPort-forwards are running in the background.\033[0m"
echo "Press [Ctrl+C] to stop this script and terminate all port-forwards."

# Catch termination signals to clean up background processes
cleanup() {
  echo ""
  echo "Shutting down port-forwards..."
  pkill -f "port-forward" 2>/dev/null || true
  exit 0
}

trap cleanup INT TERM

# Keep script running
while true; do
  sleep 1
done
