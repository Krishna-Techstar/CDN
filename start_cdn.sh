#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_cdn.sh — Launch all Mini CDN components
# Usage: ./start_cdn.sh [start|stop|status|restart]
# ─────────────────────────────────────────────────────────────────────────────

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

# ── Colors ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

start_server() {
    local name=$1
    local cmd=$2
    local log="$LOG_DIR/${name}.log"
    local pid_file="$PID_DIR/${name}.pid"

    echo -e "${CYAN}Starting ${name}...${NC}"
    $cmd > "$log" 2>&1 &
    echo $! > "$pid_file"
    sleep 0.5
    echo -e "${GREEN}✔ ${name} started (PID: $!)${NC}"
}

stop_server() {
    local name=$1
    local pid_file="$PID_DIR/${name}.pid"
    if [ -f "$pid_file" ]; then
        kill "$(cat "$pid_file")" 2>/dev/null || true
        rm "$pid_file"
        echo -e "${RED}✖ ${name} stopped${NC}"
    fi
}

check_status() {
    local name=$1
    local pid_file="$PID_DIR/${name}.pid"
    if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo -e "${GREEN}✔ ${name} is running (PID: $(cat $pid_file))${NC}"
    else
        echo -e "${RED}✖ ${name} is NOT running${NC}"
    fi
}

start_all() {
    echo ""
    echo -e "${YELLOW}════════════════════════════════════════${NC}"
    echo -e "${YELLOW}       🚀 Starting Mini CDN System       ${NC}"
    echo -e "${YELLOW}════════════════════════════════════════${NC}"
    echo ""

    # 1. Metrics server (start first so edges can report immediately)
    start_server "metrics_server" \
        "python3 $PROJECT_DIR/metrics/metrics_server.py"

    sleep 1

    # 2. Origin server
    start_server "origin_server" \
        "python3 $PROJECT_DIR/origin_server/origin_server.py"

    sleep 1

    # 3. Edge servers (3 instances on ports 8001, 8002, 8003)
    start_server "edge1" \
        "python3 $PROJECT_DIR/edge_server/edge_server.py --id edge1 --port 8001"
    start_server "edge2" \
        "python3 $PROJECT_DIR/edge_server/edge_server.py --id edge2 --port 8002"
    start_server "edge3" \
        "python3 $PROJECT_DIR/edge_server/edge_server.py --id edge3 --port 8003"

    sleep 1

    # 4. Load balancer
    start_server "load_balancer" \
        "python3 $PROJECT_DIR/load_balancer/load_balancer.py"

    sleep 2

    echo ""
    echo -e "${YELLOW}════════════════════════════════════════${NC}"
    echo -e "${GREEN}  ✅ Mini CDN System is RUNNING!${NC}"
    echo -e "${YELLOW}════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${CYAN}Entry Point (NGINX):${NC}     http://localhost:80"
    echo -e "  ${CYAN}Load Balancer:${NC}           http://localhost:8080"
    echo -e "  ${CYAN}Edge 1:${NC}                  http://localhost:8001"
    echo -e "  ${CYAN}Edge 2:${NC}                  http://localhost:8002"
    echo -e "  ${CYAN}Edge 3:${NC}                  http://localhost:8003"
    echo -e "  ${CYAN}Origin Server:${NC}           http://localhost:8000"
    echo -e "  ${CYAN}Metrics API:${NC}             http://localhost:9000/metrics"
    echo ""
    echo -e "  ${YELLOW}Example Requests:${NC}"
    echo -e "  curl http://localhost:8080/get/index.html"
    echo -e "  curl http://localhost:9000/metrics"
    echo -e "  curl -X POST http://localhost:8080/invalidate?file=index.html"
    echo ""
}

stop_all() {
    echo -e "${YELLOW}Stopping all CDN components...${NC}"
    for name in edge1 edge2 edge3 load_balancer origin_server metrics_server; do
        stop_server "$name"
    done
    echo -e "${GREEN}All components stopped.${NC}"
}

status_all() {
    echo -e "${YELLOW}CDN Component Status:${NC}"
    for name in metrics_server origin_server edge1 edge2 edge3 load_balancer; do
        check_status "$name"
    done
}

case "${1:-start}" in
    start)   start_all ;;
    stop)    stop_all ;;
    restart) stop_all; sleep 1; start_all ;;
    status)  status_all ;;
    *)       echo "Usage: $0 [start|stop|restart|status]" ;;
esac
