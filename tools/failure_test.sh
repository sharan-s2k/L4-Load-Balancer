#!/usr/bin/env bash
set -euo pipefail
BUILD_DIR="${1:-build}"
cleanup(){ kill "${LB:-}" "${B1:-}" "${B2:-}" "${B3:-}" 2>/dev/null || true; }
trap cleanup EXIT
"$BUILD_DIR/echo-backend" 9001 backend-1 >/tmp/lb-b1.log 2>&1 & B1=$!
"$BUILD_DIR/echo-backend" 9002 backend-2 >/tmp/lb-b2.log 2>&1 & B2=$!
"$BUILD_DIR/echo-backend" 9003 backend-3 >/tmp/lb-b3.log 2>&1 & B3=$!
"$BUILD_DIR/tcp-load-balancer" \
  --listen 127.0.0.1:8080 \
  --admin-listen 127.0.0.1:9090 \
  --backend backend-1=127.0.0.1:9001 \
  --backend backend-2=127.0.0.1:9002 \
  --backend backend-3=127.0.0.1:9003 \
  --algorithm least-connections >/tmp/lb-main.log 2>&1 & LB=$!
sleep 1
echo "Before failure"
for _ in $(seq 1 6); do printf 'hello\n' | nc -w 1 127.0.0.1 8080; done
echo "Killing backend-2"
kill "$B2"; sleep 3
echo "After failure"
for _ in $(seq 1 6); do printf 'hello\n' | nc -w 1 127.0.0.1 8080; done
echo "Relevant metrics"
curl -s http://127.0.0.1:9090/metrics | grep -E 'lb_backend_healthy|lb_backend_failures_total|lb_connections_accepted_total'
