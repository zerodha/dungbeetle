#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f docker-compose.test.yml)
if (echo > /dev/tcp/127.0.0.1/6060) 2>/dev/null; then
    echo "Port 6060 is already in use; stop the existing server first" >&2
    exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
go build -o "$tmp/dungbeetle" ./cmd
pid=""

stop_server() {
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        pid=""
    fi
}

cleanup() {
    stop_server
    "${compose[@]}" down -v
    rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up -d --wait --wait-timeout 120
schema='CREATE TABLE IF NOT EXISTS entries (id BIGINT PRIMARY KEY, amount REAL, user_id VARCHAR(6), entry_date DATE, timestamp TIMESTAMP)'
"${compose[@]}" exec -T postgres psql -U testUser -d testDB -v ON_ERROR_STOP=1 -c "$schema"
"${compose[@]}" exec -T mysql mysql -uroot -prootpassword mydb -e "$schema"

for db in pg mysql; do
    echo "Running integration tests: $db"
    "$tmp/dungbeetle" --config "config.test_$db.toml" --sql-directory="sql/$db" &
    pid=$!
    ready=false
    for ((i=0; i<30; i++)); do
        kill -0 "$pid" || break
        if curl --fail --silent --max-time 1 http://127.0.0.1:6060/tasks > /dev/null; then
            ready=true
            break
        fi
        sleep 1
    done
    if [[ "$ready" != true ]]; then
        echo "Test server failed to start" >&2
        exit 1
    fi
    go test -tags=integration -count=1 -v ./client
    stop_server
done
