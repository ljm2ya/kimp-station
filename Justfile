# Kimp Station commands

set dotenv-load

# Start everything (Grafana + Scraper)
start:
    #!/usr/bin/env bash
    set -a; source .env 2>/dev/null || true; set +a
    GRAFANA_PORT=${GRAFANA_PORT:-3001}
    echo "Starting Kimp Station..."

    # Start Grafana
    mkdir -p .grafana-data/dashboards .grafana-data/logs .grafana-data/plugins
    cp -n grafana/dashboards/*.json .grafana-data/dashboards/ 2>/dev/null || true
    nohup grafana server \
        --homepath="$(dirname $(dirname $(which grafana)))/share/grafana" \
        --config=/dev/null \
        "cfg:paths.data=$PWD/.grafana-data" \
        "cfg:paths.logs=$PWD/.grafana-data/logs" \
        "cfg:paths.plugins=$PWD/.grafana-data/plugins" \
        "cfg:paths.provisioning=$PWD/grafana/provisioning" \
        "cfg:server.http_port=$GRAFANA_PORT" \
        "cfg:server.http_addr=0.0.0.0" \
        "cfg:security.admin_user=admin" \
        "cfg:security.admin_password=admin" \
        "cfg:users.allow_sign_up=false" \
        > .grafana-data/logs/stdout.log 2>&1 &

    sleep 2

    # Start scraper
    nohup cargo run --release > .scraper.log 2>&1 &

    echo ""
    echo "✓ Grafana: http://localhost:$GRAFANA_PORT (admin/admin)"
    echo "✓ Scraper: Running (log: .scraper.log)"
    echo ""
    echo "Stop with: just stop"

# Stop everything
stop:
    #!/usr/bin/env bash
    pkill -f "grafana server" 2>/dev/null || true
    # Kill the kimp-station binary specifically (not matching postgres paths)
    pgrep -f 'target/(release|debug)/kimp-station' | xargs -r kill 2>/dev/null || true
    pkill -f "cargo run.*kimp" 2>/dev/null || true
    echo "Stopped all services"

restart: stop start

# Start Grafana dashboard
grafana:
    #!/usr/bin/env bash
    set -a; source .env 2>/dev/null || true; set +a
    GRAFANA_PORT=${GRAFANA_PORT:-3001}
    mkdir -p .grafana-data/dashboards .grafana-data/logs .grafana-data/plugins
    cp -n grafana/dashboards/*.json .grafana-data/dashboards/ 2>/dev/null || true
    echo "Starting Grafana at http://localhost:$GRAFANA_PORT"
    echo "Login: admin / admin"
    grafana server \
        --homepath="$(dirname $(dirname $(which grafana)))/share/grafana" \
        --config=/dev/null \
        "cfg:paths.data=$PWD/.grafana-data" \
        "cfg:paths.logs=$PWD/.grafana-data/logs" \
        "cfg:paths.plugins=$PWD/.grafana-data/plugins" \
        "cfg:paths.provisioning=$PWD/grafana/provisioning" \
        "cfg:server.http_port=$GRAFANA_PORT" \
        "cfg:server.http_addr=0.0.0.0" \
        "cfg:security.admin_user=admin" \
        "cfg:security.admin_password=admin" \
        "cfg:users.allow_sign_up=false"

# Stop Grafana
grafana-stop:
    @pkill -f "grafana server" || echo "Grafana not running"

# Run the data collector
run:
    cargo run

# Run in release mode
run-release:
    cargo run --release

# Build the project
build:
    cargo build --release

# Show database stats
db-stats:
    psql "$DATABASE_URL" -c "SELECT source, COUNT(*) FROM trades GROUP BY source; SELECT source, COUNT(*) FROM snapshots GROUP BY source;"
