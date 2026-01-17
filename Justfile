# Kimp Station commands

# Start Grafana dashboard
grafana:
    @mkdir -p .grafana-data/dashboards
    @cp -n grafana/dashboards/*.json .grafana-data/dashboards/ 2>/dev/null || true
    @echo "Starting Grafana at http://localhost:3001"
    @echo "Login: admin / admin"
    grafana server \
        --homepath=${GRAFANA_HOME:-$(dirname $(which grafana))/../share/grafana} \
        --config=/dev/null \
        cfg:paths.data=.grafana-data \
        cfg:paths.logs=.grafana-data/logs \
        cfg:paths.provisioning=grafana/provisioning \
        cfg:server.http_port=3001 \
        cfg:security.admin_user=admin \
        cfg:security.admin_password=admin \
        cfg:users.allow_sign_up=false

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
