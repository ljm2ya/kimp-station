# Kimp Station commands

# Start Grafana dashboard
grafana:
    docker compose up -d
    @echo ""
    @echo "Grafana: http://localhost:3001"
    @echo "Login: admin / admin"

# Stop Grafana
grafana-stop:
    docker compose down

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
