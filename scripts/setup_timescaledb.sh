#!/usr/bin/env bash
# TimescaleDB Setup Script for Kimp Station
# Handles both local development (Nix) and system-wide setup

set -e

# Default Configuration
DB_NAME="kimp_station"
DB_USER="kimp_user"
DB_PASSWORD="${KIMP_DB_PASSWORD:-kimp_password}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🚀 TimescaleDB Setup for Kimp Station"
echo "======================================"

# Determine environment mode
if command -v pg_ctl &> /dev/null; then
    # Local mode (Nix or manual local setup)
    if [ -z "$PGDATA" ]; then
        # Check specifically for our nix local convention defaults
        export PGDATA="$PWD/.postgres-data"
        export PGHOST="$PWD/.postgres-socket"
        export PGPORT="5432"
        export PGDATABASE="$DB_NAME"
        echo "🔧 Local environment detected (auto-configuring defaults)"
    else
        echo "🔧 Local environment detected (using present PGDATA: $PGDATA)"
    fi
    IS_LOCAL="true"
else
    # System mode (Global Postgres)
    IS_LOCAL="false"
    echo "🔧 System environment assumed (using sudo -u postgres)"
fi

setup_local() {
    # Initialize PostgreSQL if not already done
    if [ ! -d "$PGDATA" ]; then
        echo "📦 Initializing local PostgreSQL database..."
        initdb -D "$PGDATA" --no-locale --encoding=UTF8

        # Configure PostgreSQL
        echo "unix_socket_directories = '$PGHOST'" >> "$PGDATA/postgresql.conf"
        echo "listen_addresses = '127.0.0.1'" >> "$PGDATA/postgresql.conf"
        echo "port = $PGPORT" >> "$PGDATA/postgresql.conf"
        # Add TimescaleDB to shared_preload_libraries
        echo "shared_preload_libraries = 'timescaledb'" >> "$PGDATA/postgresql.conf"
    fi

    # Start PostgreSQL if not running
    if ! pg_isready -q; then
        echo "🔄 Starting PostgreSQL server..."
        mkdir -p "$PGHOST"
        pg_ctl start -D "$PGDATA" -l "$PGDATA/logfile" -o "-k $PGHOST"
        sleep 2
    else
        echo "✅ PostgreSQL already running"
    fi

    # Create database if it doesn't exist
    if ! psql -lqt | cut -d \| -f 1 | grep -qw "$PGDATABASE"; then
        echo "📊 Creating database..."
        createdb "$PGDATABASE"
        psql -d "$PGDATABASE" -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
        echo -e "${GREEN}✅ Database created with TimescaleDB extension${NC}"
    else
        echo -e "${GREEN}✅ Database already exists${NC}"
    fi

    # Configure User and Permissions (Always run this to ensure correctness)
    echo "👤 Configuring user permissions..."
    psql -d "$PGDATABASE" <<EOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$DB_USER') THEN
    CREATE USER $DB_USER WITH ENCRYPTED PASSWORD '$DB_PASSWORD';
  END IF;
END
\$\$;

-- Grant permissions (Idempotent)
GRANT ALL PRIVILEGES ON DATABASE "$PGDATABASE" TO $DB_USER;
GRANT ALL ON SCHEMA public TO $DB_USER;
EOF
    echo -e "${GREEN}✅ User '$DB_USER' configured and permissions granted${NC}"

    # Create .env
    create_env "postgresql://$DB_USER:$DB_PASSWORD@localhost:$PGPORT/$PGDATABASE?host=$PGHOST"
    
    # Setup exit trap is handled by shell.nix
    echo "ℹ️  Local Postgres is running."
    echo "    (Auto-stops on shell exit, or use: pg_ctl stop -D .postgres-data)"
}

setup_system() {
    # Check requirements
    if ! command -v psql &> /dev/null; then
        echo "❌ PostgreSQL not found. Please install postgresql."
        exit 1
    fi
    
    # Check TimescaleDB availability (requires sudo)
    if ! sudo -u postgres psql -c "SELECT * FROM pg_available_extensions WHERE name = 'timescaledb';" | grep -q timescaledb; then
         echo "❌ TimescaleDB extension not found."
         exit 1
    fi

    echo "📦 Creating/Resetting database: $DB_NAME"
    sudo -u postgres psql <<EOF
-- Drop database if exists (optional, maybe ask user?)
-- DROP DATABASE IF EXISTS $DB_NAME;
-- DROP USER IF EXISTS $DB_USER;

-- Create user if not exists
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$DB_USER') THEN
    CREATE USER $DB_USER WITH ENCRYPTED PASSWORD '$DB_PASSWORD';
  END IF;
END
\$\$;

-- Create database if not exists
SELECT 'CREATE DATABASE $DB_NAME'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')\\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;

\c $DB_NAME

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO $DB_USER;

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
\q
EOF

    echo -e "${GREEN}✅ System Database setup complete${NC}"
    create_env "postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME"
}

create_env() {
    local db_url="$1"
    if [ -f .env ]; then
        echo "ℹ️  .env file already exists. Skipping creation to prevent overwrite."
        echo "    If you need to update the configuration, here is the connection string:"
        echo "    DATABASE_URL=$db_url"
    else
        echo "📝 Creating .env file..."
        cat > .env <<EOF
# TimescaleDB Configuration
DATABASE_URL=$db_url

# Korea Investment (KIS) API Credentials
KINVEST_API_KEY=your_api_key_here
KINVEST_SECRET_KEY=your_secret_key_here
KINVEST_FUTURES_CODE=101V9000
EOF
        echo -e "${YELLOW}⚠️  Please update .env with your credentials${NC}"
    fi
}

# Main Execution Logic
if [ "$IS_LOCAL" = "true" ]; then
    setup_local
else
    setup_system
fi

echo ""
echo "🎉 Setup complete!"
echo "Run: cargo run"