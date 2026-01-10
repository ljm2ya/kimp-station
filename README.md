# Kimp Station

Real-time market data collector for **Upbit** (KRW-USDT) and **Korea Investment** (Commodity Futures) using WebSockets and TimescaleDB.

## Features

- **Upbit**: Subscribes to `KRW-USDT` (24/7).
- **Kinvest**: Dual subscription to day/night markets with server-managed hours.
- **Storage**: TimescaleDB hypertable with 24h chunks and auto-compression.
- **Resilience**: Heartbeat-enabled WebSocket connections.
- **Data Quality**: Filters zero-price snapshots (market transitions) and duplicate orderbooks.

## Prerequisites

- **Rust**: Toolchain installed.
- **PostgreSQL**: v12+ with TimescaleDB v2+ extension.
- **API Keys**: Korea Investment (KIS) API credentials.

## Configuration

### Quick Setup with Nix (Recommended)

**Easiest setup:**

```bash
# 1. Enter environment (auto-installs DB & Tools)
nix-shell

# 2. Copy .env.example to .env
cp .env.example .env

# 3. Add API keys
vim .env

# 4. Validation (Optional)
./test-timescaledb.sh

# 5. Run
cargo run
```

**Database Management:**
- **Connect**: `pgcli` or `psql`
- **Restart**: `pg_ctl restart -D .postgres-data`
- **Reset**: Exit shell, then `rm -rf .postgres-data .postgres-socket`

### Quick Setup (Script)

```bash
./scripts/setup_timescaledb.sh
# Default DB Password: 'kimp_password'
# Then edit .env with credentials
```

### Manual Setup

1. **Install**: Postgres + TimescaleDB extension.
2. **Database**:
   ```sql
   CREATE DATABASE kimp_station;
   -- Set your own password
   CREATE USER kimp_user WITH ENCRYPTED PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE kimp_station TO kimp_user;
   \c kimp_station
   CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
   ```
3. **Configure**: Copy `.env.example` to `.env` and edit.
   ```ini
   DATABASE_URL=postgresql://kimp_user:your_secure_password@localhost:5432/kimp_station
   ```

## Usage

```bash
cargo run
# Release build
cargo run --release
```

## External Database Access

To allow external connections (e.g., for visualization tools like Grafana/Tableau), it is **strongly recommended** to whitelist only specific IP addresses.

1.  **Edit `postgresql.conf`** (in `.postgres-data/`):
    ```ini
    listen_addresses = '*' 
    ```
2.  **Edit `pg_hba.conf`** (in `.postgres-data/`):
    Add the following line to the end to allow password auth **only** from your specific IP (replace `YOUR.IP.GOES.HERE`):
    ```
    # Allow access from specific IP (e.g., 203.0.113.5/32)
    host    all             all             YOUR.IP.GOES.HERE/32            scram-sha-256
    ```
    *Avoid using `0.0.0.0/0` (any IP) unless absolutely necessary and behind a firewall.*

3.  **Restart Database**:
    ```bash
    pg_ctl restart -D .postgres-data
    ```

**Security Warning**: Never expose your database to the public internet without IP restrictions and strong passwords.

## Market Hours

- **Upbit**: 24/7.
- **Kinvest Day**: 08:45-15:45 KST (`H0CFASP0`).
- **Kinvest Night**: 18:00-06:00 KST (`H0MFASP0`).

## Database Schema

Using TimescaleDB hypertable:

```sql
CREATE TABLE snapshots (
    time TIMESTAMPTZ NOT NULL,
    timestamp BIGINT NOT NULL,
    source VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    data JSONB NOT NULL
);

SELECT create_hypertable('snapshots', 'time', chunk_time_interval => INTERVAL '24 hours');
CREATE INDEX idx_snapshots_timestamp ON snapshots (timestamp);
SELECT add_compression_policy('snapshots', INTERVAL '7 days');
```

The `data` column stores efficiently queryable JSON:

```json
{
  "symbol": "...",
  "time": "...",
  "asks": [{ "price": "...", "vol": "..." }, ...],
  "bids": [{ "price": "...", "vol": "..." }, ...]
}
```

## Querying Data

You can interactively query the database using `pgcli` (pre-installed in the Nix shell).

1. **Connect**:
   ```bash
   pgcli
   ```

2. **Example Queries**:

   **View latest snapshots:**
   ```sql
   SELECT time, source, symbol FROM snapshots ORDER BY time DESC LIMIT 5;
   ```

   **Extract prices from JSON (Best Ask Price):**
   ```sql
   SELECT time, 
          symbol, 
          (data->'asks'->0->>'price')::numeric AS best_ask 
   FROM snapshots 
   WHERE source = 'upbit' 
   ORDER BY time DESC 
   LIMIT 5;
   ```

   **TimescaleDB Aggregation (1-minute average):**
   ```sql
   SELECT time_bucket('1 minute', time) AS bucket,
          avg((data->'asks'->0->>'price')::numeric) AS avg_price
   FROM snapshots
   WHERE symbol = 'KRW-USDT'
   GROUP BY bucket
   ORDER BY bucket DESC;
   ```
