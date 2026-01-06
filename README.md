# Kimp Station

This application subscribes to real-time market data from **Upbit** (KRW-USDT) and **Korea Investment** (Commodity Futures) via WebSockets and stores orderbook snapshots into a SQLite database.

## Features

- **Upbit Subscription**: Subscribes to the `KRW-USDT` orderbook stream.
- **Kinvest Subscription**: Subscribes to Commodity Futures (default: USD Futures) orderbook stream.
- **Data Storage**: Stores the top 10 levels of bids and asks for each snapshot in a local SQLite database (`snapshots.db`).
- **Resilience**: Includes heartbeat mechanisms for WebSocket connections.

## Prerequisites

- **Rust**: Ensure you have the Rust toolchain installed.
- **API Keys**: You need a valid API Key and Secret Key for Korea Investment (KIS) API.

## Configuration

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` and fill in your Kinvest credentials:
   ```ini
   KINVEST_API_KEY=your_actual_api_key
   KINVEST_SECRET_KEY=your_actual_secret_key
   KINVEST_FUTURES_CODE=101V9000
   ```

## Usage

Run the application using Cargo:

```bash
cargo run
```

To build for release:

```bash
cargo build --release
./target/release/kimp-station
```

## Database Schema
    
The application creates a SQLite database file named `snapshots.db` with the following schema:

```sql
CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,  -- Unix timestamp (ms)
    source TEXT,        -- "upbit" or "kinvest"
    symbol TEXT,        -- e.g., "KRW-USDT" or "101V9000"
    data TEXT           -- JSON blob containing top 10 orderbook units
);
```

### Data JSON Format

The `data` column contains a JSON object with the following structure:

```json
{
  "symbol": "...",
  "time": "...",
  "current_price": "...",
  "asks": [
    { "price": "...", "vol": "..." },
    ...
  ],
  "bids": [
    { "price": "...", "vol": "..." },
    ...
  ]
}
```
