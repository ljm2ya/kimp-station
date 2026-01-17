use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::error::Error;
use tracing::info;
use crate::types::{OrderbookSnapshot, TradeSnapshot};

pub struct Db {
    pool: Pool<Postgres>,
}

impl Db {
    pub async fn new(database_url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        // Enable TimescaleDB extension
        sqlx::query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            .execute(&pool)
            .await?;

        // Create hypertable (handles migration from non-hypertable if needed)
        sqlx::query(
            r#"
            DO $$
            DECLARE
                is_hypertable BOOLEAN;
                table_exists BOOLEAN;
                has_data BOOLEAN;
            BEGIN
                -- Check if table exists
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'snapshots' AND table_schema = 'public'
                ) INTO table_exists;

                -- Check if it's already a hypertable
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'snapshots'
                ) INTO is_hypertable;

                IF NOT table_exists THEN
                    -- Create fresh table and hypertable
                    CREATE TABLE snapshots (
                        time TIMESTAMPTZ NOT NULL,
                        timestamp BIGINT NOT NULL,
                        source VARCHAR(50) NOT NULL,
                        symbol VARCHAR(50) NOT NULL,
                        data JSONB NOT NULL
                    );
                    PERFORM create_hypertable('snapshots', 'time', chunk_time_interval => INTERVAL '24 hours');
                    RAISE NOTICE 'Created new hypertable: snapshots';

                ELSIF NOT is_hypertable THEN
                    -- Table exists but is not a hypertable - need to migrate
                    SELECT EXISTS (SELECT 1 FROM snapshots LIMIT 1) INTO has_data;

                    IF has_data THEN
                        -- Migrate existing data
                        RAISE NOTICE 'Migrating existing table to hypertable...';
                        ALTER TABLE snapshots RENAME TO snapshots_old;

                        CREATE TABLE snapshots (
                            time TIMESTAMPTZ NOT NULL,
                            timestamp BIGINT NOT NULL,
                            source VARCHAR(50) NOT NULL,
                            symbol VARCHAR(50) NOT NULL,
                            data JSONB NOT NULL
                        );
                        PERFORM create_hypertable('snapshots', 'time', chunk_time_interval => INTERVAL '24 hours');

                        INSERT INTO snapshots SELECT * FROM snapshots_old;
                        DROP TABLE snapshots_old;
                        RAISE NOTICE 'Migration complete';
                    ELSE
                        -- Empty table, just drop and recreate
                        DROP TABLE snapshots;
                        CREATE TABLE snapshots (
                            time TIMESTAMPTZ NOT NULL,
                            timestamp BIGINT NOT NULL,
                            source VARCHAR(50) NOT NULL,
                            symbol VARCHAR(50) NOT NULL,
                            data JSONB NOT NULL
                        );
                        PERFORM create_hypertable('snapshots', 'time', chunk_time_interval => INTERVAL '24 hours');
                        RAISE NOTICE 'Recreated empty table as hypertable';
                    END IF;
                ELSE
                    RAISE NOTICE 'Hypertable snapshots already exists';
                END IF;
            END $$;
            "#,
        )
        .execute(&pool)
        .await?;

        // Create indexes
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots (timestamp);")
            .execute(&pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots (source);")
            .execute(&pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_snapshots_symbol ON snapshots (symbol);")
            .execute(&pool)
            .await?;

        // Enable compression on the hypertable
        sqlx::query(
            r#"
            DO $$
            BEGIN
                -- Enable compression if not already enabled
                IF NOT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'snapshots' AND compression_enabled = true
                ) THEN
                    ALTER TABLE snapshots SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'source, symbol',
                        timescaledb.compress_orderby = 'time DESC'
                    );
                END IF;
            END $$;
            "#,
        )
        .execute(&pool)
        .await
        .ok();

        // Add compression policy for chunks older than 7 days
        sqlx::query(
            r#"
            SELECT add_compression_policy('snapshots', INTERVAL '7 days', if_not_exists => true);
            "#,
        )
        .execute(&pool)
        .await
        .ok();

        info!(target: "storage", "TimescaleDB snapshots initialized with 24h chunks and compression enabled");

        // ============================================
        // Create trades hypertable for trade data
        // ============================================
        sqlx::query(
            r#"
            DO $$
            DECLARE
                is_hypertable BOOLEAN;
                table_exists BOOLEAN;
                has_data BOOLEAN;
            BEGIN
                -- Check if table exists
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'trades' AND table_schema = 'public'
                ) INTO table_exists;

                -- Check if it's already a hypertable
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'trades'
                ) INTO is_hypertable;

                IF NOT table_exists THEN
                    -- Create fresh table and hypertable
                    CREATE TABLE trades (
                        time TIMESTAMPTZ NOT NULL,
                        source VARCHAR(50) NOT NULL,
                        code VARCHAR(50) NOT NULL,
                        trade_price DOUBLE PRECISION NOT NULL,
                        trade_volume DOUBLE PRECISION NOT NULL,
                        ask_bid VARCHAR(10) NOT NULL,
                        timestamp BIGINT NOT NULL,
                        trade_timestamp BIGINT NOT NULL
                    );
                    PERFORM create_hypertable('trades', 'time', chunk_time_interval => INTERVAL '24 hours');
                    RAISE NOTICE 'Created new hypertable: trades';

                ELSIF NOT is_hypertable THEN
                    -- Table exists but is not a hypertable - need to migrate
                    SELECT EXISTS (SELECT 1 FROM trades LIMIT 1) INTO has_data;

                    IF has_data THEN
                        -- Migrate existing data
                        RAISE NOTICE 'Migrating existing trades table to hypertable...';
                        ALTER TABLE trades RENAME TO trades_old;

                        CREATE TABLE trades (
                            time TIMESTAMPTZ NOT NULL,
                            source VARCHAR(50) NOT NULL,
                            code VARCHAR(50) NOT NULL,
                            trade_price DOUBLE PRECISION NOT NULL,
                            trade_volume DOUBLE PRECISION NOT NULL,
                            ask_bid VARCHAR(10) NOT NULL,
                            timestamp BIGINT NOT NULL,
                            trade_timestamp BIGINT NOT NULL
                        );
                        PERFORM create_hypertable('trades', 'time', chunk_time_interval => INTERVAL '24 hours');

                        INSERT INTO trades SELECT * FROM trades_old;
                        DROP TABLE trades_old;
                        RAISE NOTICE 'Migration complete';
                    ELSE
                        -- Empty table, just drop and recreate
                        DROP TABLE trades;
                        CREATE TABLE trades (
                            time TIMESTAMPTZ NOT NULL,
                            source VARCHAR(50) NOT NULL,
                            code VARCHAR(50) NOT NULL,
                            trade_price DOUBLE PRECISION NOT NULL,
                            trade_volume DOUBLE PRECISION NOT NULL,
                            ask_bid VARCHAR(10) NOT NULL,
                            timestamp BIGINT NOT NULL,
                            trade_timestamp BIGINT NOT NULL
                        );
                        PERFORM create_hypertable('trades', 'time', chunk_time_interval => INTERVAL '24 hours');
                        RAISE NOTICE 'Recreated empty table as hypertable';
                    END IF;
                ELSE
                    RAISE NOTICE 'Hypertable trades already exists';
                END IF;
            END $$;
            "#,
        )
        .execute(&pool)
        .await?;

        // Create indexes for trades table
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp);")
            .execute(&pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_source ON trades (source);")
            .execute(&pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_code ON trades (code);")
            .execute(&pool)
            .await?;

        // Enable compression on trades hypertable
        sqlx::query(
            r#"
            DO $$
            BEGIN
                -- Enable compression if not already enabled
                IF NOT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'trades' AND compression_enabled = true
                ) THEN
                    ALTER TABLE trades SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'source, code',
                        timescaledb.compress_orderby = 'time DESC'
                    );
                END IF;
            END $$;
            "#,
        )
        .execute(&pool)
        .await
        .ok();

        // Add compression policy for trades chunks older than 7 days
        sqlx::query(
            r#"
            SELECT add_compression_policy('trades', INTERVAL '7 days', if_not_exists => true);
            "#,
        )
        .execute(&pool)
        .await
        .ok();

        info!(target: "storage", "TimescaleDB trades initialized with 24h chunks and compression enabled");

        Ok(Db { pool })
    }

    pub async fn save_snapshot(&self, ob: &OrderbookSnapshot) -> Result<(), Box<dyn Error>> {
        // Convert timestamp (milliseconds) to TIMESTAMPTZ
        let time = chrono::DateTime::from_timestamp_millis(ob.timestamp)
            .unwrap_or_else(|| chrono::Utc::now());

        // Store asks/bids as JSONB for efficient querying
        let data_json = serde_json::json!({
            "asks": ob.asks,
            "bids": ob.bids
        });

        sqlx::query(
            r#"
            INSERT INTO snapshots (time, timestamp, source, symbol, data)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(time)
        .bind(ob.timestamp)
        .bind(&ob.name)
        .bind(&ob.symbol)
        .bind(data_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn save_trade(&self, trade: &TradeSnapshot) -> Result<(), Box<dyn Error>> {
        // Convert timestamp (milliseconds) to TIMESTAMPTZ
        let time = chrono::DateTime::from_timestamp_millis(trade.timestamp)
            .unwrap_or_else(|| chrono::Utc::now());

        sqlx::query(
            r#"
            INSERT INTO trades (time, source, code, trade_price, trade_volume, ask_bid, timestamp, trade_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(time)
        .bind(&trade.source)
        .bind(&trade.code)
        .bind(trade.trade_price)
        .bind(trade.trade_volume)
        .bind(&trade.ask_bid)
        .bind(trade.timestamp)
        .bind(trade.trade_timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_stats(&self) -> Result<(), Box<dyn Error>> {
        use sqlx::Row;

        // Snapshots stats
        let rows = sqlx::query(
            r#"
            SELECT source, COUNT(*) as count, MAX(timestamp) as latest
            FROM snapshots
            GROUP BY source
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        info!(target: "storage", "Database Statistics - Snapshots:");
        for row in rows {
            let source: String = row.get("source");
            let count: i64 = row.get("count");
            let latest: i64 = row.get("latest");
            info!(
                target: "storage",
                source = %source,
                count = count,
                latest_timestamp = latest,
                "Snapshot stats"
            );
        }

        // Trades stats
        let trade_rows = sqlx::query(
            r#"
            SELECT source, COUNT(*) as count, MAX(timestamp) as latest
            FROM trades
            GROUP BY source
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        info!(target: "storage", "Database Statistics - Trades:");
        for row in trade_rows {
            let source: String = row.get("source");
            let count: i64 = row.get("count");
            let latest: i64 = row.get("latest");
            info!(
                target: "storage",
                source = %source,
                count = count,
                latest_timestamp = latest,
                "Trade stats"
            );
        }

        Ok(())
    }
}
