use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::error::Error;
use tracing::info;
use crate::types::OrderbookSnapshot;

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

        // Create regular table first
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS snapshots (
                time TIMESTAMPTZ NOT NULL,
                timestamp BIGINT NOT NULL,
                source VARCHAR(50) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                data JSONB NOT NULL
            );
            "#,
        )
        .execute(&pool)
        .await?;

        // Convert to hypertable with 24-hour chunks
        // Use DO block to handle "already a hypertable" error gracefully
        sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'snapshots'
                ) THEN
                    PERFORM create_hypertable('snapshots', 'time', chunk_time_interval => INTERVAL '24 hours');
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

        // Enable compression on chunks older than 7 days
        sqlx::query(
            r#"
            SELECT add_compression_policy('snapshots', INTERVAL '7 days', if_not_exists => true);
            "#,
        )
        .execute(&pool)
        .await
        .ok(); // Ignore errors if policy already exists

        info!(target: "storage", "TimescaleDB initialized with 24h chunks and compression enabled");

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

    pub async fn get_stats(&self) -> Result<(), Box<dyn Error>> {
        use sqlx::Row;

        let rows = sqlx::query(
            r#"
            SELECT source, COUNT(*) as count, MAX(timestamp) as latest
            FROM snapshots
            GROUP BY source
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        info!(target: "storage", "Database Statistics:");
        for row in rows {
            let source: String = row.get("source");
            let count: i64 = row.get("count");
            let latest: i64 = row.get("latest");
            info!(
                target: "storage",
                source = %source,
                count = count,
                latest_timestamp = latest,
                "Source stats"
            );
        }

        Ok(())
    }
}
