use sqlx::{sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Pool, Sqlite};
use std::error::Error;
use std::str::FromStr;
use crate::types::OrderbookSnapshot;

pub struct Db {
    pool: Pool<Sqlite>,
}

impl Db {
    pub async fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", path))?
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                source TEXT,
                symbol TEXT,
                data TEXT
            );
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Db { pool })
    }

    pub async fn save_snapshot(&self, ob: &OrderbookSnapshot) -> Result<(), Box<dyn Error>> {
        // Optimize: Only store asks/bids in JSON since other fields are in columns
        let data_json = serde_json::json!({
            "asks": ob.asks,
            "bids": ob.bids
        }).to_string();

        sqlx::query(
            r#"
            INSERT INTO snapshots (timestamp, source, symbol, data)
            VALUES (?, ?, ?, ?)
            "#,
        )
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

        println!("\n📊 Database Statistics:");
        println!("{}", "=".repeat(50));
        for row in rows {
            let source: String = row.get("source");
            let count: i64 = row.get("count");
            let latest: i64 = row.get("latest");
            println!("\n🔹 Source: {}", source);
            println!("   Count: {}", count);
            println!("   Latest timestamp: {}", latest);
        }
        println!("\n{}", "=".repeat(50));

        Ok(())
    }
}
