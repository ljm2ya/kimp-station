mod storage;
mod types;
mod upbit;
mod kinvest;

use std::error::Error;
use std::sync::Arc;
use tokio::signal;
use dotenv::dotenv;
use std::env;
use tracing::{info, error, Level};
use tracing_subscriber::fmt::time::ChronoLocal;

use crate::storage::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    // Initialize tracing with timestamps
    tracing_subscriber::fmt()
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f".to_string()))
        .with_max_level(Level::INFO)
        .with_target(true)
        .init();

    info!("Starting kimp-station...");

    // Initialize DB
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env file");
    let db = Arc::new(Db::new(&database_url).await?);

    // Start Upbit (runs 24/7)
    let upbit_symbols: Vec<String> = env::var("UPBIT_SYMBOLS")
        .unwrap_or_else(|_| "KRW-USDT,KRW-BTC,KRW-ETH,KRW-XRP,BTC-USDT,USDT-BTC".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if let Err(e) = upbit::subscribe(upbit_symbols, db.clone()).await {
        error!(error = %e, "Failed to start Upbit");
    }

    // Get Kinvest configuration
    let futures_code = env::var("KINVEST_FUTURES_CODE").unwrap_or_else(|_| "A75601".to_string());

    // Subscribe to both day and night markets
    // Approval key will be fetched/refreshed inside the stream loop
    info!(futures_code = %futures_code, "Subscribing to Kinvest day and night markets");

    let mut subscriptions = Vec::new();
    subscriptions.push((futures_code.clone(), "H0CFASP0".to_string())); // Day
    subscriptions.push((futures_code.clone(), "H0MFASP0".to_string())); // Night

    if let Err(e) = kinvest::start_stream(subscriptions, db.clone()).await {
        error!(error = %e, "Kinvest subscription stream error");
    }

    info!("Station running. Press Ctrl+C to stop.");
    signal::ctrl_c().await?;
    info!("Shutting down...");

    // Show database statistics
    if let Err(e) = db.get_stats().await {
        error!(error = %e, "Failed to get stats");
    }

    Ok(())
}
