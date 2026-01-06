mod storage;
mod types;
mod upbit;
mod kinvest;

use std::error::Error;
use std::sync::Arc;
use tokio::signal;
use dotenv::dotenv;
use std::env;

use crate::storage::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    // Initialize DB
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env file");
    let db = Arc::new(Db::new(&database_url).await?);

    // Start Upbit (runs 24/7)
    let upbit_symbols = vec!["KRW-USDT".to_string()];
    if let Err(e) = upbit::subscribe(upbit_symbols, db.clone()).await {
        eprintln!("Failed to start Upbit: {}", e);
    }

    // Get Kinvest configuration
    let futures_code = env::var("KINVEST_FUTURES_CODE").unwrap_or_else(|_| "A75601".to_string());

    // Subscribe to both day and night markets
    // The server will only send data during active market hours
    println!("[Kinvest] Subscribing to both day and night markets...");

    // Day market subscription
    let db_day = db.clone();
    let futures_code_day = futures_code.clone();
    if let Err(e) = kinvest::subscribe(futures_code_day, "H0CFASP0", db_day).await {
        eprintln!("[Kinvest] Day market subscription error: {}", e);
    }

    // Night market subscription
    let db_night = db.clone();
    if let Err(e) = kinvest::subscribe(futures_code, "H0MFASP0", db_night).await {
        eprintln!("[Kinvest] Night market subscription error: {}", e);
    }

    println!("Station running. Press Ctrl+C to stop.");
    signal::ctrl_c().await?;
    println!("\nShutting down...");

    // Show database statistics
    if let Err(e) = db.get_stats().await {
        eprintln!("Failed to get stats: {}", e);
    }

    Ok(())
}
