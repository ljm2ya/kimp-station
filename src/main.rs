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
    
    // Fetch approval key once to be shared across connections
    // This prevents race conditions where generating a new key invalidates the previous one
    let approval_key = match kinvest::fetch_approval_key().await {
        Ok(key) => key,
        Err(e) => {
            eprintln!("[Kinvest] Failed to fetch approval key: {}", e);
            String::new()
        }
    };

    // Subscribe to both day and night markets
    // The server will only send data during active market hours
    println!("[Kinvest] Subscribing to both day and night markets...");

    let mut subscriptions = Vec::new();
    subscriptions.push((futures_code.clone(), "H0CFASP0".to_string())); // Day
    subscriptions.push((futures_code.clone(), "H0MFASP0".to_string())); // Night

    if let Err(e) = kinvest::start_stream(subscriptions, db.clone(), approval_key).await {
        eprintln!("[Kinvest] Subscription stream error: {}", e);
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
