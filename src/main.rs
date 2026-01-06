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
    let db = Arc::new(Db::new("snapshots.db").await?);
    
    // Upbit
    let upbit_symbols = vec!["KRW-USDT".to_string()];
    if let Err(e) = upbit::subscribe(upbit_symbols, db.clone()).await {
        eprintln!("Failed to start Upbit: {}", e);
    }
    
    // Kinvest
    let futures_code = env::var("KINVEST_FUTURES_CODE").unwrap_or_else(|_| "101V9000".to_string());
    if let Err(e) = kinvest::subscribe(futures_code, db.clone()).await {
        eprintln!("Failed to start Kinvest: {}", e);
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
