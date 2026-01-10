use futures_util::{StreamExt, SinkExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use std::error::Error;
use std::sync::Arc;
use reqwest::Client;
use std::env;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

use crate::types::{OrderbookSnapshot, OrderbookItem};
use crate::storage::Db;

const WS_URL_REAL: &str = "ws://ops.koreainvestment.com:21000";
const WS_URL_MOCK: &str = "ws://ops.koreainvestment.com:31000";

// TR IDs
const TR_COMMODITY_FUTURES_ORDERBOOK: &str = "H0CFASP0";
const TR_NIGHTTIME_FUTURES_ORDERBOOK: &str = "H0MFASP0";

// Error codes that require key refresh
const ERR_INVALID_APPROVAL: &str = "OPSP0011";

/// Connection result indicating whether to refresh the approval key
#[derive(Debug)]
enum ConnectionResult {
    /// Normal close or network error - retry with same key
    Retry,
    /// Approval key invalid - need to fetch new key
    RefreshKey,
}

pub async fn fetch_approval_key() -> Result<String, Box<dyn Error + Send + Sync>> {
    let api_key = env::var("KINVEST_API_KEY").unwrap_or_default();
    let secret_key = env::var("KINVEST_SECRET_KEY").unwrap_or_default();

    if api_key.is_empty() {
        return Err("KINVEST_API_KEY not set".into());
    }

    let is_mock = false;
    let base_url = if is_mock {
        "https://openapivts.koreainvestment.com:29443"
    } else {
        "https://openapi.koreainvestment.com:9443"
    };

    get_approval_key(base_url, &api_key, &secret_key).await
}

pub async fn start_stream(
    subscriptions: Vec<(String, String)>, // (futures_code, tr_id)
    db: Arc<Db>,
) -> Result<(), Box<dyn Error>> {
    let subscriptions = Arc::new(subscriptions);

    tokio::spawn(async move {
        let mut approval_key = String::new();
        let mut consecutive_key_failures = 0;
        const MAX_KEY_FAILURES: u32 = 5;

        loop {
            // Fetch approval key if empty or after RefreshKey result
            if approval_key.is_empty() {
                info!(target: "kinvest", "Fetching new approval key...");
                match fetch_approval_key().await {
                    Ok(key) => {
                        info!(target: "kinvest", "Approval key obtained successfully");
                        approval_key = key;
                        consecutive_key_failures = 0;
                    }
                    Err(e) => {
                        consecutive_key_failures += 1;
                        error!(
                            target: "kinvest",
                            error = %e,
                            failures = consecutive_key_failures,
                            "Failed to fetch approval key"
                        );

                        if consecutive_key_failures >= MAX_KEY_FAILURES {
                            error!(
                                target: "kinvest",
                                "Max key fetch failures reached ({}), waiting 5 minutes before retry",
                                MAX_KEY_FAILURES
                            );
                            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
                            consecutive_key_failures = 0;
                        } else {
                            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                        }
                        continue;
                    }
                }
            }

            info!(
                target: "kinvest",
                subscriptions = subscriptions.len(),
                "Connecting to websocket..."
            );

            // Attempt connection
            match connect_and_handle(&subscriptions, &db, &approval_key).await {
                Ok(ConnectionResult::Retry) => {
                    info!(target: "kinvest", "Connection closed. Reconnecting in 5s...");
                }
                Ok(ConnectionResult::RefreshKey) => {
                    warn!(target: "kinvest", "Approval key invalid. Refreshing key and reconnecting in 5s...");
                    approval_key.clear(); // Clear key to trigger refresh on next iteration
                }
                Err(e) => {
                    error!(target: "kinvest", error = %e, "Connection error. Reconnecting in 5s...");
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    Ok(())
}

async fn connect_and_handle(
    subscriptions: &[(String, String)],
    db: &Arc<Db>,
    approval_key: &str,
) -> Result<ConnectionResult, Box<dyn Error + Send + Sync>> {
    let is_mock = false;
    let ws_url = if is_mock { WS_URL_MOCK } else { WS_URL_REAL };

    let url = Url::parse(ws_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    info!(target: "kinvest", "Websocket connected");

    // Channel for writing to WS
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    // Spawn writer task
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Send all subscriptions
    for (futures_code, tr_id) in subscriptions {
        let req_body = json!({
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": futures_code
                }
            }
        });

        tx.send(Message::Text(req_body.to_string())).await?;
        info!(
            target: "kinvest",
            futures_code = %futures_code,
            tr_id = %tr_id,
            "Sent subscription request"
        );

        // Small delay to prevent rate limiting
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Reader loop
    let tx_reader = tx.clone();
    let mut result = ConnectionResult::Retry;
    let mut last_snapshot: Option<OrderbookSnapshot> = None;

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Some(msg_result) = handle_msg(&text, db, &tx_reader, &mut last_snapshot).await {
                    result = msg_result;
                    break;
                }
            }
            Ok(Message::Binary(data)) => {
                if let Ok(text) = String::from_utf8(data) {
                    if let Some(msg_result) = handle_msg(&text, db, &tx_reader, &mut last_snapshot).await {
                        result = msg_result;
                        break;
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!(target: "kinvest", "Received close frame");
                break;
            }
            Ok(Message::Ping(_)) => {
                let _ = tx_reader.send(Message::Pong(vec![])).await;
            },
            Err(e) => {
                error!(target: "kinvest", error = %e, "Websocket error");
                return Err(Box::new(e));
            }
            _ => {}
        }
    }

    // Ensure writer task is aborted
    write_task.abort();

    Ok(result)
}

async fn get_approval_key(base_url: &str, app_key: &str, secret_key: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let resp = client.post(format!("{}/oauth2/Approval", base_url))
        .header("Content-Type", "application/json")
        .json(&json!({
            "grant_type": "client_credentials",
            "appkey": app_key,
            "secretkey": secret_key
        }))
        .send()
        .await?;

    let body: Value = resp.json().await?;
    match body["approval_key"].as_str() {
        Some(key) => Ok(key.to_string()),
        None => Err(format!("Failed to get approval key: {:?}", body).into())
    }
}

/// Handle incoming message. Returns Some(ConnectionResult) if connection should be closed.
async fn handle_msg(
    text: &str,
    db: &Arc<Db>,
    tx: &mpsc::Sender<Message>,
    last_snapshot: &mut Option<OrderbookSnapshot>,
) -> Option<ConnectionResult> {
    // Try to parse as JSON first (modern KIS API format)
    if text.trim().starts_with('{') {
        if let Ok(json_msg) = serde_json::from_str::<Value>(text) {
            // Check for PINGPONG heartbeat from server
            if let Some(tr_id) = json_msg["header"]["tr_id"].as_str() {
                if tr_id == "PINGPONG" {
                    let pong_response = json!({
                        "header": { "tr_id": "PINGPONG" }
                    });
                    if let Err(e) = tx.send(Message::Text(pong_response.to_string())).await {
                        error!(target: "kinvest", error = %e, "Failed to send PINGPONG response");
                    }
                    return None;
                }

                // Handle response messages
                if let Some(body) = json_msg["body"].as_object() {
                    // Check for success
                    if body.get("msg1").and_then(|v| v.as_str()) == Some("SUBSCRIBE SUCCESS") {
                        info!(target: "kinvest", tr_id = %tr_id, "Subscription confirmed");
                        return None;
                    }

                    // Check for errors
                    if let Some(msg_cd) = body.get("msg_cd").and_then(|v| v.as_str()) {
                        let msg1 = body.get("msg1").and_then(|v| v.as_str()).unwrap_or("unknown");

                        // Check for invalid approval error
                        if msg_cd == ERR_INVALID_APPROVAL {
                            error!(
                                target: "kinvest",
                                msg_cd = %msg_cd,
                                msg1 = %msg1,
                                tr_id = %tr_id,
                                "Invalid approval key detected - will refresh"
                            );
                            return Some(ConnectionResult::RefreshKey);
                        }

                        // Log other errors
                        warn!(
                            target: "kinvest",
                            msg_cd = %msg_cd,
                            msg1 = %msg1,
                            tr_id = %tr_id,
                            "Received error message"
                        );
                    }
                }
            }
            return None;
        }
    }

    // Parse delimited data format
    let parts: Vec<&str> = if text.contains('|') {
        text.split('|').collect()
    } else {
        text.split('^').collect()
    };

    if parts.len() < 4 {
        return None;
    }

    let tr_id = parts[1]; // Index 1 is TR_ID

    if tr_id == TR_COMMODITY_FUTURES_ORDERBOOK || tr_id == TR_NIGHTTIME_FUTURES_ORDERBOOK {
        let data_part = parts[3];
        let data_fields: Vec<&str> = data_part.split('^').collect();

        // 5 levels * 6 blocks = 30 fields + code + time = 32 fields minimum
        if data_fields.len() >= 32 {
            let extracted_code = data_fields[0];

            let mut asks = Vec::new();
            let mut bids = Vec::new();

            for i in 0..5 {
                // Asks: Price from Block 1 (idx 2), Size from Block 5 (idx 22)
                let ap_idx = 2 + i;
                let as_idx = 22 + i;

                if as_idx < data_fields.len() {
                    if let (Ok(p), Ok(s)) = (data_fields[ap_idx].parse::<f64>(), data_fields[as_idx].parse::<f64>()) {
                        asks.push(OrderbookItem { price: p, size: s });
                    }
                }

                // Bids: Price from Block 2 (idx 7), Size from Block 6 (idx 27)
                let bp_idx = 7 + i;
                let bs_idx = 27 + i;

                if bs_idx < data_fields.len() {
                    if let (Ok(p), Ok(s)) = (data_fields[bp_idx].parse::<f64>(), data_fields[bs_idx].parse::<f64>()) {
                        bids.push(OrderbookItem { price: p, size: s });
                    }
                }
            }

            let timestamp = chrono::Utc::now().timestamp_millis();

            let standardized = OrderbookSnapshot {
                name: "kinvest".to_string(),
                timestamp,
                symbol: extracted_code.to_string(),
                asks,
                bids,
            };

            // Filter out invalid orderbooks (zero prices during market transitions)
            if !standardized.has_valid_prices() {
                debug!(
                    target: "kinvest",
                    symbol = %extracted_code,
                    "Skipping snapshot with zero prices (market transition)"
                );
                return None;
            }

            // Skip if orderbook is identical to previous one
            if let Some(ref last) = last_snapshot {
                if standardized.is_same_data(last) {
                    return None;
                }
            }

            // Update cache with current snapshot
            *last_snapshot = Some(standardized.clone());

            if let Err(e) = db.save_snapshot(&standardized).await {
                error!(
                    target: "kinvest",
                    error = %e,
                    symbol = %extracted_code,
                    "Failed to save snapshot"
                );
            } else {
                debug!(
                    target: "kinvest",
                    symbol = %extracted_code,
                    tr_id = %tr_id,
                    "Saved orderbook snapshot"
                );
            }
        }
    }

    None
}
