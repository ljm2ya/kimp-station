use futures_util::{StreamExt, SinkExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use std::error::Error;
use std::sync::Arc;
use reqwest::Client;
use std::env;
use tokio::sync::mpsc;

use crate::types::{OrderbookSnapshot, OrderbookItem};
use crate::storage::Db;

const WS_URL_REAL: &str = "ws://ops.koreainvestment.com:21000";
const WS_URL_MOCK: &str = "ws://ops.koreainvestment.com:31000";

// TR IDs
const TR_COMMODITY_FUTURES_ORDERBOOK: &str = "H0CFASP0";
const TR_NIGHTTIME_FUTURES_ORDERBOOK: &str = "H0MFASP0";

pub async fn subscribe(
    futures_code: String,
    db: Arc<Db>,
) -> Result<(), Box<dyn Error>> {
    let api_key = env::var("KINVEST_API_KEY").unwrap_or_default();
    let secret_key = env::var("KINVEST_SECRET_KEY").unwrap_or_default();
    
    if api_key.is_empty() {
        println!("KINVEST_API_KEY not set, skipping Kinvest subscription");
        return Ok(());
    }

    let is_mock = false;  
    let base_url = if is_mock { "https://openapivts.koreainvestment.com:29443" } else { "https://openapi.koreainvestment.com:9443" };
    let ws_url = if is_mock { WS_URL_MOCK } else { WS_URL_REAL };

    // Get Approval Key
    let approval_key = get_approval_key(base_url, &api_key, &secret_key).await?;
    println!("Got Kinvest Approval Key: {}", approval_key);

    let url = Url::parse(ws_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Create a channel to send messages to the WebSocket writer
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    // Spawn writer task
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write.send(msg).await {
                eprintln!("Kinvest write error: {}", e);
                break;
            }
        }
    });

    // Subscribe
    let req_body = json!({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": TR_NIGHTTIME_FUTURES_ORDERBOOK,
                "tr_key": futures_code
            }
        }
    });

    tx.send(Message::Text(req_body.to_string())).await?;
    println!("Subscribed to Kinvest: {}", futures_code);

    // Reader task
    let tx_reader = tx.clone();
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    handle_msg(&text, &futures_code, &db, &tx_reader).await;
                }
                Ok(Message::Binary(data)) => {
                    // Try to decode as UTF-8
                    if let Ok(text) = String::from_utf8(data) {
                        handle_msg(&text, &futures_code, &db, &tx_reader).await;
                    }
                }
                Ok(Message::Close(frame)) => {
                    println!("[Kinvest] Connection closed: {:?}", frame);
                    break;
                }
                Err(e) => {
                    eprintln!("[Kinvest] WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        println!("[Kinvest] Reader task ended");
    });

    Ok(())
}

async fn get_approval_key(base_url: &str, app_key: &str, secret_key: &str) -> Result<String, Box<dyn Error>> {
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

async fn handle_msg(text: &str, code: &str, db: &Arc<Db>, tx: &mpsc::Sender<Message>) {
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
                        eprintln!("[Kinvest] Failed to send PINGPONG response: {}", e);
                    }
                    return;
                }
                
                // Log subscriptions
                if let Some(body) = json_msg["body"].as_object() {
                    if body.contains_key("msg1") && body["msg1"].as_str() == Some("SUBSCRIBE SUCCESS") {
                        println!("[Kinvest] ✅ Subscription confirmed (TR_ID: {})", tr_id);
                    }
                }
            }
            return;
        }
    }

    // Delimited
    let parts: Vec<&str> = if text.contains('|') {
        text.split('|').collect()
    } else {
        text.split('^').collect()
    };

    if parts.len() < 4 {
        return;
    }

    let tr_id = parts[1]; // Index 1 is TR_ID

    if tr_id == TR_COMMODITY_FUTURES_ORDERBOOK || tr_id == TR_NIGHTTIME_FUTURES_ORDERBOOK {
        let data_part = parts[3];
        let data_fields: Vec<&str> = data_part.split('^').collect();

        // 5 levels * 6 blocks = 30 fields.
        // + code + time = 32 fields minimum.
        if data_fields.len() >= 32 {
            let mut asks = Vec::new();
            let mut bids = Vec::new();

            // Kinvest 5-level structure (User specified):
            // 0: Code
            // 1: Time
            // Block 1 (2..6): Ask Price x 5
            // Block 2 (7..11): Bid Price x 5
            // Block 3 (12..16): Ask Total Count x 5 (Ignored)
            // Block 4 (17..21): Bid Total Count x 5 (Ignored)
            // Block 5 (22..26): Ask Remain (Size) x 5
            // Block 6 (27..31): Bid Remain (Size) x 5
            
            for i in 0..5 {
                // Asks: Price from Block 1, Size from Block 5
                let ap_idx = 2 + i;
                let as_idx = 22 + i;
                
                if as_idx < data_fields.len() {
                    if let (Ok(p), Ok(s)) = (data_fields[ap_idx].parse::<f64>(), data_fields[as_idx].parse::<f64>()) {
                         asks.push(OrderbookItem { price: p, size: s });
                    }
                }

                // Bids: Price from Block 2, Size from Block 6
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
                symbol: code.to_string(),
                asks,
                bids,
            };

            if let Err(e) = db.save_snapshot(&standardized).await {
                eprintln!("[Kinvest] Failed to save snapshot: {}", e);
            } else {
            }
        }
    }
}
