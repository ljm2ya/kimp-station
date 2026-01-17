use futures_util::{StreamExt, SinkExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn, error, debug};

use crate::types::{UpbitOrderbook, OrderbookSnapshot, OrderbookItem, UpbitTrade, TradeSnapshot};
use crate::storage::Db;

const WS_URL: &str = "wss://api.upbit.com/websocket/v1";

pub async fn subscribe(
    symbols: Vec<String>,
    db: Arc<Db>,
) -> Result<(), Box<dyn Error>> {
    let symbols = Arc::new(symbols);

    tokio::spawn(async move {
        loop {
            info!(
                target: "upbit",
                symbols = ?*symbols,
                "Connecting to websocket..."
            );

            match connect_and_handle(&symbols, &db).await {
                Ok(()) => {
                    info!(target: "upbit", "Connection closed. Reconnecting in 5s...");
                }
                Err(e) => {
                    error!(target: "upbit", error = %e, "Connection error. Reconnecting in 5s...");
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    Ok(())
}

async fn connect_and_handle(
    symbols: &[String],
    db: &Arc<Db>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = Url::parse(WS_URL)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    info!(target: "upbit", "Websocket connected");

    // Subscribe message - orderbook and trade types
    let subscribe_msg = json!([
        { "ticket": "kimp-station-rs" },
        { "type": "orderbook", "codes": symbols },
        { "type": "trade", "codes": symbols, "isOnlyRealtime": true },
        { "format": "SIMPLE" }
    ]);

    write.send(Message::Text(subscribe_msg.to_string())).await?;
    info!(target: "upbit", symbols = ?symbols, "Sent subscription request (orderbook + trade)");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                handle_msg(&text, db).await;
            }
            Ok(Message::Binary(bin)) => {
                if let Ok(text) = String::from_utf8(bin) {
                    handle_msg(&text, db).await;
                }
            }
            Ok(Message::Ping(data)) => {
                if let Err(e) = write.send(Message::Pong(data)).await {
                    error!(target: "upbit", error = %e, "Failed to send pong");
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                info!(target: "upbit", "Received close frame");
                break;
            }
            Err(e) => {
                error!(target: "upbit", error = %e, "Websocket error");
                return Err(Box::new(e));
            }
            _ => {}
        }
    }

    Ok(())
}

async fn handle_msg(text: &str, db: &Arc<Db>) {
    // Try to parse as orderbook first
    if let Ok(ob) = serde_json::from_str::<UpbitOrderbook>(text) {
        if ob.type_ == "orderbook" {
            handle_orderbook(ob, db).await;
            return;
        }
    }

    // Try to parse as trade
    if let Ok(trade) = serde_json::from_str::<UpbitTrade>(text) {
        if trade.type_ == "trade" {
            handle_trade(trade, db).await;
            return;
        }
    }

    // Log unknown messages for debugging (ignore empty)
    if !text.is_empty() && !text.contains("\"ty\":\"orderbook\"") && !text.contains("\"ty\":\"trade\"") {
        warn!(target: "upbit", message = %text, "Received unknown message type");
    }
}

async fn handle_orderbook(ob: UpbitOrderbook, db: &Arc<Db>) {
    // Convert to OrderbookSnapshot
    let mut asks = Vec::new();
    let mut bids = Vec::new();

    for unit in ob.orderbook_units {
        asks.push(OrderbookItem {
            price: unit.ask_price,
            size: unit.ask_size,
        });
        bids.push(OrderbookItem {
            price: unit.bid_price,
            size: unit.bid_size,
        });
    }

    // Truncate to 10 if necessary
    if asks.len() > 10 { asks.truncate(10); }
    if bids.len() > 10 { bids.truncate(10); }

    let standardized = OrderbookSnapshot {
        name: "upbit".to_string(),
        timestamp: ob.timestamp,
        symbol: ob.code.clone(),
        asks,
        bids,
    };

    // Filter out invalid orderbooks (zero prices)
    if !standardized.has_valid_prices() {
        debug!(
            target: "upbit",
            symbol = %ob.code,
            "Skipping snapshot with zero prices"
        );
        return;
    }

    if let Err(e) = db.save_snapshot(&standardized).await {
        error!(
            target: "upbit",
            error = %e,
            symbol = %ob.code,
            "Failed to save orderbook snapshot"
        );
    } else {
        debug!(
            target: "upbit",
            symbol = %ob.code,
            "Saved orderbook snapshot"
        );
    }
}

async fn handle_trade(trade: UpbitTrade, db: &Arc<Db>) {
    let snapshot = TradeSnapshot {
        source: "upbit".to_string(),
        code: trade.code.clone(),
        trade_price: trade.trade_price,
        trade_volume: trade.trade_volume,
        ask_bid: trade.ask_bid.clone(),
        timestamp: trade.timestamp,
        trade_timestamp: trade.trade_timestamp,
    };

    if let Err(e) = db.save_trade(&snapshot).await {
        error!(
            target: "upbit",
            error = %e,
            code = %trade.code,
            "Failed to save trade"
        );
    } else {
        debug!(
            target: "upbit",
            code = %trade.code,
            price = %trade.trade_price,
            volume = %trade.trade_volume,
            side = %trade.ask_bid,
            "Saved trade"
        );
    }
}
