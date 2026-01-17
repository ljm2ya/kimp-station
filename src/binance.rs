use futures_util::{SinkExt, StreamExt};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use std::error::Error;
use std::sync::Arc;
use tracing::{info, error, debug};

use crate::types::{BinanceTrade, BinanceDepth, BinanceStreamWrapper, OrderbookSnapshot, OrderbookItem, TradeSnapshot};
use crate::storage::Db;

const WS_URL: &str = "wss://stream.binance.com:9443/stream";

pub async fn subscribe(
    symbols: Vec<String>,
    db: Arc<Db>,
) {
    // Spawn the websocket task
    tokio::spawn(async move {
        loop {
            info!(target: "binance", symbols = ?symbols, "Connecting to websocket...");
            match connect_and_handle(&symbols, &db).await {
                Ok(_) => {
                    info!(target: "binance", "Websocket disconnected normally");
                }
                Err(e) => {
                    error!(target: "binance", error = %e, "Connection error. Reconnecting in 5s...");
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn connect_and_handle(
    symbols: &[String],
    db: &Arc<Db>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Build stream names for combined stream
    // Format: symbol@trade/symbol@depth20@100ms
    let streams: Vec<String> = symbols
        .iter()
        .flat_map(|s| {
            let lower = s.to_lowercase();
            vec![
                format!("{}@trade", lower),
                format!("{}@depth20@100ms", lower),
            ]
        })
        .collect();

    let streams_param = streams.join("/");
    let url = format!("{}?streams={}", WS_URL, streams_param);

    let (ws_stream, _) = connect_async(&url).await?;
    let (mut write, mut read) = ws_stream.split();

    info!(target: "binance", "Websocket connected");
    info!(target: "binance", streams = ?streams, "Subscribed to streams");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                handle_msg(&text, db).await;
            }
            Ok(Message::Binary(data)) => {
                if let Ok(text) = String::from_utf8(data) {
                    handle_msg(&text, db).await;
                }
            }
            Ok(Message::Ping(data)) => {
                write.send(Message::Pong(data)).await?;
            }
            Ok(Message::Close(_)) => {
                info!(target: "binance", "Received close frame");
                break;
            }
            Err(e) => {
                error!(target: "binance", error = %e, "Websocket error");
                return Err(Box::new(e));
            }
            _ => {}
        }
    }

    Ok(())
}

async fn handle_msg(text: &str, db: &Arc<Db>) {
    // Binance combined stream format: { "stream": "btcusdt@trade", "data": {...} }
    if let Ok(wrapper) = serde_json::from_str::<BinanceStreamWrapper>(text) {
        let stream = &wrapper.stream;

        if stream.ends_with("@trade") {
            // Trade stream
            if let Ok(trade) = serde_json::from_value::<BinanceTrade>(wrapper.data) {
                handle_trade(trade, db).await;
            }
        } else if stream.contains("@depth") {
            // Depth stream - extract symbol from stream name
            let symbol = stream.split('@').next().unwrap_or("").to_uppercase();
            if let Ok(depth) = serde_json::from_value::<BinanceDepth>(wrapper.data) {
                handle_depth(depth, &symbol, db).await;
            }
        }
    } else {
        // Log unknown messages for debugging
        if !text.is_empty() {
            debug!(target: "binance", message = %text, "Unknown message format");
        }
    }
}

async fn handle_trade(trade: BinanceTrade, db: &Arc<Db>) {
    // Parse price and quantity from strings
    let price: f64 = trade.price.parse().unwrap_or(0.0);
    let volume: f64 = trade.quantity.parse().unwrap_or(0.0);

    if price <= 0.0 || volume <= 0.0 {
        debug!(target: "binance", symbol = %trade.symbol, "Skipping trade with invalid price/volume");
        return;
    }

    // is_buyer_maker: true = seller initiated (SELL/ASK), false = buyer initiated (BUY/BID)
    let ask_bid = if trade.is_buyer_maker { "ASK" } else { "BID" };

    let snapshot = TradeSnapshot {
        source: "binance".to_string(),
        code: trade.symbol.clone(),
        trade_price: price,
        trade_volume: volume,
        ask_bid: ask_bid.to_string(),
        timestamp: trade.event_time,
        trade_timestamp: trade.trade_time,
    };

    if let Err(e) = db.save_trade(&snapshot).await {
        error!(
            target: "binance",
            error = %e,
            symbol = %trade.symbol,
            "Failed to save trade"
        );
    } else {
        debug!(
            target: "binance",
            symbol = %trade.symbol,
            price = %price,
            volume = %volume,
            side = %ask_bid,
            "Saved trade"
        );
    }
}

async fn handle_depth(depth: BinanceDepth, symbol: &str, db: &Arc<Db>) {
    // Parse bids and asks from string arrays
    let mut asks: Vec<OrderbookItem> = Vec::new();
    let mut bids: Vec<OrderbookItem> = Vec::new();

    for ask in &depth.asks {
        let price: f64 = ask[0].parse().unwrap_or(0.0);
        let size: f64 = ask[1].parse().unwrap_or(0.0);
        if price > 0.0 {
            asks.push(OrderbookItem { price, size });
        }
    }

    for bid in &depth.bids {
        let price: f64 = bid[0].parse().unwrap_or(0.0);
        let size: f64 = bid[1].parse().unwrap_or(0.0);
        if price > 0.0 {
            bids.push(OrderbookItem { price, size });
        }
    }

    // Truncate to 10 levels if necessary
    if asks.len() > 10 { asks.truncate(10); }
    if bids.len() > 10 { bids.truncate(10); }

    let now = chrono::Utc::now().timestamp_millis();

    let snapshot = OrderbookSnapshot {
        name: "binance".to_string(),
        timestamp: now,
        symbol: symbol.to_string(),
        asks,
        bids,
    };

    // Filter out invalid orderbooks
    if !snapshot.has_valid_prices() {
        debug!(
            target: "binance",
            symbol = %symbol,
            "Skipping snapshot with zero prices"
        );
        return;
    }

    if let Err(e) = db.save_snapshot(&snapshot).await {
        error!(
            target: "binance",
            error = %e,
            symbol = %symbol,
            "Failed to save orderbook snapshot"
        );
    } else {
        debug!(
            target: "binance",
            symbol = %symbol,
            "Saved orderbook snapshot"
        );
    }
}
