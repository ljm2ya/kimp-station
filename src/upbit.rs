use futures_util::{StreamExt, SinkExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use std::error::Error;
use std::sync::Arc;

use crate::types::{UpbitOrderbook, OrderbookSnapshot, OrderbookItem};
use crate::storage::Db;

pub async fn subscribe(
    symbols: Vec<String>,
    db: Arc<Db>,
) -> Result<(), Box<dyn Error>> {
    let url = Url::parse("wss://api.upbit.com/websocket/v1")?;
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Subscribe message
    let subscribe_msg = json!([
        { "ticket": "kimp-station-rs" },
        { "type": "orderbook", "codes": symbols },
        { "format": "SIMPLE" }
    ]);

    write.send(Message::Text(subscribe_msg.to_string())).await?;
    println!("Subscribed to Upbit: {:?}", symbols);

    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    handle_msg(&text, &db).await;
                }
                Ok(Message::Binary(bin)) => {
                    if let Ok(text) = String::from_utf8(bin) {
                        handle_msg(&text, &db).await;
                    }
                }
                Err(e) => {
                    eprintln!("Upbit WS error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    Ok(())
}

async fn handle_msg(text: &str, db: &Arc<Db>) {
    if let Ok(ob) = serde_json::from_str::<UpbitOrderbook>(text) {
        if ob.type_ == "orderbook" {
            // Convert to OrderbookSnapshot
            let mut asks = Vec::new();
            let mut bids = Vec::new();
            
            // Collect all units (truncate later if needed, but user wants unified structure)
            // Upbit gives levels with both ask/bid in same unit
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
            
            // Sort Asks ascending (lowest price first - best ask)
            // Sort Bids descending (highest price first - best bid)
            // Upbit units usually come sorted by proximity to spread, so unit[0] is best.
            // Asks: unit[0].ap < unit[1].ap ...
            // Bids: unit[0].bp > unit[1].bp ...
            // So they are already sorted by level. 

            // Truncate to 10 if necessary
            if asks.len() > 10 { asks.truncate(10); }
            if bids.len() > 10 { bids.truncate(10); }

            let standardized = OrderbookSnapshot {
                name: "upbit".to_string(),
                // Upbit timestamp is ms
                timestamp: ob.timestamp,
                symbol: ob.code.clone(),
                asks,
                bids,
            };

            if let Err(e) = db.save_snapshot(&standardized).await {
                eprintln!("Failed to save Upbit snapshot: {}", e);
            }
        }
    }
}
