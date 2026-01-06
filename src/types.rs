use serde::{Deserialize, Serialize};



#[derive(Debug, Deserialize, Serialize)]
pub struct UpbitOrderbookUnit {
    #[serde(rename = "ap")]
    pub ask_price: f64,
    #[serde(rename = "bp")]
    pub bid_price: f64,
    #[serde(rename = "as")]
    pub ask_size: f64,
    #[serde(rename = "bs")]
    pub bid_size: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UpbitOrderbook {
    #[serde(rename = "ty")]
    pub type_: String,
    #[serde(rename = "cd")]
    pub code: String,
    #[serde(rename = "tms")]
    pub timestamp: i64,
    #[serde(rename = "obu")]
    pub orderbook_units: Vec<UpbitOrderbookUnit>,
    #[serde(rename = "tas")]
    pub total_ask_size: f64,
    #[serde(rename = "tbs")]
    pub total_bid_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub name: String,
    pub timestamp: i64,
    pub symbol: String,
    pub asks: Vec<OrderbookItem>,
    pub bids: Vec<OrderbookItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookItem {
    pub price: f64,
    pub size: f64,
}
