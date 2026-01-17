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

impl OrderbookSnapshot {
    /// Check if the orderbook has valid prices (non-zero best bid/ask)
    /// Returns false for sparse orderbooks during market transitions
    pub fn has_valid_prices(&self) -> bool {
        let valid_bid = self.bids.first().map_or(false, |b| b.price > 0.0);
        let valid_ask = self.asks.first().map_or(false, |a| a.price > 0.0);
        valid_bid && valid_ask
    }

    /// Check if orderbook data is identical (ignores timestamp)
    pub fn is_same_data(&self, other: &Self) -> bool {
        self.symbol == other.symbol && self.asks == other.asks && self.bids == other.bids
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderbookItem {
    pub price: f64,
    pub size: f64,
}

// ============================================
// Trade Types
// ============================================

/// Raw Upbit trade format (SIMPLE mode)
#[derive(Debug, Deserialize, Serialize)]
pub struct UpbitTrade {
    #[serde(rename = "ty")]
    pub type_: String,
    #[serde(rename = "cd")]
    pub code: String,
    #[serde(rename = "tp")]
    pub trade_price: f64,
    #[serde(rename = "tv")]
    pub trade_volume: f64,
    #[serde(rename = "ab")]
    pub ask_bid: String,
    #[serde(rename = "tms")]
    pub timestamp: i64,
    #[serde(rename = "ttms")]
    pub trade_timestamp: i64,
}

/// Standardized trade snapshot for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSnapshot {
    pub source: String,
    pub code: String,
    pub trade_price: f64,
    pub trade_volume: f64,
    pub ask_bid: String,
    pub timestamp: i64,
    pub trade_timestamp: i64,
}
