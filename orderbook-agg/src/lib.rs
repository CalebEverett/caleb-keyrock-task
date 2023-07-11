//! gRPC server that streams an aggregate summary of Level 2 order book data from two exchanges.
//!
//! ## Server
//! Connects to two exchanges, Bitstamp and Binance, and aggregates the order book data from
//! both into a single stream. The server can be started with:
//! ```
//! cargo run --bin server
//! ```
//!
//! ## Client
//! There is a client that can be used to connect to the server and receive the summary stream.
//! ```
//! cargo run --bin client
//! ```
//!
//! ## Order Book
//! Order books for each exchange are maintained in separate processes with a summary streamed
//! back to the server. The server then aggregates the summaries and streams the aggregate
//! to each connected client.
//!
//! Order books are maintained in vecs with indices corresponding to price levels. Price levels
//! for the base and quote assets are stored with [`u64`] and displayed with [Decimal](rust_decimal::Decimal)
//! to preserve the precisions specified by the exchanges.
//!
//! ## Exchanges
//! Additional exchanges can be added by creating a wrapper struct around an [OrderBook](crate::core::order_book::OrderBook) instance
//! and implementing the [ExchangeBook](crate::core::exchange_book::ExchangeBook) trait. See [BitstampOrderBook](crate::exchanges::bitstamp::BitstampOrderBook)
//! and [BinanceOrderBook](crate::exchanges::binance::BinanceOrderBook) for example implementations.
use crate::core::order_book::BookLevels;

use book_summary::{Level, Summary};
use serde::{Deserialize, Serialize};

/// Module built from protobuf definitions.
pub mod book_summary {
    tonic::include_proto!("booksummary");
}
pub mod core;
pub mod exchanges;

/// The symbol the order book data is for
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum Symbol {
    #[default]
    BTCUSDT,
    BTCUSD,
    ETHBTC,
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Symbol::BTCUSDT => write!(f, "BTCUSDT"),
            Symbol::BTCUSD => write!(f, "BTCUSD"),
            Symbol::ETHBTC => write!(f, "ETHBTC"),
        }
    }
}

/// The exchange the order book data is for
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Hash, Eq)]
pub enum Exchange {
    #[default]
    BINANCE,
    BITSTAMP,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Exchange::BINANCE => write!(f, "BINANCE"),
            Exchange::BITSTAMP => write!(f, "BITSTAMP"),
        }
    }
}

/// Returns a single summary of order book data aggregated from multiple exchanges
///
/// # Arguments
///
/// * `book_levels_vec` - A vector of [BookLevels] structs from multiple exchanges
/// * `symbol` - The symbol the order book data is for
// TODO: Remove symbol argument and get from BookLevels
pub fn make_summary(mut book_levels_vec: Vec<BookLevels>, symbol: Symbol) -> Summary {
    let levels_count = book_levels_vec[0].bids.len();
    let exchange_count = book_levels_vec.len();

    let mut bids = Vec::<Level>::with_capacity(levels_count);
    let mut asks = Vec::<Level>::with_capacity(levels_count);
    for i in 0..exchange_count {
        bids.append(&mut book_levels_vec[i].bids);
        asks.append(&mut book_levels_vec[i].asks);
    }
    bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
    asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

    let take_bids = bids.into_iter().take(levels_count).collect::<Vec<Level>>();
    let take_asks = asks.into_iter().take(levels_count).collect::<Vec<Level>>();
    let summary = Summary {
        symbol: symbol.to_string(),
        spread: take_asks[0].price - take_bids[0].price,
        timestamp: chrono::Utc::now().timestamp() as u64,
        bids: take_bids,
        asks: take_asks,
    };
    summary
}
