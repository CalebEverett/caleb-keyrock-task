use crate::core::orderbook::BookLevels;

use booksummary::{Level, Summary};
use serde::{Deserialize, Serialize};

pub mod booksummary {
    tonic::include_proto!("booksummary");
}
pub mod core;
pub mod exchanges;

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
