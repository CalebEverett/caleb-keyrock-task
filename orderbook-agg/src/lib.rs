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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
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
