use orderbook::{ExchangeType, Level, Summary, Symbol};
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use std::collections::HashSet;
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

pub async fn get_symbols_binance(
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let symbols = reqwest::get("https://api.binance.us/api/v3/exchangeInfo")
        .await?
        .json::<serde_json::Value>()
        .await?["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|symbol| symbol["symbol"].as_str().unwrap().to_string())
        .collect::<HashSet<String>>();
    Ok(symbols)
}

pub async fn get_symbols_bitstamp(
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let symbols = reqwest::get("https://www.bitstamp.net/api/v2/ticker/")
        .await?
        .json::<serde_json::Value>()
        .await?
        .as_array()
        .unwrap()
        .iter()
        .map(|symbol| symbol["pair"].as_str().unwrap().replace("/", ""))
        .collect::<HashSet<String>>();
    Ok(symbols)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    #[serde(
        alias = "microtimestamp",
        deserialize_with = "deserialize_number_from_string"
    )]
    pub last_update_id: i64,
    #[serde(deserialize_with = "from_str")]
    pub bids: Vec<[f64; 2]>,
    #[serde(deserialize_with = "from_str")]
    pub asks: Vec<[f64; 2]>,
}

fn from_str<'de, D>(deserializer: D) -> Result<Vec<[f64; 2]>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<[&str; 2]> = Deserialize::deserialize(deserializer)?;
    Ok(v.iter()
        .map(|s| {
            [
                s[0].parse::<f64>().expect("Failed to parse"),
                s[1].parse::<f64>().expect("Failed to parse"),
            ]
        })
        .collect::<Vec<[f64; 2]>>())
}

pub async fn get_snapshot(
    exchange: ExchangeType,
    symbol: Symbol,
    limit: usize,
) -> Result<Snapshot, Box<dyn std::error::Error + Send + Sync>> {
    let url = match exchange {
        orderbook::ExchangeType::Binance => {
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit={}",
                symbol.symbol, limit
            )
        }
        orderbook::ExchangeType::Bitstamp => format!(
            "https://www.bitstamp.net/api/v2/order_book/{}/",
            symbol.symbol
        ),
    };

    let mut snapshot = reqwest::get(url)
        .await
        .expect("Failed to get snapshot")
        .json::<Snapshot>()
        .await
        .expect("Failed to parse json");

    if exchange == ExchangeType::Bitstamp {
        snapshot.bids = snapshot.bids[0..limit].to_vec();
        snapshot.asks = snapshot.asks[0..limit].to_vec();
    }

    Ok(snapshot)
}
