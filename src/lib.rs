use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashSet;
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

pub async fn get_symbols_binance() -> Result<HashSet<String>, Box<dyn std::error::Error>> {
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

pub async fn get_symbols_bitstamp() -> Result<HashSet<String>, Box<dyn std::error::Error>> {
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
    pub last_update_id: i64,
    #[serde(deserialize_with = "from_string")]
    pub bids: Vec<[f64; 2]>,
    #[serde(deserialize_with = "from_string")]
    pub asks: Vec<[f64; 2]>,
}

fn from_string<'de, D>(deserializer: D) -> Result<Vec<[f64; 2]>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<[&str; 2]> = Deserialize::deserialize(deserializer)?;
    // do better hex decoding than this
    Ok(v.iter()
        .map(|s| {
            [
                s[0].parse::<f64>().expect("Failed to parse"),
                s[1].parse::<f64>().expect("Failed to parse"),
            ]
        })
        .collect::<Vec<[f64; 2]>>())
}
