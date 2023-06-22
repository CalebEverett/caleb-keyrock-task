use crate::booksummary::ExchangeType;
use futures::future::try_join_all;
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use strum::IntoEnumIterator;

/// Structure to hold a snapshot of an orderbook.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    #[serde(
        alias = "microtimestamp",
        deserialize_with = "deserialize_number_from_string"
    )]
    pub last_update_id: u64,
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

/// Gets a orderbook snapshot with up to 1000 levels for a given exchange and symbol.
pub async fn get_snapshot(
    exchange: ExchangeType,
    symbol: &String,
) -> Result<(ExchangeType, Snapshot), Box<dyn std::error::Error + Send + Sync>> {
    let url = match exchange {
        ExchangeType::Binance => {
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit=1000",
                symbol
            )
        }
        ExchangeType::Bitstamp => format!(
            "https://www.bitstamp.net/api/v2/order_book/{}/",
            symbol.to_lowercase()
        ),
    };

    let snapshot = reqwest::get(url)
        .await
        .expect(format!("Failed to get snapshot for {}", exchange.as_str_name()).as_str())
        .json::<Snapshot>()
        .await
        .expect(format!("Failed to parse json for {}", exchange.as_str_name()).as_str());

    Ok((exchange, snapshot))
}

/// Gets snapshots for all exchanges for a given symbol.
pub async fn get_snapshots(
    symbol: &String,
) -> Result<Vec<(ExchangeType, Snapshot)>, Box<dyn std::error::Error + Send + Sync>> {
    let snapshots_vec: Vec<(ExchangeType, Snapshot)> = try_join_all(
        ExchangeType::iter()
            .map(|exchange| get_snapshot(exchange, symbol))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("couldn't get snapshots");
    Ok(snapshots_vec)
}
