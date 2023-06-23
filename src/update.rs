use crate::booksummary::ExchangeType;
use anyhow::{Context, Result};
use futures::SinkExt;
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use serde_json::{Map, Value};
use strum::IntoEnumIterator;
use tokio::net::TcpStream;
use tokio_stream::StreamMap;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

/// Structure to hold update data.
#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Update {
    pub exchange: Option<ExchangeType>,
    #[serde(
        alias = "microtimestamp",
        alias = "E",
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
    Ok(v.into_iter()
        .map(|s| (s[0].parse::<f64>(), s[1].parse::<f64>()))
        .filter_map(|p| Some([p.0.ok()?, p.1.ok()?]))
        .collect::<Vec<[f64; 2]>>())
}

/// Converts a pair of strings to a pair of numbers of type T.
fn str_pair_to_num<T>(pair: &Value) -> Result<[T; 2], anyhow::Error>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let price = pair[0]
        .as_str()
        .context("Failed to get price str")?
        .parse::<T>()
        .context("Failed to parse price str to num")?;
    let qty = pair[1]
        .as_str()
        .context("Failed to get qty str")?
        .parse::<T>()
        .context("Failed to parse price str to num")?;
    Ok([price, qty])
}

/// Converts an array of pairs of strings to a vec of pairs of nums of type T.
fn str_vec_to_num_vec<T>(str_vec: &Value) -> Result<Vec<[T; 2]>, anyhow::Error>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let num_vec = str_vec
        .as_array()
        .context("Failed to get array")?
        .iter()
        .filter_map(|p| str_pair_to_num::<T>(p).ok())
        .collect::<Vec<[T; 2]>>();

    Ok(num_vec)
}

/// Deserializes updates from binance.
pub fn get_updates_binance(value: &Value) -> Result<Update, anyhow::Error> {
    let last_update_id = value["E"]
        .as_u64()
        .context("Failed to get binance last_update_id")?;

    let bids = str_vec_to_num_vec::<f64>(&value["b"])?;
    let asks = str_vec_to_num_vec::<f64>(&value["a"])?;

    Ok(Update {
        exchange: Some(ExchangeType::Binance),
        last_update_id,
        bids,
        asks,
    })
}

/// Deserializes updates from bitstamp.
pub fn get_updates_bitstamp(
    value: &Map<String, serde_json::Value>,
) -> Result<Update, Box<dyn std::error::Error + Send + Sync>> {
    let last_update_id = value["microtimestamp"]
        .as_str()
        .context("Failed to get bitstamp last_update_id")?
        .parse::<u64>()
        .context("Failed to parse bitstamp last_update_id str")?;

    let bids = str_vec_to_num_vec::<f64>(&value["bids"])?;
    let asks = str_vec_to_num_vec::<f64>(&value["asks"])?;

    Ok(Update {
        exchange: Some(ExchangeType::Binance),
        last_update_id,
        bids,
        asks,
    })
}

/// Gets a websocket stream for each exchange and returns a map of them.
pub async fn get_stream(
    symbol: String,
) -> Result<StreamMap<ExchangeType, WebSocketStream<MaybeTlsStream<TcpStream>>>, anyhow::Error> {
    let mut map = StreamMap::new();
    let symbol = symbol.to_lowercase();

    for exchange in ExchangeType::iter() {
        match exchange {
            ExchangeType::Binance => {
                let ws_url_binance = url::Url::parse("wss://stream.binance.us:9443")
                    .context("bad binance url")?
                    .join(&format!("ws/{}@depth@100ms", symbol))?;

                let (ws_stream_binance, _) = connect_async(&ws_url_binance)
                    .await
                    .context("Failed to connect to binance wss endpoint")?;

                map.insert(ExchangeType::Binance, ws_stream_binance);
            }
            ExchangeType::Bitstamp => {
                let ws_url_bitstamp =
                    url::Url::parse("wss://ws.bitstamp.net").context("bad bitstamp url")?;
                let subscribe_msg = serde_json::json!({
                    "event": "bts:subscribe",
                    "data": {
                        "channel": format!("diff_order_book_{}", symbol)
                    }
                });

                let (mut ws_stream_bitstamp, _) = connect_async(&ws_url_bitstamp)
                    .await
                    .context("Failed to connect to bit stamp wss endpoint")?;

                ws_stream_bitstamp
                    .start_send_unpin(Message::Text(subscribe_msg.to_string()))
                    .context("Failed to send subscribe message to bitstamp")?;

                map.insert(ExchangeType::Bitstamp, ws_stream_bitstamp);
            }
        }
    }
    Ok(map)
}

/// Gets a orderbook snapshot with up to 1000 levels for a given exchange and symbol.
pub async fn get_snapshot(
    exchange: ExchangeType,
    symbol: &String,
) -> Result<Update, anyhow::Error> {
    let url = match exchange {
        ExchangeType::Binance => {
            tracing::info!("Getting snapshot from binance");
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit=1000",
                symbol
            )
        }
        ExchangeType::Bitstamp => {
            tracing::info!("Getting snapshot from bitstamp");
            format!(
                "https://www.bitstamp.net/api/v2/order_book/{}/",
                symbol.to_lowercase()
            )
        }
    };

    let mut snapshot = reqwest::get(url)
        .await
        .context("Failed to get snapshot")?
        // .text()
        .json::<Update>()
        .await
        .context("Failed to deserialize snapshot to json")?;

    snapshot.exchange = Some(exchange);

    Ok(snapshot)
}
