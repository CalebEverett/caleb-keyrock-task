use crate::booksummary::ExchangeType;
use futures::SinkExt;
use serde_json::{Map, Value};
use strum::IntoEnumIterator;
use tokio::net::TcpStream;
use tokio_stream::StreamMap;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

/// Structure to hold update data.
#[derive(Debug)]
pub struct Updates {
    pub exchange: ExchangeType,
    pub last_update_id: u64,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}

/// Deserializes updates from binance.
pub fn updates_binance(value: Value) -> Result<Updates, Box<dyn std::error::Error + Send + Sync>> {
    let last_update_id = value["E"].as_u64().ok_or("Failed to get last_update_id")?;

    let bids = value["b"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            [
                b[0].as_str().unwrap().parse::<f64>().unwrap(),
                b[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    let asks = value["a"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            [
                a[0].as_str().unwrap().parse::<f64>().unwrap(),
                a[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    Ok(Updates {
        exchange: ExchangeType::Binance,
        last_update_id,
        bids,
        asks,
    })
}

/// Deserializes updates from bitstamp.
pub fn updates_bitstamp(
    value: &Map<String, serde_json::Value>,
) -> Result<Updates, Box<dyn std::error::Error + Send + Sync>> {
    let last_update_id = value["microtimestamp"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let bids = value["bids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            [
                b[0].as_str().unwrap().parse::<f64>().unwrap(),
                b[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    let asks = value["asks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            [
                a[0].as_str().unwrap().parse::<f64>().unwrap(),
                a[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    Ok(Updates {
        exchange: ExchangeType::Bitstamp,
        last_update_id,
        bids,
        asks,
    })
}

/// Gets a websocket stream for each exchange and returns a map of them.
pub async fn get_stream(
    symbol: String,
) -> Result<
    StreamMap<ExchangeType, WebSocketStream<MaybeTlsStream<TcpStream>>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut map = StreamMap::new();
    let symbol = symbol.to_lowercase();

    for exchange in ExchangeType::iter() {
        match exchange {
            ExchangeType::Binance => {
                let ws_url_binance = url::Url::parse("wss://stream.binance.us:9443")
                    .expect("bad binance url")
                    .join(&format!("ws/{}@depth@100ms", symbol))
                    .unwrap();

                let (ws_stream_binance, _) = connect_async(&ws_url_binance)
                    .await
                    .expect(format!("Failed to connect to {}", &ws_url_binance.as_str()).as_str());

                map.insert(ExchangeType::Binance, ws_stream_binance);
            }
            ExchangeType::Bitstamp => {
                let ws_url_bitstamp =
                    url::Url::parse("wss://ws.bitstamp.net").expect("bad bitstamp url");
                let subscribe_msg = serde_json::json!({
                    "event": "bts:subscribe",
                    "data": {
                        "channel": format!("diff_order_book_{}", symbol)
                    }
                });

                let (mut ws_stream_bitstamp, _) = connect_async(&ws_url_bitstamp)
                    .await
                    .expect(format!("Failed to connect to {}", &ws_url_bitstamp.as_str()).as_str());

                ws_stream_bitstamp
                    .start_send_unpin(Message::Text(subscribe_msg.to_string()))
                    .expect("Failed to send subscribe message to bitstamp");

                map.insert(ExchangeType::Bitstamp, ws_stream_bitstamp);
            }
        }
    }
    Ok(map)
}
