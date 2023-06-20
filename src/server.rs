use dotenv::dotenv;
use futures::future::try_join_all;
use futures::{SinkExt, Stream};
use serde_json::json;
use std::{collections::HashSet, pin::Pin};
use strum::IntoEnumIterator;
use tokio_stream::StreamMap;
use tokio_tungstenite::tungstenite::Message;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Result};
use tonic::{transport::Server, Status};
use url::Url;

use ckt_common::{
    get_symbols_binance, get_symbols_bitstamp,
    orderbook::{
        orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
        Empty, ExchangeType, Level, Summary, SummaryRequest, Symbols,
    },
    Orderbook, Snapshot,
};

const BASE_WS_BINANCE: &str = "wss://stream.binance.us:9443";
const BASE_WS_BISTAMP: &str = "wss://ws.bitstamp.net";

#[derive(Debug)]
pub struct OrderbookSummary {
    summary: Summary,
    symbols: Symbols,
}

impl Default for OrderbookSummary {
    fn default() -> Self {
        Self {
            summary: Summary {
                spread: 0.0,
                bids: vec![Level {
                    exchange: ExchangeType::Binance as i32,
                    price: 0.0,
                    amount: 0.0,
                }],
                asks: vec![Level {
                    exchange: ExchangeType::Bitstamp as i32,
                    price: 0.0,
                    amount: 0.0,
                }],
            },
            symbols: Symbols::default(),
        }
    }
}

pub async fn get_symbols(
    exchange: ExchangeType,
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    match exchange {
        ExchangeType::Binance => get_symbols_binance().await,
        ExchangeType::Bitstamp => get_symbols_bitstamp().await,
    }
}

pub async fn get_symbols_all() -> Result<Symbols, Box<dyn std::error::Error + Send + Sync>> {
    let symbols_vec = try_join_all(
        ExchangeType::iter()
            .map(|exchange| get_symbols(exchange))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("couldn't get symbols");

    let exchange_names: Vec<&str> = ExchangeType::iter().map(|e| e.as_str_name()).collect();
    let symbols_intersection: HashSet<String> =
        symbols_vec
            .into_iter()
            .enumerate()
            .fold(HashSet::new(), |acc, (i, set)| {
                tracing::info!(exchange = &exchange_names[i], symbols_count = set.len());
                if acc.is_empty() {
                    set
                } else {
                    acc.intersection(&set).map(|s| s.to_string()).collect()
                }
            });

    let mut symbols: Vec<String> = symbols_intersection.into_iter().collect();

    symbols.sort();

    tracing::info!(
        exchanges = format!("{}", exchange_names.join(", ")),
        symbols_count = symbols.len()
    );

    Ok(Symbols { symbols })
}

pub async fn validate_summary_request(
    request: tonic::Request<SummaryRequest>,
) -> Result<(String, i32), Status> {
    let addr = request.remote_addr().unwrap();
    let summary_request = request.into_inner();
    let symbol = summary_request.symbol;
    let limit = summary_request.limit;
    tracing::info!(
        "Received request for symbol {} with limit of {} from {:?}",
        symbol,
        limit,
        addr
    );

    let symbols = get_symbols_all().await.expect("Failed to get symbols");
    if !symbols.symbols.iter().any(|s| s == &symbol) {
        tracing::error!("Symbol {} not found on one or more exchanges", symbol);
        return Err(Status::not_found("Symbol not found"));
    }
    Ok((symbol, limit))
}

pub async fn get_snapshot(
    exchange: ExchangeType,
    symbol: &String,
) -> Result<(ExchangeType, Snapshot), Box<dyn std::error::Error + Send + Sync>> {
    let url = match exchange {
        ExchangeType::Binance => {
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit=100",
                symbol
            )
        }
        ExchangeType::Bitstamp => format!(
            "https://www.bitstamp.net/api/v2/order_book/{}/",
            symbol.to_lowercase()
        ),
    };

    tracing::info!(
        "Getting snapshot for {} from {}",
        exchange.as_str_name(),
        url
    );

    let mut snapshot = reqwest::get(url)
        .await
        .expect(format!("Failed to get snapshot for {}", exchange.as_str_name()).as_str())
        .json::<Snapshot>()
        .await
        .expect(format!("Failed to parse json for {}", exchange.as_str_name()).as_str());

    if exchange == ExchangeType::Bitstamp {
        snapshot.bids = snapshot.bids[0..100 as usize].to_vec();
        snapshot.asks = snapshot.asks[0..100 as usize].to_vec();
    }

    Ok((exchange, snapshot))
}

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

pub fn snapshot_to_summary(exchange: ExchangeType, snapshot: Snapshot) -> Summary {
    Summary {
        spread: snapshot.asks[0][0] - snapshot.bids[0][0],
        bids: snapshot
            .bids
            .into_iter()
            .map(|b| Level {
                exchange: exchange as i32,
                price: b[0],
                amount: b[1],
            })
            .collect(),

        asks: snapshot
            .asks
            .into_iter()
            .map(|b| Level {
                exchange: exchange as i32,
                price: b[0],
                amount: b[1],
            })
            .collect(),
    }
}

#[async_trait::async_trait]
impl OrderbookAggregator for OrderbookSummary {
    type WatchSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

    async fn get_symbols(
        &self,
        request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Symbols>, Status> {
        let addr = request.remote_addr().unwrap();
        tracing::info!("Got a request for symbols from {:?}", addr);
        let symbols: Symbols = get_symbols_all().await.expect("Failed to get symbols");
        let response = tonic::Response::new(symbols);
        Ok(response)
    }

    async fn get_summary(
        &self,
        request: tonic::Request<SummaryRequest>,
    ) -> Result<tonic::Response<Summary>, Status> {
        let (symbol, limit) = validate_summary_request(request).await?;
        let mut ob = Orderbook::new(symbol.clone(), 25700., 27700., 2);
        let snapshots: Vec<(ExchangeType, Snapshot)> = get_snapshots(&symbol)
            .await
            .expect("couldn't get snapshots");

        // let (e, s) = snapshots.pop().unwrap();
        snapshots.into_iter().for_each(|(e, s)| {
            ob.add_snapshot(e, s);
        });
        // ob.add_snapshot(e, s);

        tracing::info!("bid_max {}, ask_min: {}", ob.bid_max, ob.ask_min,);

        let summary = ob.get_summary(limit);
        let response = tonic::Response::new(summary);
        Ok(response)
    }

    async fn watch_summary(
        &self,
        request: tonic::Request<SummaryRequest>,
    ) -> Result<tonic::Response<Self::WatchSummaryStream>, Status> {
        let symbol = request.into_inner();
        tracing::info!("Got a request for symbol {}", symbol.symbol,);
        let (tx, rx) = mpsc::unbounded_channel();

        let mut map = StreamMap::new();
        let ws_url_binance = Url::parse(BASE_WS_BINANCE)
            .expect("bad binance url")
            .join("ws/btcusdt@depth")
            .unwrap();

        let (ws_stream_binance, _) = connect_async(&ws_url_binance)
            .await
            .expect(format!("Failed to connect to {}", &ws_url_binance.as_str()).as_str());
        map.insert(ExchangeType::Binance, ws_stream_binance);

        let ws_url_bitstamp = Url::parse(BASE_WS_BISTAMP).expect("bad bitstamp url");
        let subscribe_msg = json!({
            "event": "bts:subscribe",
            "data": {
                "channel": "diff_order_book_btcusdt"
            }
        });

        let (mut ws_stream_bitstamp, _) = connect_async(&ws_url_bitstamp)
            .await
            .expect(format!("Failed to connect to {}", &ws_url_bitstamp.as_str()).as_str());

        ws_stream_bitstamp
            .start_send_unpin(Message::Text(subscribe_msg.to_string()))
            .expect("Failed to send subscribe message to bitstamp");

        map.insert(ExchangeType::Bitstamp, ws_stream_bitstamp);

        let summary = Summary::default();
        while let Some((key, msg)) = map.next().await {
            let msg = msg.expect("Failed to get message");
            let msg = msg.into_text().expect("Failed to convert to text");
            tracing::info!("Got message from {}: {}", key.as_str_name(), msg);

            if let Err(err) = tx.send(Ok(summary.clone())) {
                tracing::error!("Error sending summary: {:?}", err);
                return Err(Status::internal("Error sending summary"));
            }
        }

        let stream = UnboundedReceiverStream::new(rx);
        Ok(tonic::Response::new(
            Box::pin(stream) as Self::WatchSummaryStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    dotenv().ok();
    let addr = "127.0.0.1:9001";
    tracing::info!("Server listening on {}", addr);

    let socket_addr = addr.parse()?;
    let orderbook = OrderbookSummary::default();
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook))
        .serve(socket_addr)
        .await?;
    Ok(())
}
