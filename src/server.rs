use std::pin::Pin;

use caleb_keyrock_task_lib::Snapshot;
use dotenv::dotenv;
use futures::Stream;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Result};
use tonic::{transport::Server, Status};
use url::Url;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}
use orderbook::{
    orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
    Empty, ExchangeType, Level, Summary, Symbol, Symbols,
};

const BASE_URL_BINANCE: &str = "wss://stream.binance.us:9443";

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

pub async fn get_symbols_all() -> Result<Symbols, Box<dyn std::error::Error>> {
    let symbols_binance = caleb_keyrock_task_lib::get_symbols_binance().await?;
    let symbols_bitstamp = caleb_keyrock_task_lib::get_symbols_bitstamp().await?;
    let mut symbols: Vec<_> = symbols_binance
        .intersection(&symbols_bitstamp)
        .map(|symbol| Symbol {
            symbol: symbol.clone(),
        })
        .collect();

    symbols.sort_by(|a, b| a.symbol.cmp(&b.symbol));

    println!(
        "binance: {}, bitstamp: {}, all: {}",
        &symbols_binance.len(),
        &symbols_bitstamp.len(),
        &symbols.len()
    );
    Ok(Symbols { symbols })
}

pub async fn validate_symbol(request: tonic::Request<Symbol>) -> Result<Symbol, Status> {
    let addr = request.remote_addr().unwrap();
    let symbol = request.into_inner();
    tracing::info!("Got a request for symbol {} from {:?}", symbol.symbol, addr);

    let symbols = get_symbols_all().await.expect("Failed to get symbols");
    if !symbols.symbols.iter().any(|s| {
        println!("s: {:?}, symbol: {:?}", s, symbol);
        s == &symbol
    }) {
        tracing::error!(
            "Symbol {} not found on one or more exchanges",
            symbol.symbol
        );
        return Err(Status::not_found("Symbol not found"));
    }
    Ok(symbol)
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
        tracing::info!(
            "Found {} matching symbols on both exchanges",
            symbols.symbols.len()
        );
        let response = tonic::Response::new(symbols);
        Ok(response)
    }

    async fn get_summary(
        &self,
        request: tonic::Request<Symbol>,
    ) -> Result<tonic::Response<Summary>, Status> {
        let symbol = validate_symbol(request).await?;
        let snapshot = reqwest::get(format!(
            "https://www.binance.us/api/v3/depth?symbol={}&limit=10",
            symbol.symbol
        ))
        .await
        .expect("Failed to get snapshot")
        .json::<Snapshot>()
        .await
        .expect("Failed to parse json");

        tracing::info!("Got snapshot: {:?}", snapshot);
        let summary = Summary {
            spread: snapshot.asks[0][0] - snapshot.bids[0][0],
            bids: snapshot
                .bids
                .into_iter()
                .map(|b| Level {
                    exchange: ExchangeType::Binance as i32,
                    price: b[0],
                    amount: b[1],
                })
                .collect(),

            asks: snapshot
                .asks
                .into_iter()
                .map(|b| Level {
                    exchange: ExchangeType::Binance as i32,
                    price: b[0],
                    amount: b[1],
                })
                .collect(),
        };
        let response = tonic::Response::new(summary);
        Ok(response)
    }

    async fn watch_summary(
        &self,
        request: tonic::Request<Symbol>,
    ) -> Result<tonic::Response<Self::WatchSummaryStream>, Status> {
        let symbol = request.into_inner();
        tracing::info!("Got a request for symbol {}", symbol.symbol,);
        let (tx, rx) = mpsc::unbounded_channel();

        let snapshot = reqwest::get("https://www.binance.us/api/v3/depth?symbol=BTCUSDT&limit=10")
            .await
            .expect("Failed to get snapshot")
            .json::<Snapshot>()
            .await
            .expect("Failed to parse json");

        tracing::info!("Got snapshot: {:?}", snapshot);

        let ws_url = Url::parse(BASE_URL_BINANCE)
            .expect("bad binance url")
            .join("ws/btcusdt@depth")
            .unwrap();

        let (mut ws_stream, _) = connect_async(&ws_url)
            .await
            .expect(format!("Failed to connect to {}", &ws_url.as_str()).as_str());

        while let Some(msg) = ws_stream.next().await {
            let msg = msg.expect("Failed to get message");
            let msg = msg.into_text().expect("Failed to convert to text");
            tracing::info!("Got message: {}", msg);
            // let msg =
            //     serde_json::from_str::<serde_json::Value>(&msg).expect("Failed to parse json");
            // let bids = msg["bids"]
            //     .as_array()
            //     .expect("Failed to get bids")
            //     .iter()
            //     .map(|bid| Level {
            //         exchange: "binance".to_string(),
            //         price: bid[0].as_str().unwrap().parse::<f64>().unwrap(),
            //         amount: bid[1].as_str().unwrap().parse::<f64>().unwrap(),
            //     })
            //     .collect::<Vec<Level>>();
            // let asks = msg["asks"]
            //     .as_array()
            //     .expect("Failed to get asks")
            //     .iter()
            //     .map(|ask| Level {
            //         exchange: "binance".to_string(),
            //         price: ask[0].as_str().unwrap().parse::<f64>().unwrap(),
            //         amount: ask[1].as_str().unwrap().parse::<f64>().unwrap(),
            //     })
            //     .collect::<Vec<Level>>();
            // let spread = bids[0].price - asks[0].price;
            // let summary = Summary { spread, bids, asks };
            // if let Err(err) = tx.send(Ok(summary.clone())) {
            //     tracing::error!("Error sending summary: {:?}", err);
            //     return Err(Status::internal("Error sending summary"));
            // }
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
