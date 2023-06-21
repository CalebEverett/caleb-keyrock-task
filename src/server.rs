use dotenv::dotenv;
use futures::{lock::Mutex, SinkExt, Stream};
use serde_json::json;
use std::{pin::Pin, sync::Arc};
use tokio_stream::StreamMap;
use tokio_tungstenite::tungstenite::Message;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Result};
use tonic::{transport::Server, Status};
use url::Url;

use ckt_common::{
    get_symbols_all,
    orderbook::{
        orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
        Empty, ExchangeType, Summary, SummaryRequest, Symbols,
    },
    validate_symbol, Orderbook,
};

const BASE_WS_BINANCE: &str = "wss://stream.binance.us:9443";
const BASE_WS_BISTAMP: &str = "wss://ws.bitstamp.net";

#[derive(Debug)]
pub struct OrderbookSummary {
    orderbook: Arc<Mutex<Orderbook>>,
}

impl Default for OrderbookSummary {
    fn default() -> Self {
        Self {
            orderbook: Arc::new(Mutex::new(Orderbook::default())),
        }
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
        let SummaryRequest {
            symbol,
            limit,
            min_price,
            max_price,
            power_price,
        } = request.into_inner();

        validate_symbol(&symbol).await?;
        let mut ob = self.orderbook.lock().await;
        ob.reset(symbol.clone(), limit, min_price, max_price, power_price);
        ob.add_snapshots().await.expect("Could not add snapshots");

        let summary = ob.get_summary();
        let response = tonic::Response::new(summary);
        Ok(response)
    }

    async fn watch_summary(
        &self,
        request: tonic::Request<SummaryRequest>,
    ) -> Result<tonic::Response<Self::WatchSummaryStream>, Status> {
        let SummaryRequest {
            symbol,
            limit,
            min_price,
            max_price,
            power_price,
        } = request.into_inner();
        tracing::info!("Got a request for symbol {}", symbol,);
        let (tx, rx) = mpsc::unbounded_channel();

        validate_symbol(&symbol).await?;
        let mut ob = self.orderbook.lock().await;
        ob.reset(symbol.clone(), limit, min_price, max_price, power_price);
        ob.add_snapshots().await.expect("Could not add snapshots");

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

        let summary = ob.get_summary();
        while let Some((key, msg)) = map.next().await {
            let msg = msg.expect("Failed to get message");
            // let msg_value: serde_json::Value = serde_json::from_slice(&msg.into_data()).unwrap();
            // let msg: Vec<&String> = msg_value.as_object().unwrap().keys().collect();
            tracing::info!(
                "Got message from {}: {}",
                key.as_str_name(),
                msg.into_text().unwrap()
            );

            if let Err(err) = tx.send(Ok(summary.clone())) {
                tracing::error!("Error sending summary: {:?}", err);
                return Err(Status::internal("Error sending summary"));
            } else {
                tracing::info!("Summary: {:?}", summary);
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
