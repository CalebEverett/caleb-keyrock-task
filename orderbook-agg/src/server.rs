use dotenv::dotenv;
use futures::Stream;
use std::pin::Pin;
use tokio::{select, sync::mpsc};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_tungstenite::tungstenite::Result;
use tonic::{transport::Server, Status};

use orderbook_agg::{
    booksummary::{
        orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
        Empty, ExchangeType, Summary, SummaryRequest, Symbols,
    },
    orderbook::Orderbook,
    symbol::{get_symbols_all, validate_symbol},
    update::get_stream,
    update::{get_updates_binance, get_updates_bitstamp},
};

#[derive(Debug)]
pub struct OrderbookSummary {}

impl Default for OrderbookSummary {
    fn default() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl OrderbookAggregator for OrderbookSummary {
    type WatchSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

    /// Returns a list of symbols present on all exchanges.
    async fn get_symbols(
        &self,
        request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Symbols>, Status> {
        let addr = request.remote_addr().unwrap();
        tracing::info!("Got a request for symbols from {:?}", addr);
        let symbols: Symbols = get_symbols_all()
            .await
            .map_err(|_| Status::internal("Failed to get symbols"))?;
        let response = tonic::Response::new(symbols);
        Ok(response)
    }

    /// Gets an orderbook summary for a given symbol from the most recently available
    /// snapshots from teh exchanges.
    async fn get_summary(
        &self,
        request: tonic::Request<SummaryRequest>,
    ) -> Result<tonic::Response<Summary>, Status> {
        let SummaryRequest {
            symbol,
            levels,
            price_range,
            decimals,
        } = request.into_inner();

        validate_symbol(&symbol).await?;
        let snapshots = Orderbook::get_snapshots(&symbol).await.map_err(|err| {
            tracing::error!("Failed to get snapshots: {:?}", err);
            Status::internal("Failed to get snapshots")
        })?;

        let ob = Orderbook::new(symbol.clone(), levels, price_range, decimals, snapshots).map_err(
            |err| {
                tracing::error!("Failed to create orderbook: {:?}", err);
                Status::internal("Failed to create orderbook")
            },
        )?;

        let summary = ob.get_summary();
        let response = tonic::Response::new(summary);
        Ok(response)
    }

    /// Streaming summary for a given symbol, updated for changes from all exchanges.
    async fn watch_summary(
        &self,
        request: tonic::Request<SummaryRequest>,
    ) -> Result<tonic::Response<Self::WatchSummaryStream>, Status> {
        let SummaryRequest {
            symbol,
            levels,
            price_range,
            decimals,
        } = request.into_inner();

        validate_symbol(&symbol).await?;
        let snapshots = Orderbook::get_snapshots(&symbol).await.map_err(|err| {
            tracing::error!("Failed to get snapshots: {:?}", err);
            Status::internal("Failed to get snapshots")
        })?;

        let mut ob = Orderbook::new(symbol.clone(), levels, price_range, decimals, snapshots)
            .map_err(|err| {
                tracing::error!("Failed to create orderbook: {:?}", err);
                Status::internal("Failed to create orderbook")
            })?;

        let (tx, rx) = mpsc::unbounded_channel();
        let mut map = get_stream(symbol)
            .await
            .map_err(|_| Status::internal("Could not get stream"))?;
        tracing::info!("Opened stream");
        // let ob_clone = self.orderbook.clone();
        tokio::spawn(async move {
            // let mut ob = ob_clone.lock().await;
            let mut summary = ob.get_summary();
            loop {
                select! {
                    Some((key, msg)) = map.next() => {
                        let msg = msg.map_err(|_| Status::internal("Failed to get message"))?;

                        if let Ok(msg_value) = serde_json::from_slice(&msg.into_data()) {
                            match key {
                                ExchangeType::Binance => {
                                    if let Ok(updates) = get_updates_binance(&msg_value) {
                                        ob.update(updates).map_err(|_| Status::internal("Failed to process binance update"))?;
                                    };
                                }
                                ExchangeType::Bitstamp => {
                                    if let Some(data) = msg_value["data"].as_object() {
                                        if data.len() > 0 {
                                            let updates =
                                                get_updates_bitstamp(data).map_err(|_| Status::internal("failed to get updates"))?;
                                                ob.update(updates).map_err(|_| Status::internal("Failed to process bitstamp update"))?;
                                        }
                                    };
                                }
                            }
                        };
                        let mut new_summary = ob.get_summary();
                        if new_summary != summary {
                            summary = new_summary.clone();
                            new_summary.timestamp = chrono::Utc::now().timestamp_millis() as u64;
                            if let Err(err) = tx.send(Ok(new_summary)) {
                                tracing::error!("Error sending summary: {:?}", err);
                                return Err(Status::internal("Error sending summary"));
                            }
                        }
                    },
                    () = tx.closed() => {
                        tracing::info!("Client closed stream");
                        for (_, exchange_stream) in map.iter_mut() {
                            exchange_stream.close(None).await.map_err(|_| Status::internal("Failed to close stream"))?;
                        }
                        return Ok(());
                    },
                }
            }
        });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(tonic::Response::new(
            Box::pin(stream) as Self::WatchSummaryStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
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
