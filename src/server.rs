use std::pin::Pin;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server, Status};

pub mod orderbook {
    tonic::include_proto!("orderbook");
}
use orderbook::{
    orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
    Level, Pair, Summary,
};

#[derive(Debug)]
pub struct OrderbookSummary {
    summary: Summary,
}

impl Default for OrderbookSummary {
    fn default() -> Self {
        Self {
            summary: Summary {
                spread: 0.0,
                bids: vec![Level {
                    exchange: "binance".to_string(),
                    price: 0.0,
                    amount: 0.0,
                }],
                asks: vec![Level {
                    exchange: "bitstamp".to_string(),
                    price: 0.0,
                    amount: 0.0,
                }],
            },
        }
    }
}

#[async_trait::async_trait]
impl OrderbookAggregator for OrderbookSummary {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

    async fn get(&self, request: tonic::Request<Pair>) -> Result<tonic::Response<Summary>, Status> {
        let addr = request.remote_addr();
        let pair = request.into_inner();
        tracing::info!(
            "Got a request for symbols {} and {} from {:?}",
            pair.symbol_1,
            pair.symbol_2,
            addr
        );
        let response = tonic::Response::new(self.summary.clone());
        Ok(response)
    }

    async fn watch(
        &self,
        request: tonic::Request<Pair>,
    ) -> Result<tonic::Response<Self::WatchStream>, Status> {
        let pair = request.into_inner();
        tracing::info!(
            "Got a request for symbols {} and {}",
            pair.symbol_1,
            pair.symbol_2
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let summary = self.summary.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if let Err(err) = tx.send(Ok(summary.clone())) {
                    tracing::error!("Error sending summary: {:?}", err);
                    return;
                }
            }
        });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(stream) as Self::WatchStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

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
