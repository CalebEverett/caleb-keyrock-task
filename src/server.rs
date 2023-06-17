use tonic::transport::Server;

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
    #[tracing::instrument]
    async fn book_summary(
        &self,
        request: tonic::Request<Pair>,
    ) -> Result<tonic::Response<Summary>, tonic::Status> {
        tracing::info!("Got a request from {:?}", request.remote_addr());
        let pair = request.into_inner();
        let response = tonic::Response::new(self.summary.clone());
        Ok(response)
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
