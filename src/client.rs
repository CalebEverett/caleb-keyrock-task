use clap::Parser;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}
use orderbook::{orderbook_aggregator_client::OrderbookAggregatorClient, Pair};

#[derive(Debug, Parser)]
struct Options {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Get(GetOptions),
}

#[derive(Debug, Parser)]
struct GetOptions {
    #[clap(long)]
    symbol_1: String,
    #[clap(long)]
    symbol_2: String,
}

async fn get(opts: GetOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;

    let request = tonic::Request::new(Pair {
        symbol_1: opts.symbol_1,
        symbol_2: opts.symbol_2,
    });
    let summary = client.book_summary(request).await?.into_inner();
    tracing::info!("summary: {:?}", summary);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::parse();

    use Command::*;
    match opts.command {
        Get(opts) => get(opts).await?,
    };

    Ok(())
}
