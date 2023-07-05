use anyhow::Result;
use clap::Parser;
use tokio_stream::StreamExt;

use orderbook_agg::booksummary::{orderbook_aggregator_client::OrderbookAggregatorClient, Empty};

#[derive(Debug, Parser)]
struct Options {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    GetLevels,
    WatchSummary,
}

#[derive(Debug, Parser)]
struct SummaryOptions {
    #[clap(long)]
    symbol: String,
}

/// Streams a summary of the aggregatge orderbook for a given symbol, updated for changes from all exchanges.
async fn watch_levels(
    mut client: OrderbookAggregatorClient<tonic::transport::Channel>,
) -> Result<()> {
    let request = tonic::Request::new(Empty {});

    let mut stream = client.get_levels(request).await?.into_inner();
    while let Some(level) = stream.next().await {
        match level {
            Ok(level) => println!("\n{:#?}", level),
            Err(err) => {
                return Err(err.into());
            }
        };
    }
    println!("stream closed");

    Ok(())
}

async fn watch_summary(
    mut client: OrderbookAggregatorClient<tonic::transport::Channel>,
) -> Result<()> {
    let request = tonic::Request::new(Empty {});

    let mut stream = client.watch_summary(request).await?.into_inner();
    while let Some(result) = stream.next().await {
        match result {
            Ok(summary) => println!("\n{:#?}", summary),
            Err(err) => {
                return Err(err.into());
            }
        };
    }
    println!("stream closed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;
    let opts = Options::parse();

    use Command::*;
    match opts.command {
        GetLevels => watch_levels(client).await?,
        WatchSummary => watch_summary(client).await?,
    };

    Ok(())
}
