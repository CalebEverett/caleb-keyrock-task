use clap::Parser;
use tokio_stream::StreamExt;
pub mod orderbook {
    tonic::include_proto!("orderbook");
}
use orderbook::{orderbook_aggregator_client::OrderbookAggregatorClient, Empty};

use crate::orderbook::SummaryRequest;

#[derive(Debug, Parser)]
struct Options {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    GetSummary(SummaryOptions),
    WatchSummary(SummaryOptions),
    GetSymbols,
}

#[derive(Debug, Parser)]
struct SummaryOptions {
    symbol: String,
    limit: Option<i32>,
}

async fn get_summary(opts: SummaryOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;

    let limit = opts.limit.unwrap_or(10);
    let request = tonic::Request::new(SummaryRequest {
        symbol: opts.symbol,
        limit: opts.limit.unwrap_or(10),
    });

    let summary = client.get_summary(request).await?.into_inner();
    println!("summary: {:?}", summary);
    Ok(())
}

async fn watch_summary(opts: SummaryOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;
    let request = tonic::Request::new(SummaryRequest {
        symbol: opts.symbol,
        limit: opts.limit.unwrap_or(10),
    });
    let mut stream = client.watch_summary(request).await?.into_inner();

    while let Some(summary) = stream.next().await {
        match summary {
            Ok(summary) => println!("summary was updated: {:?}", summary),
            Err(err) => {
                if err.code() == tonic::Code::NotFound {
                    println!("watched item has been removed from the inventory.");
                    break;
                } else {
                    return Err(err.into());
                }
            }
        };
    }
    println!("stream closed");

    Ok(())
}

async fn get_symbols() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;

    let request = tonic::Request::new(Empty {});
    let symbols = client.get_symbols(request).await?.into_inner();
    for symbol in symbols.symbols {
        println!("{}", symbol);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::parse();

    use Command::*;
    match opts.command {
        GetSummary(opts) => get_summary(opts).await?,
        WatchSummary(opts) => watch_summary(opts).await?,
        GetSymbols => {
            get_symbols().await?;
            return Ok(());
        }
    };

    Ok(())
}
