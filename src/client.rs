use clap::Parser;
use tokio_stream::StreamExt;

use ckt_lib::booksummary::{
    orderbook_aggregator_client::OrderbookAggregatorClient, Empty, SummaryRequest,
};

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
    #[clap(long)]
    symbol: String,
    #[clap(long)]
    levels: Option<u32>,
    #[clap(long)]
    min_price: f64,
    #[clap(long)]
    max_price: f64,
    #[clap(long)]
    decimals: u32,
}

/// Gets a list of symbols present on all exchanges.
async fn get_symbols() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;

    let request = tonic::Request::new(Empty {});
    let symbols = client.get_symbols(request).await?.into_inner();
    for symbol in symbols.symbols {
        println!("{}", symbol);
    }
    Ok(())
}

/// Gets a summary for a given symbol from the most recently available snapshots from the exchanges.
async fn get_summary(opts: SummaryOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;

    let request = tonic::Request::new(SummaryRequest {
        symbol: opts.symbol,
        levels: opts.levels.unwrap_or(10),
        min_price: opts.min_price,
        max_price: opts.max_price,
        decimals: opts.decimals,
    });

    let summary = client.get_summary(request).await?.into_inner();
    println!("summary: {:?}", summary);
    Ok(())
}

/// Streams a summary of the aggregatge orderbook for a given symbol, updated for changes from all exchanges.
async fn watch_summary(opts: SummaryOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;
    let request = tonic::Request::new(SummaryRequest {
        symbol: opts.symbol,
        levels: opts.levels.unwrap_or(10),
        min_price: opts.min_price,
        max_price: opts.max_price,
        decimals: opts.decimals,
    });

    let mut stream = client.watch_summary(request).await?.into_inner();
    while let Some(summary) = stream.next().await {
        match summary {
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
