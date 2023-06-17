use clap::Parser;
use tokio_stream::StreamExt;

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
    Get(SummaryOptions),
    Watch(SummaryOptions),
}

#[derive(Debug, Parser)]
struct SummaryOptions {
    symbol_1: String,
    symbol_2: String,
}

async fn get(opts: SummaryOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;

    let request = tonic::Request::new(Pair {
        symbol_1: opts.symbol_1,
        symbol_2: opts.symbol_2,
    });
    let summary = client.get(request).await?.into_inner();
    tracing::info!("summary: {:?}", summary);
    Ok(())
}

async fn watch(opts: SummaryOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;
    let mut stream = client
        .watch(Pair {
            symbol_1: opts.symbol_1,
            symbol_2: opts.symbol_2,
        })
        .await?
        .into_inner();

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::parse();

    use Command::*;
    match opts.command {
        Get(opts) => get(opts).await?,
        Watch(opts) => watch(opts).await?,
    };

    Ok(())
}
