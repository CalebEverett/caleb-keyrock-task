use anyhow::Result;
use futures::Stream;
use orderbook_agg::{
    booksummary::{
        orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
        Empty, Summary,
    },
    core::{exchangebook::ExchangeOrderbook, orderbook::BookLevels},
    exchanges::{binance::BinanceOrderbook, bitstamp::BitstampOrderbook},
    make_summary, Exchange, Symbol,
};
use std::{collections::HashMap, pin::Pin};
use tokio::{select, sync::mpsc, sync::oneshot, sync::watch};
use tokio_stream::wrappers::WatchStream;
use tonic::{transport::Server, Status};

async fn start_symbol(
    symbol: Symbol,
    price_range: u8,
    levels: u32,
) -> mpsc::Sender<oneshot::Sender<watch::Receiver<Result<Summary, Status>>>> {
    // send summary back to tonic server to be consumed by client.
    let (tx, rx) = watch::channel(Ok(Summary::default()));
    drop(rx);

    // Receive one shot sender that sends a summary receiver back to the server.
    let (tx2, mut rx2) =
        mpsc::channel::<oneshot::Sender<watch::Receiver<Result<Summary, Status>>>>(100);

    // Create orderbooks for each of the exchanges
    let ob_bs = BitstampOrderbook::new(symbol, price_range).await.unwrap();
    let ob_bn = BinanceOrderbook::new(symbol, price_range).await.unwrap();

    // Tx3 and tx4 go into each of the order books to send back the book levels.
    // The receiver stays here to be used in the select loop below to create summaries from the book levels.
    let (tx3, mut rx3) = mpsc::channel::<BookLevels>(100);
    let tx4 = tx3.clone();

    // Start the order books.
    tokio::spawn(async move {
        ob_bs.start(levels, tx3).await.unwrap();
    });
    tokio::spawn(async move {
        ob_bn.start(levels, tx4).await.unwrap();
    });

    tokio::spawn(async move {
        let mut levels_map = HashMap::<Exchange, BookLevels>::new();
        let mut summary_count = 0;
        loop {
            select! {
                // Receive a one shot sender that sends a summary receiver back to the server.
                val = rx2.recv() => {
                    if let Some(oneshot_sender) = val {
                        oneshot_sender.send(tx.subscribe()).unwrap();
                        tracing::info!("summary_count: {}, rx count: {}", summary_count, tx.receiver_count());
                    }
                },
                // Receive book levels from the order books.
                val = rx3.recv() => {
                    if let Some(book_levels) = val {
                        match book_levels {
                            BookLevels { exchange: Exchange::BITSTAMP, .. }  => {
                                levels_map.insert(Exchange::BITSTAMP, book_levels);
                            }
                            BookLevels { exchange: Exchange::BINANCE, .. } => {
                                levels_map.insert(Exchange::BINANCE, book_levels);
                            }
                        }
                        if levels_map.len() < 2 {
                            continue;
                        }

                        // Book levels are stored in the hashmap above and a new summary created from both exchanges
                        // every time an update is received from either.
                        let current_levels = levels_map.values().map(|v| v.clone()).collect::<Vec<BookLevels>>();
                        let summary = make_summary(current_levels, symbol);

                        tx.send_replace(Ok(summary)).unwrap();
                        summary_count += 1;
                    }
                },
            }
        }
    });
    tx2
}

#[derive(Debug)]
pub struct OrderbookSummary {
    tx_summary: mpsc::Sender<oneshot::Sender<watch::Receiver<Result<Summary, Status>>>>,
}

#[async_trait::async_trait]
impl OrderbookAggregator for OrderbookSummary {
    type WatchSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;
    async fn watch_summary(
        &self,
        request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::WatchSummaryStream>, Status> {
        let addr = request.remote_addr().unwrap();
        tracing::info!("Got a request from {:?}", addr);
        let (tx1, rx1) = oneshot::channel::<watch::Receiver<Result<Summary, Status>>>();
        self.tx_summary.send(tx1).await.unwrap();
        let rx_summary = rx1.await.unwrap();
        let stream = WatchStream::new(rx_summary);
        Ok(tonic::Response::new(
            Box::pin(stream) as Self::WatchSummaryStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let addr = "127.0.0.1:9001";
    tracing::info!("Server listening on {}", addr);

    let tx_summary = start_symbol(Symbol::BTCUSDT, 5, 15).await;

    let socket_addr = addr.parse()?;
    let orderbook = OrderbookSummary { tx_summary };
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook))
        .serve(socket_addr)
        .await?;
    Ok(())
}
