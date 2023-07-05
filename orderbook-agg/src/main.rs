use std::{collections::HashMap, ops::Deref};

use anyhow::Result;
use orderbook_agg::{
    core::exchange_book::ExchangeOrderbook,
    core::orderbook::BookLevels,
    exchanges::{binance::BinanceOrderbook, bitstamp::BitstampOrderbook},
    Exchange, Symbol,
};
use tokio_stream::{wrappers::WatchStream, StreamExt, StreamMap};

fn print_book_levels(book_levels: &BookLevels) {
    println!(
        "exchange: {}, symbol, {}, best_bid: {:?}, best_ask: {:?}, spread: {:?}, bid_qty: {}, ask_qty: {}, last_update_id: {}, bid_levels: {:?}, ask_levels: {:?}",
        book_levels.exchange,
        book_levels.symbol,
        book_levels.bids[0][0],
        book_levels.asks[0][0],
        (book_levels.asks[0][0] - book_levels.bids[0][0]),
        book_levels.bids.len(),
        book_levels.asks.len(),
        book_levels.last_update_id,
        book_levels.bids,
        book_levels.asks,
    );
}

async fn start_binance(symbol: Symbol, price_range: u8, levels: u32) -> Result<()> {
    let orderbook = BinanceOrderbook::new(Exchange::BINANCE, symbol, price_range).await?;
    let mut rx_summary = orderbook.rx_summary();

    let rx_handle = tokio::spawn(async move {
        while rx_summary.changed().await.is_ok() {
            if let Some(book_levels) = rx_summary.borrow().deref() {
                print_book_levels(&book_levels);
            };
        }
    });
    let ob_handle = tokio::spawn(async move {
        orderbook.start(levels).await.unwrap();
    });
    rx_handle.await?;
    ob_handle.await?;
    Ok(())
}
async fn start_bitstamp(symbol: Symbol, price_range: u8, levels: u32) -> Result<()> {
    let orderbook = BitstampOrderbook::new(Exchange::BITSTAMP, symbol, price_range).await?;
    let mut rx_summary = orderbook.rx_summary();

    let rx_handle = tokio::spawn(async move {
        while rx_summary.changed().await.is_ok() {
            if let Some(book_levels) = rx_summary.borrow().deref() {
                print_book_levels(&book_levels);
            };
        }
    });
    let ob_handle = tokio::spawn(async move {
        orderbook.start(levels).await.unwrap();
    });
    rx_handle.await?;
    ob_handle.await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let symbols = vec![Symbol::BTCUSDT, Symbol::BTCUSDT, Symbol::ETHBTC];
    let price_range = 3;
    let levels = 2;

    let mut map = StreamMap::new();

    let orderbook =
        BitstampOrderbook::new(Exchange::BITSTAMP, Symbol::BTCUSDT, price_range).await?;
    let rx_summary = orderbook.rx_summary();
    map.insert(Exchange::BITSTAMP, WatchStream::new(rx_summary));

    let orderbook_handle = tokio::spawn(async move {
        orderbook.start(levels).await.unwrap();
    });

    let orderbook1 = BinanceOrderbook::new(Exchange::BINANCE, Symbol::BTCUSDT, price_range).await?;
    let rx_summary1 = orderbook1.rx_summary();
    map.insert(Exchange::BINANCE, WatchStream::new(rx_summary1));

    let orderbook1_handle = tokio::spawn(async move {
        orderbook1.start(levels).await.unwrap();
    });

    let rx_handle = tokio::spawn(async move {
        let mut summarymap = HashMap::<Exchange, BookLevels>::new();
        while let Some((exch, Some(book_levels))) = map.next().await {
            if book_levels.bids.is_empty() || book_levels.asks.is_empty() {
                continue;
            }

            summarymap.insert(exch, book_levels);

            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for (_, bl) in summarymap.iter() {
                bids.append(&mut bl.bids.clone());
                asks.append(&mut bl.asks.clone());
            }
            bids.sort_by(|a, b| b[0].partial_cmp(&a[0]).unwrap());
            asks.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
            let lup = summarymap.get(&exch).unwrap().last_update_id;
            let nb = BookLevels {
                exchange: exch,
                symbol: Symbol::BTCUSDT,
                bids,
                asks,
                last_update_id: lup,
            };
            print_book_levels(&nb);

            // println!("exchange: {:?}, book_levels: {:?}", exchange, book_levels);
            // summarymap.insert(exchange, book_levels);
        }
    });

    orderbook_handle.await?;
    orderbook1_handle.await?;
    let _ = rx_handle.await?;
    tracing::info!("done");
    Ok(())
}
