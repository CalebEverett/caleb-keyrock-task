use std::ops::Deref;

use anyhow::Result;
use futures::future::try_join_all;
use orderbook_agg::{
    core::exchange_book::ExchangeOrderbook,
    core::orderbook::{BookLevels, OrderbookMessage},
    exchanges::{binance::BinanceOrderbook, bitstamp::BitstampOrderbook},
    Exchange, Symbol,
};

fn print_book_levels(book_levels: &BookLevels) {
    println!(
        "exchange: {}, symbol, {}, best_bid: {:?}, best_ask: {:?}, spread: {:?}, bid_qty: {}, ask_qty: {}",
        book_levels.exchange,
        book_levels.symbol,
        book_levels.bids[0].0,
        book_levels.asks[0].0,
        (book_levels.asks[0].0 - book_levels.bids[0].0),
        book_levels.bids.len(),
        book_levels.asks.len(),
    );
}

async fn start_binance(symbol: Symbol, price_range: u8, levels: u32) -> Result<()> {
    let orderbook = BinanceOrderbook::new(Exchange::BINANCE, symbol, price_range).await?;
    let mut rx_summary = orderbook.rx_summary();

    let rx_handle = tokio::spawn(async move {
        while rx_summary.changed().await.is_ok() {
            if let OrderbookMessage::BookLevels(book_levels) = rx_summary.borrow().deref() {
                if book_levels.bids.is_empty() || book_levels.asks.is_empty() {
                    continue;
                }
                print_book_levels(book_levels);
            }
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
            if let OrderbookMessage::BookLevels(book_levels) = rx_summary.borrow().deref() {
                if book_levels.bids.is_empty() || book_levels.asks.is_empty() {
                    continue;
                }
                print_book_levels(book_levels);
            }
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

    let handles_binance = try_join_all(
        symbols
            .iter()
            .map(|symbol| start_binance(*symbol, price_range, levels))
            .collect::<Vec<_>>(),
    );

    let handles_bitstamp = try_join_all(
        symbols
            .iter()
            .map(|symbol| start_bitstamp(*symbol, price_range, levels))
            .collect::<Vec<_>>(),
    );

    tokio::select! {
        Err(e) = handles_binance => {
            tracing::error!("binance error: {}", e);
        }
        Err(e) = handles_bitstamp => {
            tracing::error!("bitstamp error: {}", e);
        }
    }

    tracing::info!("done");
    Ok(())
}
