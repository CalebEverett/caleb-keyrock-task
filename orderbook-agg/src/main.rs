use std::ops::Deref;

use anyhow::Result;
use futures::future::try_join_all;
use orderbook_agg::{
    core::exchange_book::ExchangeOrderbook, core::orderbook::OrderbookMessage,
    exchanges::binance::BinanceOrderbook, Exchange, Symbol,
};
use tokio::task::JoinHandle;

async fn start(
    exchange: Exchange,
    symbol: Symbol,
    price_range: u8,
    levels: u32,
) -> Result<Vec<JoinHandle<()>>> {
    let orderbook = BinanceOrderbook::new(exchange, symbol, price_range).await?;
    tracing::info!("starting {exchange} {symbol}, price_range: {price_range}, levels: {levels}");

    let mut rx_summary = orderbook.rx_summary();

    let rx_handle = tokio::spawn(async move {
        while rx_summary.changed().await.is_ok() {
            if let OrderbookMessage::BookLevels(book_levels) = rx_summary.borrow().deref() {
                if book_levels.bids.is_empty() || book_levels.asks.is_empty() {
                    continue;
                }
                println!(
                    "exchange: {}, symbol, {}, best_bid: {:?}, best_ask: {:?}, spread: {:?}, bid_qty: {}, ask_qty: {}",
                    exchange,
                    symbol,
                    book_levels.bids[0].0,
                    book_levels.asks[0].0,
                    (book_levels.asks[0].0 - book_levels.bids[0].0),
                    book_levels.bids.len(),
                    book_levels.asks.len()
                );
            }
        }
    });
    let ob_handle = tokio::spawn(async move {
        orderbook.start(levels).await.unwrap();
    });
    Ok(vec![rx_handle, ob_handle])
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let symbols = vec![Symbol::BTCUSDT, Symbol::BTCUSD];
    let exchange = Exchange::BINANCE;
    let price_range = 3;
    let levels = 2;

    let handles = try_join_all(
        symbols
            .iter()
            .map(|symbol| start(exchange, *symbol, price_range, levels))
            .collect::<Vec<_>>(),
    )
    .await?;

    for handles in handles {
        for handle in handles {
            handle.await?;
        }
    }

    tracing::info!("done");
    Ok(())
}
