use anyhow::Result;
use orderbook_agg::{
    booksummary::Exchange, core::exchange_orderbook::ExchangeOrderbook,
    exchanges::binance::BinanceOrderbook, Symbol,
};
use tokio::task::JoinHandle;

async fn start(exchange: Exchange, symbol: Symbol, price_range: u8) -> Result<Vec<JoinHandle<()>>> {
    let orderbook = BinanceOrderbook::new(exchange, symbol, price_range).await?;
    tracing::info!("starting {} {}", exchange.as_str_name(), symbol);

    let mut rx_summary = orderbook.rx_summary();

    let rx_handle = tokio::spawn(async move {
        while rx_summary.changed().await.is_ok() {
            println!("summary: {:?}", *rx_summary.borrow());
        }
    });
    let ob_handle = tokio::spawn(async move {
        orderbook.start().await.unwrap();
    });
    Ok(vec![rx_handle, ob_handle])
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let symbols = vec![Symbol::BTCUSDT, Symbol::BTCUSD];
    let exchange = Exchange::Binance;
    let symbol = Symbol::BTCUSDT;
    let price_range = 3;
    let orderbook = BinanceOrderbook::new(exchange, symbol, price_range).await?;
    tracing::info!("starting {} {}", exchange.as_str_name(), symbol);

    let mut rx_summary = orderbook.rx_summary();

    let rx_handle = tokio::spawn(async move {
        while rx_summary.changed().await.is_ok() {
            println!("summary: {:?}", *rx_summary.borrow());
        }
    });
    tracing::info!("created rx_handle");
    let ob_handle = tokio::spawn(async move {
        orderbook.start().await.unwrap();
    });

    rx_handle.await?;
    ob_handle.await?;

    tracing::info!("done");
    Ok(())
}
