use anyhow::Result;
use orderbook_agg::exchanges::{
    binance::{data::Update, BinanceOrderbook},
    ExchangeOrderbookMethods, ExchangeOrderbookState, Symbol, SymbolInfo,
};
use tokio::task::JoinHandle;

async fn start(symbol: Symbol) -> Result<Vec<JoinHandle<()>>> {
    let symbol = BinanceOrderbook::fetch_symbol_info(symbol, 5).await?;
    let exchange_symbol_info = BinanceOrderbook::fetch_exchange_symbol_info(symbol).await?;
    let orderbook = BinanceOrderbook::<Update>::new(exchange_symbol_info);
    let ob_clone = orderbook.orderbook();
    let mut rx_summary = {
        let ob = ob_clone.lock().unwrap();
        ob.rx_summary()
    };

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
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let symbols = vec![
        Symbol::BTCUSDT {
            info: SymbolInfo::default(),
        },
        Symbol::BTCUSD {
            info: SymbolInfo::default(),
        },
    ];

    let handles = futures::future::join_all(symbols.into_iter().map(|s| start(s))).await;
    for handle in handles.into_iter().flatten().flatten() {
        handle.await?;
    }

    Ok(())
}
