use anyhow::Result;
use orderbook_agg::symbol::{Symbol, SymbolInfo};

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let symbol = Symbol::BTCUSDT {
        info: SymbolInfo::default(),
    };

    // let symbols = vec![
    //     Symbol {
    //         symbol: "BTCUSDT".to_string(),
    //         price_range: 10,
    //     },
    //     Symbol {
    //         symbol: "BTCUSD".to_string(),
    //         price_range: 10,
    //     },
    //     Symbol {
    //         symbol: "ETHBTC".to_string(),
    //         price_range: 10,
    //     },
    // ];

    // let orderbooks: HashMap<String, SingleBook> = HashMap::new();

    // let ob = SingleBook::new(
    //     "EXCHANGE".to_string(),
    //     Symbol {
    //         symbol: "TEST".to_string(),
    //         min_price: 0,
    //         max_price: 10,
    //     },
    //     8,
    //     8,
    // )
    // .await?;

    // let mut rx_summary = ob.get_rx_summary()?;

    // tracing::info!("listening for summaries");
    // let listener_handle = tokio::spawn(async move {
    //     loop {
    //         match rx_summary.changed().await {
    //             Ok(_) => {
    //                 if let Messages::Summary(message) = rx_summary.borrow().deref() {
    //                     tracing::info!("received summary: {:?}", message);
    //                 }
    //             }
    //             Err(err) => {
    //                 tracing::info!("rx_summary error: {:?}", err);
    //                 break;
    //             }
    //         }
    //     }
    // });
    // let summary_handler = tokio::spawn(async move { ob.start().await });

    // listener_handle.await?;
    // let _ = summary_handler.await?;

    Ok(())
}
