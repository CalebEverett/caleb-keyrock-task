use clap::Parser;
use orderbook_agg::book_summary::SummaryRequest;
use std::sync::Arc;

use anyhow::Result;
use log::LevelFilter;
use terminal_app::app::App;
use terminal_app::io::handler::IoAsyncHandler;
use terminal_app::io::IoEvent;
use terminal_app::start_ui;

#[derive(Debug, Parser)]
struct SummaryOptions {
    #[clap(long)]
    symbol: String,
    #[clap(long)]
    levels: Option<u32>,
    #[clap(long)]
    price_range: Option<f64>,
    #[clap(long)]
    decimals: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let (sync_io_tx, mut sync_io_rx) = tokio::sync::mpsc::channel::<IoEvent>(100);

    // We need to share the App between thread
    let app = Arc::new(tokio::sync::Mutex::new(App::new(sync_io_tx.clone())));
    let app_ui = Arc::clone(&app);

    // Configure log
    tui_logger::init_logger(LevelFilter::Debug).unwrap();
    tui_logger::set_default_level(log::LevelFilter::Debug);

    // Handle IO in a specifc thread
    tokio::spawn(async move {
        let mut handler = IoAsyncHandler::new(app);
        while let Some(io_event) = sync_io_rx.recv().await {
            handler.handle_io_event(io_event).await;
        }
    });

    let opts = SummaryOptions::parse();
    let summary_request = SummaryRequest {
        symbol: opts.symbol,
    };

    start_ui(&app_ui, summary_request).await?;

    Ok(())
}
