use std::io::stdout;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use app::{App, AppReturn};
use inputs::events::Events;
use inputs::InputEvent;
use io::IoEvent;
use orderbook_agg::booksummary::orderbook_aggregator_client::OrderbookAggregatorClient;
use orderbook_agg::booksummary::SummaryRequest;
use tui::backend::CrosstermBackend;
use tui::Terminal;

use crate::app::ui;

pub mod app;
pub mod inputs;
pub mod io;

pub async fn start_ui(
    app: &Arc<tokio::sync::Mutex<App>>,
    summary_request: SummaryRequest,
) -> Result<()> {
    // Configure Crossterm backend for tui
    let stdout = stdout();
    crossterm::terminal::enable_raw_mode()?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    terminal.hide_cursor()?;

    // User event handler
    let tick_rate = Duration::from_millis(100);
    let client: OrderbookAggregatorClient<tonic::transport::Channel> =
        OrderbookAggregatorClient::connect("http://127.0.0.1:9001").await?;
    let decimals = summary_request.decimals;
    let symbol = summary_request.symbol.clone();
    let mut events = Events::new(tick_rate, client, summary_request);

    // Trigger state change from Init to Initialized
    {
        let mut app = app.lock().await;
        // Here we assume the the first load is a long task
        app.dispatch(IoEvent::Initialize).await;
    }

    loop {
        let mut app = app.lock().await;

        // Render
        terminal.draw(|rect| ui::draw(rect, &app, &symbol, decimals))?;

        // Handle inputs
        let result = match events.next().await {
            InputEvent::Input(key) => app.do_action(key).await,
            InputEvent::Tick => app.update_on_tick().await,
            InputEvent::Update(summary) => app.update_summary(summary).await,
        };
        // Check if we should exit
        if result == AppReturn::Exit {
            events.close();
            break;
        }
    }

    // Restore the terminal and close application
    terminal.clear()?;
    terminal.show_cursor()?;
    crossterm::terminal::disable_raw_mode()?;

    Ok(())
}
