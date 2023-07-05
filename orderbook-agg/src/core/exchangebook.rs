use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::orderbook::{BookLevels, Orderbook, OrderbookArgs, Update};
use crate::{core::numtypes::*, Exchange, Symbol};

#[async_trait]
pub trait ExchangeOrderbook<
    S: Update + Send,
    U: std::fmt::Debug
        + Update
        + From<S>
        + TryFrom<Message, Error = anyhow::Error>
        + Send
        + Sync
        + 'static,
    Error = anyhow::Error,
>
{
    const BASE_URL_HTTPS: &'static str;
    const BASE_URL_WSS: &'static str;

    fn orderbook(&self) -> Arc<Mutex<Orderbook>>;
    fn base_url_https() -> Url {
        Url::parse(&Self::BASE_URL_HTTPS).unwrap()
    }
    fn base_url_wss() -> Url {
        Url::parse(&Self::BASE_URL_WSS).unwrap()
    }
    // fn tx_levels(&self) -> Arc<Mutex<watch::Sender<Option<BookLevels>>>>;

    async fn new(symbol: Symbol, price_range: u8) -> Result<Self>
    where
        Self: Sized;
    async fn new_orderbook(exchange: Exchange, symbol: Symbol, price_range: u8) -> Result<Orderbook>
    where
        Self: Sized,
    {
        let OrderbookArgs {
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        } = Self::fetch_orderbook_args(&symbol, price_range).await?;

        let orderbook = Orderbook::new(
            exchange,
            symbol,
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        );

        tracing::debug!(
            "returning orderbook for {} {} min: {} max: {} scale_p: {}, scale_q: {}",
            exchange,
            symbol,
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity
        );
        Ok(orderbook)
    }
    /// Fetches the best bid and ask from the exchange
    async fn fetch_prices(symbol: &Symbol) -> Result<(DisplayAmount, DisplayAmount)>;

    /// Fetches precious data from the exchange
    async fn fetch_orderbook_args(symbol: &Symbol, price_range: u8) -> Result<OrderbookArgs>;

    /// Fetches the initial snapshot from the exchange
    async fn fetch_snapshot(&self) -> Result<S>;

    /// Fetches the update stream from the exchange
    async fn fetch_update_stream(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>>;

    async fn start(&self, levels: u32, tx_summary: mpsc::Sender<BookLevels>) -> Result<()> {
        let (tx_update, mut rx_update) = mpsc::channel::<U>(100);

        let mut stream = self.fetch_update_stream().await?;
        let snapshot = self.fetch_snapshot().await?;
        let snapshot_update = U::from(snapshot);

        let fetcher: JoinHandle<std::result::Result<(), anyhow::Error>> =
            tokio::spawn(async move {
                tx_update
                    .send(snapshot_update)
                    .await
                    .context("failed to send snapshot")?;
                while let Some(response) = stream.next().await {
                    match response {
                        Ok(message) => match U::try_from(message) {
                            Ok(mut update) => {
                                tracing::debug!(
                                    "sending update with {} bids and {} asks",
                                    update.bids_mut().len(),
                                    update.asks_mut().len()
                                );
                                tx_update
                                    .send(update)
                                    .await
                                    .context("failed to send update")?;
                            }
                            Err(_) => {
                                tracing::error!("failed to get update from message");
                            }
                        },
                        Err(e) => {
                            tracing::error!("failed to get message, {:?}", e);
                            continue;
                        }
                    }
                }
                Ok(())
            });

        let orderbook = self.orderbook();
        while let Some(mut update) = rx_update.recv().await {
            let mut ob = orderbook.lock().await;
            let exchange = ob.exchange;
            let symbol = ob.symbol;
            tracing::debug!(
                "updating: {} {} {}",
                exchange,
                symbol,
                update.last_update_id()
            );
            if let Err(err) = ob.update(&mut update) {
                tracing::error!(
                    "failed to update orderbook: {} {} {}",
                    exchange,
                    symbol,
                    err
                );
            } else {
                if let Some(book_levels) = ob.get_book_levels(levels) {
                    tx_summary.send(book_levels.clone()).await.unwrap();
                }
            }
        }

        let _ = fetcher.await?;
        Ok(())
    }
}
