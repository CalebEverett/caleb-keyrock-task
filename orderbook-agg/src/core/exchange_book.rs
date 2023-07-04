use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::orderbook::{Orderbook, OrderbookArgs, OrderbookMessage, Update};
use crate::{core::num_types::*, Exchange, Symbol};

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
    fn tx_summary(&self) -> Arc<Mutex<watch::Sender<OrderbookMessage<U>>>>;
    fn rx_summary(&self) -> watch::Receiver<OrderbookMessage<U>>;

    async fn new(exchange: Exchange, symbol: Symbol, price_range: u8) -> Result<Self>
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
    async fn fetch_prices(symbol: &Symbol) -> Result<(DisplayPrice, DisplayPrice)>;

    /// Fetches precious data from the exchange
    async fn fetch_orderbook_args(symbol: &Symbol, price_range: u8) -> Result<OrderbookArgs>;

    /// Fetches the initial snapshot from the exchange
    async fn fetch_snapshot(&self) -> Result<S>;

    /// Fetches the update stream from the exchange
    async fn fetch_update_stream(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>>;
    fn update(mut ob: MutexGuard<Orderbook>, update: &mut U) -> Result<()> {
        tracing::debug!("update {:#?}", update);

        update.validate(ob.last_update_id)?;

        // this is set up this way to be able to consume the update without copying it
        for bid in update.bids_mut().into_iter() {
            tracing::debug!("adding bid: {:?}", bid);
            ob.add_bid(*bid)?
        }
        for ask in update.asks_mut().into_iter() {
            tracing::debug!("adding ask: {:?}", ask);
            ob.add_ask(*ask)?
        }

        ob.last_update_id = update.last_update_id();

        Ok(())
    }
    async fn start(&self, levels: u32) -> Result<()> {
        let (tx_update, mut rx_update) = mpsc::channel::<OrderbookMessage<U>>(100);
        let ob_clone = self.orderbook().clone();
        let tx_summary = self.tx_summary().clone();

        let mut stream = self.fetch_update_stream().await?;
        let snapshot = self.fetch_snapshot().await?;
        let snapshot_update = U::from(snapshot);

        let fetcher: JoinHandle<std::result::Result<(), anyhow::Error>> =
            tokio::spawn(async move {
                tx_update
                    .send(OrderbookMessage::Update(snapshot_update))
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
                                    .send(OrderbookMessage::Update(update))
                                    .await
                                    .context("failed to send update")?;
                            }
                            Err(_) => {
                                tracing::error!("failed to get update from message");
                            }
                        },
                        Err(_) => {
                            tracing::error!("failed to get message");
                            continue;
                        }
                    }
                }
                Ok(())
            });

        let updater: JoinHandle<()> = tokio::spawn(async move {
            let mut tx_count = 1;
            while let Some(message) = rx_update.recv().await {
                match message {
                    OrderbookMessage::Update(mut update) => {
                        let ob = ob_clone.lock().unwrap();
                        let exchange = ob.exchange;
                        let symbol = ob.symbol;
                        tracing::debug!(
                            "updating: {} {} {}",
                            exchange,
                            symbol,
                            update.last_update_id()
                        );
                        if let Err(err) = Self::update(ob, &mut update) {
                            tracing::error!(
                                "failed to update orderbook: {} {} {}",
                                exchange,
                                symbol,
                                err
                            );
                        } else {
                            let ob = ob_clone.lock().unwrap();
                            if !ob.bids.is_empty() && !ob.asks.is_empty() {
                                let book_levels = ob.get_book_levels(levels);
                                let _ = tx_summary
                                    .lock()
                                    .unwrap()
                                    .send_replace(OrderbookMessage::BookLevels(book_levels));
                            }
                        }
                        tracing::debug!("sent summary {tx_count}!");
                        tx_count += 1;
                    }
                    _ => {
                        tracing::error!("received something else");
                        continue;
                    }
                }
            }
        });

        updater.await?;
        let _ = fetcher.await?;
        Ok(())
    }
}
