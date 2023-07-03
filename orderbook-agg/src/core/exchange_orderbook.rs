use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::{
    ops::DivAssign,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::orderbook::{Orderbook, OrderbookMessage, Update};
use crate::{booksummary::Exchange, core::number_types::*, Symbol};

#[derive(Default)]
pub struct ExchangeInfo {
    pub storage_price_min: StoragePrice,
    pub storage_price_max: StoragePrice,
    pub scale_price: u32,
    pub scale_quantity: u32,
}

impl ExchangeInfo {
    pub(crate) fn get_min_max(
        price: DisplayPrice,
        price_range: u8,
        scale: u32,
    ) -> Result<(StoragePrice, StoragePrice)> {
        let mut range = Decimal::from(price_range);
        range.div_assign(Decimal::new(1, 2));
        range.checked_add(Decimal::new(1, 0)).unwrap();
        let min: StoragePrice = price.checked_div(range).unwrap().to_storage(scale)?;
        let max: StoragePrice = price.checked_mul(range).unwrap().to_storage(scale)?;
        Ok((min, max))
    }
}

#[async_trait]
pub trait ExchangeOrderbook<
    S: Update,
    U: Update + TryFrom<Message, Error = anyhow::Error> + Send + Sync + 'static,
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
        let ExchangeInfo {
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        } = Self::fetch_exchange_info(&symbol, price_range).await?;

        let orderbook = Orderbook::new(
            exchange,
            symbol,
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        );

        tracing::debug!(
            "returning orderbook for {} {}",
            exchange.as_str_name(),
            symbol
        );
        Ok(orderbook)
    }
    /// Fetches the best bid and ask from the exchange
    async fn fetch_prices(symbol: &Symbol) -> Result<(DisplayPrice, DisplayPrice)>;

    /// Fetches precious data from the exchange
    async fn fetch_exchange_info(symbol: &Symbol, price_range: u8) -> Result<ExchangeInfo>;

    /// Fetches the initial snapshot from the exchange
    async fn fetch_snapshot(&self) -> Result<S>;

    /// Fetches the update stream from the exchange
    async fn fetch_update_stream(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>>;
    fn update(mut ob: MutexGuard<Orderbook>, update: &mut U) -> Result<()> {
        if update.last_update_id() > ob.last_update_id {
            ob.last_update_id = update.last_update_id();
        } else {
            tracing::info!("Skipping update",);
            return Ok(());
        }

        // this is set up this way to be able to consume the update without copying it
        for bid in update.bids_mut().into_iter() {
            ob.add_bid(*bid)?
        }
        for ask in update.asks_mut().into_iter() {
            ob.add_ask(*ask)?
        }

        Ok(())
    }
    async fn start(&self) -> Result<()> {
        let (tx_update, mut rx_update) = mpsc::channel::<OrderbookMessage<U>>(100);
        let ob_clone = self.orderbook().clone();
        let tx_summary = self.tx_summary().clone();

        let mut stream = self.fetch_update_stream().await?;

        let fetcher: JoinHandle<std::result::Result<(), anyhow::Error>> =
            tokio::spawn(async move {
                while let Some(response) = stream.next().await {
                    match response {
                        Ok(message) => match U::try_from(message) {
                            Ok(update) => {
                                tx_update
                                    .send(OrderbookMessage::Update(update))
                                    .await
                                    .context("failed to send update")?;
                            }
                            Err(err) => {
                                tracing::error!("failed to get update from message {}", err);
                            }
                        },
                        Err(err) => {
                            tracing::error!("failed to get message: {}", err);
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
                        tracing::info!("update last_update_id: {}", update.last_update_id());
                        let last_update_id = update.last_update_id();
                        let ob = ob_clone.lock().unwrap();

                        if let Err(err) = Self::update(ob, &mut update) {
                            tracing::error!("failed to update orderbook: {}", err);
                        }
                        let _ = tx_summary
                            .lock()
                            .unwrap()
                            .send_replace(OrderbookMessage::Summary(format!(
                                "received a summary {last_update_id} {tx_count}",
                            )));

                        tracing::info!("sent summary {tx_count}!");
                        tx_count += 1;
                    }
                    _ => {
                        tracing::info!("received something else");
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
