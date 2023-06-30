use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::{
    symbol::ExchangeSymbolInfo,
    symbol::{StoragePrice, StorageQuantity, Symbol},
    update::Update,
};

#[derive(Debug)]
pub enum OrderbookMessage {
    Update(String),
    Summary(String),
}
pub struct Orderbook {
    pub exchange_symbol_info: ExchangeSymbolInfo,
    pub storage_ask_min: StoragePrice,
    pub storage_bid_max: StoragePrice,
    pub bids: Arc<Mutex<Vec<StorageQuantity>>>,
    pub asks: Arc<Mutex<Vec<StorageQuantity>>>,
    pub last_update_id: u64,
    pub tx_summary: Arc<Mutex<watch::Sender<OrderbookMessage>>>,
}

impl Orderbook {
    fn get_bids(&self) -> Arc<Mutex<Vec<StorageQuantity>>> {
        self.bids.clone()
    }
    fn get_asks(&self) -> Arc<Mutex<Vec<StorageQuantity>>> {
        self.asks.clone()
    }
    fn get_rx_summary(&self) -> watch::Receiver<OrderbookMessage> {
        self.tx_summary.lock().unwrap().subscribe()
    }
    fn get_tx_summary(&self) -> Arc<Mutex<watch::Sender<OrderbookMessage>>> {
        self.tx_summary.clone()
    }
}

pub trait ExchangeOrderbookProps {
    fn get_orderbook(&self) -> &Orderbook;
    fn get_base_url_https(&self) -> &Url;
    fn get_base_url_wss(&self) -> &Url;
}

/// This is an orderbooks for a given excange and symbol
#[async_trait]
pub trait ExchangeOrderbookMethods: ExchangeOrderbookProps {
    async fn fetch_symbol_info(
        &self,
        symbol: Symbol,
        // this specifies the percent above and below the current bid price the max and min prices will be for the orderbook
        price_range: i64,
    ) -> Result<Symbol>;
    async fn fetch_exchange_symbol_info(&self, symbol: Symbol) -> Result<ExchangeSymbolInfo>;
    async fn fetch_current_bid_price(&self, symbol: &Symbol) -> Result<Decimal>;
    async fn fetch_snapshot(&self) -> Result<Update>;
    async fn fetch_update_stream(
        &self,
    ) -> Result<Result<WebSocketStream<MaybeTlsStream<TcpStream>>>>;
    async fn update_orderbook(&self, update: Update) -> Result<()>;
    fn create_orderbook(exchange_symbol_info: ExchangeSymbolInfo) -> Result<Orderbook> {
        let storage_price_min = exchange_symbol_info.get_storage_price_min()?;
        let storage_price_max = exchange_symbol_info.get_storage_price_max()?;

        let capacity = (storage_price_min - storage_price_max) as usize;
        let bids: Arc<Mutex<Vec<StorageQuantity>>> =
            Arc::new(Mutex::new(Vec::with_capacity(capacity + 1)));
        let asks: Arc<Mutex<Vec<StorageQuantity>>> =
            Arc::new(Mutex::new(Vec::with_capacity(capacity + 1)));

        for _ in 0..capacity {
            bids.lock().unwrap().push(0);
            asks.lock().unwrap().push(0);
        }

        let (tx_summary, _) = watch::channel(OrderbookMessage::Summary("hello".to_string()));

        let orderbook = Orderbook {
            exchange_symbol_info,
            storage_ask_min: u32::MAX,
            storage_bid_max: 0,
            bids,
            asks,
            last_update_id: 0,
            tx_summary: Arc::new(Mutex::new(tx_summary)),
        };
        Ok(orderbook)
    }

    async fn start(&self) -> Result<()> {
        let (tx_update, mut rx_update) = mpsc::channel(100);

        // updater
        let bids_clone = self.get_orderbook().get_bids().clone();
        let tx_summary_clone = self.get_orderbook().get_tx_summary().clone();

        let updater = tokio::spawn(async move {
            let mut tx_count = 1;
            while let Some(message) = rx_update.recv().await {
                match message {
                    OrderbookMessage::Update(msg) => {
                        tracing::info!("received an update: {}", msg);

                        bids_clone.lock().unwrap().push(1);
                        let _ = tx_summary_clone.lock().unwrap().send_replace(
                            OrderbookMessage::Summary(format!("here is a summary {tx_count}!")),
                        );

                        tracing::info!("sent summary {tx_count}!");
                        tx_count += 1;
                    }
                    msg => {
                        tracing::info!("received something else {:?}", msg);
                        continue;
                    }
                }
            }
        });

        // fetcher - gets updates from the exchange and sends them to the updater
        let fetcher = tokio::spawn(async move {
            for _ in 0..3 {
                let _ = tx_update
                    .send(OrderbookMessage::Update("here is an update!".to_string()))
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });

        updater.await?;
        fetcher.await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        booksummary::ExchangeType,
        symbol::{Symbol, SymbolInfo},
    };

    #[test]
    fn it_converts_prices_correctly() {
        let symbol_info = SymbolInfo {
            display_price_min: Decimal::new(700, 2),
            display_price_max: Decimal::new(4200, 2),
        };
        let symbol = Symbol::BTCUSDT { info: symbol_info };

        let exchange_symbol_info: ExchangeSymbolInfo = ExchangeSymbolInfo {
            exchange: ExchangeType::default(),
            symbol,
            scale_price: 2,
            scale_quantity: 8,
        };

        let display_price = exchange_symbol_info.get_display_price(4200);
        assert_eq!(display_price.to_string(), "42.00");

        let storage_price = exchange_symbol_info
            .get_storage_price(Decimal::new(u32::MAX as i64, 2))
            .unwrap();
        assert_eq!(storage_price, u32::MAX);

        if let Err(err) =
            exchange_symbol_info.get_storage_price(Decimal::new(u32::MAX as i64 + 1, 2))
        {
            assert_eq!(err.to_string(), "price is too large");
        } else {
            panic!("greater than u32 should have failed");
        };

        if let Err(err) = exchange_symbol_info.get_storage_price(Decimal::new(0, 0)) {
            assert_eq!(err.to_string(), "price must be greater than 0");
        } else {
            panic!("not greater than 0 should have failed");
        };
    }
}
