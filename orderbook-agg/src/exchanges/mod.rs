use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::booksummary::ExchangeType;
use crate::utils::{display_to_storage_price, display_to_storage_quantity};

pub mod binance;

pub type StoragePrice = u32;
pub type StorageQuantity = u64;

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
    fn symbol(&self) -> Symbol {
        self.exchange_symbol_info.symbol
    }
    fn bids(&self) -> Arc<Mutex<Vec<StorageQuantity>>> {
        self.bids.clone()
    }
    fn asks(&self) -> Arc<Mutex<Vec<StorageQuantity>>> {
        self.asks.clone()
    }
    fn tx_summary(&self) -> Arc<Mutex<watch::Sender<OrderbookMessage>>> {
        self.tx_summary.clone()
    }
    fn rx_summary(&self) -> watch::Receiver<OrderbookMessage> {
        self.tx_summary().lock().unwrap().subscribe()
    }

    fn storage_price_min(&self) -> Result<u32> {
        self.exchange_symbol_info.storage_price_min()
    }

    fn storage_price_max(&self) -> Result<u32> {
        self.exchange_symbol_info.storage_price_max()
    }

    fn storage_price(&self, display_price: Decimal) -> Result<u32> {
        self.exchange_symbol_info.storage_price(display_price)
    }

    fn storage_quantity(&self, display_quantity: Decimal) -> Result<u64> {
        self.exchange_symbol_info.storage_quantity(display_quantity)
    }

    fn display_price(&self, storage_price: StoragePrice) -> Decimal {
        self.exchange_symbol_info.display_price(storage_price)
    }

    fn display_quantity(&self, storage_quantity: StorageQuantity) -> Decimal {
        self.exchange_symbol_info
            .get_display_quantity(storage_quantity)
    }
    fn idx(&self, storage_price: StoragePrice) -> usize {
        let storage_price_min = self.storage_price_min().unwrap();
        (storage_price - storage_price_min) as usize
    }
    /// Adds, modifies or removes a bid from the order book.
    pub fn add_bid(&mut self, level: [Decimal; 2]) -> Result<()> {
        let mut storage_price = self.exchange_symbol_info.storage_price(level[0])?;
        if storage_price > self.storage_price_max()? || storage_price < self.storage_price_min()? {
            return Ok(());
        }

        let mut bids = self.bids.lock().unwrap();

        let storage_quantity = self.storage_quantity(level[1])?;
        let mut idx = self.idx(storage_price);

        bids[idx] = storage_quantity;

        if storage_quantity > 0 {
            if storage_price > self.storage_bid_max {
                self.storage_bid_max = storage_price;
            }
        } else {
            if storage_price == self.storage_bid_max && storage_quantity == 0 {
                loop {
                    storage_price -= 1;
                    idx = self.idx(storage_price);
                    if bids[idx] > 0 {
                        self.storage_bid_max = storage_price;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    /// Adds, modifies or removes a bid from the order book.
    pub fn add_ask(&mut self, level: [Decimal; 2]) -> Result<()> {
        let mut storage_price = self.exchange_symbol_info.storage_price(level[0])?;
        if storage_price > self.storage_price_max()? || storage_price < self.storage_price_min()? {
            return Ok(());
        }

        let storage_quantity = self.storage_quantity(level[1])?;
        let mut idx = self.idx(storage_price);

        let mut asks = self.asks.lock().unwrap();

        if storage_quantity > 0 {
            if storage_price < self.storage_ask_min {
                self.storage_ask_min = storage_price;
            }
        } else {
            if storage_price == self.storage_ask_min && storage_quantity == 0 {
                loop {
                    storage_price += 1;
                    idx = self.idx(storage_price);
                    if asks[idx] > 0 {
                        self.storage_ask_min = storage_price;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

pub trait ExchangeOrderbookProps {
    const BASE_URL_HTTPS: &'static str;
    const BASE_URL_WSS: &'static str;

    fn orderbook(&self) -> &Orderbook;
    fn base_url_https() -> Url {
        Url::parse(&Self::BASE_URL_HTTPS).unwrap()
    }
    fn base_url_wss() -> Url {
        Url::parse(&Self::BASE_URL_WSS).unwrap()
    }
}

/// This is an orderbooks for a given excange and symbol
#[async_trait]
pub trait ExchangeOrderbookMethods<S: UpdateState>: ExchangeOrderbookProps {
    async fn fetch_symbol_info(
        symbol: Symbol,
        // this specifies the percent above and below the current bid price the max and min prices will be for the orderbook
        price_range: i64,
    ) -> Result<Symbol>;
    async fn fetch_exchange_symbol_info(symbol: Symbol) -> Result<ExchangeSymbolInfo>;
    async fn fetch_current_bid_price(symbol: &Symbol) -> Result<Decimal>;
    async fn fetch_snapshot(&self) -> Result<S>;
    async fn fetch_update_stream(
        &self,
    ) -> Result<Result<WebSocketStream<MaybeTlsStream<TcpStream>>>>;
    fn update_orderbook<U: UpdateState>(&self, update: U) -> Result<()>;
    fn create_orderbook(exchange_symbol_info: ExchangeSymbolInfo) -> Result<Orderbook> {
        let storage_price_min = exchange_symbol_info.storage_price_min()?;
        let storage_price_max = exchange_symbol_info.storage_price_max()?;

        let capacity = (storage_price_min - storage_price_max) as usize;
        let mut bids: Vec<StorageQuantity> = Vec::with_capacity(capacity + 1);
        let mut asks: Vec<StorageQuantity> = Vec::with_capacity(capacity + 1);

        for _ in 0..capacity {
            bids.push(0);
            asks.push(0);
        }

        let (tx_summary, _) = watch::channel(OrderbookMessage::Summary("hello".to_string()));

        let orderbook = Orderbook {
            exchange_symbol_info,
            storage_ask_min: u32::MAX,
            storage_bid_max: u32::MIN,
            bids: Arc::new(Mutex::new(bids)),
            asks: Arc::new(Mutex::new(asks)),
            last_update_id: u64::MIN,
            tx_summary: Arc::new(Mutex::new(tx_summary)),
        };
        Ok(orderbook)
    }

    async fn start(&self) -> Result<()> {
        let (tx_update, mut rx_update) = mpsc::channel(100);
        let bids_clone = self.orderbook().bids();
        let tx_summary_clone = self.orderbook().tx_summary();

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SymbolInfo {
    pub display_price_min: Decimal,
    pub display_price_max: Decimal,
}

impl Default for SymbolInfo {
    fn default() -> Self {
        Self {
            display_price_min: Decimal::new(0, 0),
            display_price_max: Decimal::MAX,
        }
    }
}

// we have a symbol to make sure we are using the same symbol across all exchanges
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Symbol {
    BTCUSDT { info: SymbolInfo },
    BTCUSD { info: SymbolInfo },
    ETHBTC { info: SymbolInfo },
}

impl Symbol {
    pub fn info(&self) -> SymbolInfo {
        match self {
            Symbol::BTCUSDT { info } => *info,
            Symbol::BTCUSD { info } => *info,
            Symbol::ETHBTC { info } => *info,
        }
    }
    pub fn set_info(&self, info: SymbolInfo) -> Self {
        match self {
            Symbol::BTCUSDT { .. } => Symbol::BTCUSDT { info },
            Symbol::BTCUSD { .. } => Symbol::BTCUSD { info },
            Symbol::ETHBTC { .. } => Symbol::ETHBTC { info },
        }
    }
}

impl Default for Symbol {
    fn default() -> Self {
        Symbol::BTCUSDT {
            info: SymbolInfo::default(),
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Symbol::BTCUSDT { .. } => write!(f, "BTCUSDT"),
            Symbol::BTCUSD { .. } => write!(f, "BTCUSD"),
            Symbol::ETHBTC { .. } => write!(f, "ETHBTC"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExchangeSymbolInfo {
    pub exchange: ExchangeType,
    pub symbol: Symbol,
    pub scale_price: u32,
    pub scale_quantity: u32,
}

impl ExchangeSymbolInfo {
    pub fn storage_price_min(&self) -> Result<u32> {
        let info = self.symbol.info();
        self.storage_price(info.display_price_min)
    }

    pub fn storage_price_max(&self) -> Result<u32> {
        let info = self.symbol.info();
        self.storage_price(info.display_price_max)
    }

    pub fn storage_price(&self, display_price: Decimal) -> Result<u32> {
        display_to_storage_price(display_price, self.scale_price)
    }

    pub fn storage_quantity(&self, display_quantity: Decimal) -> Result<u64> {
        display_to_storage_quantity(display_quantity, self.scale_quantity)
    }

    pub fn display_price(&self, storage_price: StoragePrice) -> Decimal {
        let mut decimal = Decimal::from_u32(storage_price).unwrap();
        let scale = decimal.scale();
        decimal.set_scale(scale - self.scale_price).unwrap();
        decimal
    }

    pub fn get_display_quantity(&self, storage_quantity: StorageQuantity) -> Decimal {
        let mut decimal = Decimal::from_u64(storage_quantity).unwrap();
        let scale = decimal.scale();
        decimal.set_scale(scale - self.scale_quantity).unwrap();
        decimal
    }
}

/// Updates from all exchanges should implement this trait
pub trait UpdateState {
    fn last_update_id(&self) -> u64;
    fn bids_mut(self) -> Vec<[Decimal; 2]>;
    fn asks_mut(self) -> Vec<[Decimal; 2]>;
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let display_price = exchange_symbol_info.display_price(4200);
        assert_eq!(display_price.to_string(), "42.00");

        let storage_price = exchange_symbol_info
            .storage_price(Decimal::new(u32::MAX as i64, 2))
            .unwrap();
        assert_eq!(storage_price, u32::MAX);

        if let Err(err) = exchange_symbol_info.storage_price(Decimal::new(u32::MAX as i64 + 1, 2)) {
            assert_eq!(err.to_string(), "price is too large");
        } else {
            panic!("greater than u32 should have failed");
        };

        if let Err(err) = exchange_symbol_info.storage_price(Decimal::new(0, 0)) {
            assert_eq!(err.to_string(), "price must be greater than 0");
        } else {
            panic!("not greater than 0 should have failed");
        };
    }
}
