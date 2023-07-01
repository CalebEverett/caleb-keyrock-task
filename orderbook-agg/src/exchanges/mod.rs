use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::booksummary::ExchangeType;
use crate::utils::{display_to_storage_price, display_to_storage_quantity};

pub mod binance;

pub type StoragePrice = u32;
pub type StorageQuantity = u64;

#[derive(Debug)]
pub enum OrderbookMessage<U: UpdateState + 'static> {
    Update(U),
    Summary(String),
}

#[derive(Debug)]
pub struct Orderbook<U: UpdateState + 'static> {
    pub exchange_symbol_info: ExchangeSymbolInfo,
    pub storage_ask_min: StoragePrice,
    pub storage_bid_max: StoragePrice,
    // pub bids: Arc<Mutex<Vec<StorageQuantity>>>,
    pub bids: Vec<StorageQuantity>,
    pub asks: Arc<Mutex<Vec<StorageQuantity>>>,
    pub last_update_id: u64,
    pub tx_summary: Arc<Mutex<watch::Sender<OrderbookMessage<U>>>>,
}

impl<U: UpdateState> Orderbook<U> {
    fn new(exchange_symbol_info: ExchangeSymbolInfo) -> Orderbook<U> {
        let storage_price_min = exchange_symbol_info.storage_price_min();
        let storage_price_max = exchange_symbol_info.storage_price_max();

        let capacity = (storage_price_max - storage_price_min) as usize + 1;
        let mut bids: Vec<StorageQuantity> = Vec::with_capacity(capacity);
        let mut asks: Vec<StorageQuantity> = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            bids.push(0);
            asks.push(0);
        }

        let (tx_summary, _) = watch::channel(OrderbookMessage::Summary("hello".to_string()));

        let orderbook = Orderbook {
            exchange_symbol_info,
            storage_ask_min: u32::MAX,
            storage_bid_max: u32::MIN,
            // bids: Arc::new(Mutex::new(bids)),
            bids,
            asks: Arc::new(Mutex::new(asks)),
            last_update_id: u64::MIN,
            tx_summary: Arc::new(Mutex::new(tx_summary)),
        };
        orderbook
    }
    fn symbol(&self) -> Symbol {
        self.exchange_symbol_info.symbol
    }
    // fn bids(&self) -> Arc<Mutex<Vec<StorageQuantity>>> {
    //     self.bids.clone()
    // }
    fn bids(&self) -> &Vec<StorageQuantity> {
        &self.bids
    }
    fn bids_mut(&mut self) -> &mut Vec<StorageQuantity> {
        &mut self.bids
    }
    fn asks(&self) -> Arc<Mutex<Vec<StorageQuantity>>> {
        self.asks.clone()
    }
    fn tx_summary(&self) -> Arc<Mutex<watch::Sender<OrderbookMessage<U>>>> {
        self.tx_summary.clone()
    }
    pub fn rx_summary(&self) -> watch::Receiver<OrderbookMessage<U>> {
        self.tx_summary().lock().unwrap().subscribe()
    }

    pub fn storage_price_min(&self) -> StoragePrice {
        self.exchange_symbol_info.storage_price_min()
    }

    pub fn storage_price_max(&self) -> StoragePrice {
        self.exchange_symbol_info.storage_price_max()
    }

    pub fn display_price_min(&self) -> Decimal {
        self.exchange_symbol_info.display_price_min()
    }

    pub fn display_price_max(&self) -> Decimal {
        self.exchange_symbol_info.display_price_max()
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
        let storage_price_min = self.storage_price_min();
        (storage_price - storage_price_min) as usize
    }
    /// Adds, modifies or removes a bid from the order book.
    pub fn add_bid(&mut self, level: [Decimal; 2]) -> Result<()> {
        let mut storage_price = self.exchange_symbol_info.storage_price(level[0])?;
        if storage_price > self.storage_price_max() || storage_price < self.storage_price_min() {
            return Ok(());
        }

        let storage_quantity = self.storage_quantity(level[1])?;
        let mut idx = self.idx(storage_price);

        // let mut bids = self.bids.lock().unwrap();

        {
            let bids = self.bids_mut();
            bids[idx] = storage_quantity;
        }

        if storage_quantity > 0 {
            if storage_price > self.storage_bid_max {
                self.storage_bid_max = storage_price;
            }
        } else {
            if storage_price == self.storage_bid_max && storage_quantity == 0 {
                loop {
                    storage_price -= 1;
                    idx = self.idx(storage_price).clone();
                    if &self.bids[idx] > &0 {
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
        if storage_price > self.storage_price_max() || storage_price < self.storage_price_min() {
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
    /// Processes updates to the orderbook from an exchange.
    /// TODO: Try to parallelize this - the number of collisions is likely to be low.
    pub fn update(&mut self, update: U) -> Result<()> {
        if update.last_update_id() > self.last_update_id {
            self.last_update_id = update.last_update_id();
        } else {
            tracing::info!("Skipping update",);
            return Ok(());
        }

        for bid in update.bids_mut().into_iter() {
            self.add_bid(bid)?
        }

        // for ask in update.asks.into_iter() {
        //     self.add_ask(update.exchange.context("exchange is none")?, ask)?
        // }

        Ok(())
    }
}

pub trait ExchangeOrderbookState<U: UpdateState> {
    const BASE_URL_HTTPS: &'static str;
    const BASE_URL_WSS: &'static str;

    fn orderbook(&self) -> Arc<Mutex<Orderbook<U>>>;
    fn base_url_https() -> Url {
        Url::parse(&Self::BASE_URL_HTTPS).unwrap()
    }
    fn base_url_wss() -> Url {
        Url::parse(&Self::BASE_URL_WSS).unwrap()
    }
}

/// This is an orderbooks for a given excange and symbol
#[async_trait]
pub trait ExchangeOrderbookMethods<
    S: UpdateState,
    U: UpdateState + TryFrom<Message, Error = anyhow::Error> + Send + Sync + 'static,
>: ExchangeOrderbookState<U>
{
    async fn fetch_symbol_info(
        symbol: Symbol,
        // this specifies the percent above and below the current bid price the max and min prices will be for the orderbook
        price_range: i64,
    ) -> Result<Symbol>;
    async fn fetch_exchange_symbol_info(symbol: Symbol) -> Result<ExchangeSymbolInfo>;
    async fn fetch_current_bid_price(symbol: &Symbol) -> Result<Decimal>;
    async fn fetch_snapshot(&self) -> Result<S>;
    async fn fetch_update_stream(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>>;
    async fn get_update_from_message(&self, message: Message) -> Result<U>;
    fn update_orderbook<T: UpdateState>(&self, update: T) -> Result<()>;
    async fn start(&self) -> Result<()> {
        let (tx_update, mut rx_update) = mpsc::channel::<OrderbookMessage<U>>(100);
        let ob_clone = self.orderbook().clone();
        let (tx_summary_clone, symbol, exchange) = {
            let ob = ob_clone.lock().unwrap();
            let txs = ob.tx_summary();
            let exchange = ob.exchange_symbol_info.exchange;
            let symbol = ob.symbol();
            (txs, symbol, exchange)
        };

        let mut stream = self.fetch_update_stream().await?;

        tracing::info!("hello");
        // fetcher - gets updates from the exchange and sends them to the updater
        let fetcher: tokio::task::JoinHandle<std::result::Result<(), anyhow::Error>> =
            tokio::spawn(async move {
                loop {
                    if let Some(response) = stream.next().await {
                        match response {
                            Ok(message) => match U::try_from(message) {
                                Ok(update) => {
                                    tx_update
                                        .send(OrderbookMessage::Update(update))
                                        .await
                                        .context("failed to send update")?;
                                }
                                Err(err) => {
                                    tracing::error!("failed to get update from message: {}", err);
                                }
                            },
                            Err(err) => {
                                tracing::error!("failed to get message: {}", err);
                                continue;
                            }
                        }
                    } else {
                        tracing::error!("failed to get message");
                    }
                }
            });

        tracing::info!("hello");

        let mut tx_count = 1;

        let updater = tokio::spawn(async move {
            while let Some(message) = rx_update.recv().await {
                match message {
                    OrderbookMessage::Update(update) => {
                        tracing::info!("update last_update_id: {}", update.last_update_id());
                        let mut ob = ob_clone.lock().unwrap();
                        let last_update_id = update.last_update_id();

                        if let Err(err) = ob.update(update) {
                            tracing::error!("failed to update orderbook: {}", err);
                        }
                        let _ = tx_summary_clone.lock().unwrap().send_replace(
                            OrderbookMessage::Summary(format!(
                                "received a summary for {} from {} with last_updated_id {}",
                                symbol,
                                exchange.as_str_name(),
                                last_update_id
                            )),
                        );

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
        tracing::info!("hello");
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
    pub fn storage_price(&self, display_price: Decimal) -> Result<u32> {
        display_to_storage_price(display_price, self.scale_price)
    }

    pub fn storage_price_min(&self) -> StoragePrice {
        let info = self.symbol.info();
        self.storage_price(info.display_price_min).unwrap()
    }

    pub fn storage_price_max(&self) -> StoragePrice {
        let info = self.symbol.info();
        self.storage_price(info.display_price_max).unwrap()
    }

    pub fn display_price_min(&self) -> Decimal {
        self.symbol.info().display_price_min
    }

    pub fn display_price_max(&self) -> Decimal {
        self.symbol.info().display_price_max
    }

    pub fn storage_quantity(&self, display_quantity: Decimal) -> Result<u64> {
        display_to_storage_quantity(display_quantity, self.scale_quantity)
    }

    pub fn display_price(&self, storage_price: StoragePrice) -> Decimal {
        let mut decimal = Decimal::from_u32(storage_price).unwrap();
        decimal.set_scale(self.scale_price).unwrap();
        decimal
    }

    pub fn get_display_quantity(&self, storage_quantity: StorageQuantity) -> Decimal {
        let mut decimal = Decimal::from_u64(storage_quantity).unwrap();
        decimal.set_scale(self.scale_quantity).unwrap();
        decimal
    }
}

/// Updates from all exchanges should implement this trait
pub trait UpdateState {
    fn first_update_id(&self) -> u64;
    fn last_update_id(&self) -> u64;
    fn bids_mut(self) -> Vec<[Decimal; 2]>;
    fn asks_mut(self) -> Vec<[Decimal; 2]>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_numbers_correctly() {
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

        let test_num = Decimal::from_i128_with_scale(u64::MAX as i128, 2);
        println!("test_num: {}", test_num);
        let storage_quantity = exchange_symbol_info.storage_quantity(test_num).unwrap();

        assert_eq!(storage_quantity, u64::MAX);

        if let Err(err) = exchange_symbol_info.storage_price(Decimal::new(u32::MAX as i64 + 1, 2)) {
            assert_eq!(err.to_string(), "price is too large");
        } else {
            panic!("greater than u32 should have failed");
        };

        if let Err(err) = exchange_symbol_info
            .storage_quantity(Decimal::from_i128_with_scale(u64::MAX as i128 + 1, 8))
        {
            assert_eq!(err.to_string(), "quantity is too large");
        } else {
            panic!("greater than u64 should have failed");
        };

        if let Err(err) = exchange_symbol_info.storage_price(Decimal::new(-1, 0)) {
            assert_eq!(err.to_string(), "price sign must be positive");
        } else {
            panic!("negative should have failed");
        };

        if let Err(err) = exchange_symbol_info.storage_quantity(Decimal::new(-1, 0)) {
            assert_eq!(err.to_string(), "quantity sign must be positive");
        } else {
            panic!("negative should have failed");
        };
    }

    #[test]
    fn it_converts_extreme_numbers_correctly() {
        let symbol_info = SymbolInfo {
            display_price_min: Decimal::new(1, 8),
            display_price_max: Decimal::new(42, 8),
        };
        let symbol = Symbol::BTCUSDT { info: symbol_info };

        let exchange_symbol_info: ExchangeSymbolInfo = ExchangeSymbolInfo {
            exchange: ExchangeType::default(),
            symbol,
            scale_price: 8,
            scale_quantity: 8,
        };

        assert_eq!(
            exchange_symbol_info
                .storage_price(exchange_symbol_info.display_price_min())
                .unwrap(),
            1
        );

        assert_eq!(
            exchange_symbol_info.display_price_min().to_string(),
            "0.00000001"
        );

        assert_eq!(
            exchange_symbol_info
                .storage_price(exchange_symbol_info.display_price_max())
                .unwrap(),
            42
        );

        assert_eq!(
            exchange_symbol_info
                .storage_quantity(exchange_symbol_info.display_price_min())
                .unwrap(),
            1
        );
        assert_eq!(
            exchange_symbol_info.get_display_quantity(1).to_string(),
            "0.00000001"
        );
    }
}
