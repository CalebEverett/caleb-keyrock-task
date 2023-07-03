use crate::{
    core::{
        exchange_book::ExchangeOrderbook,
        num_types::DisplayPrice,
        orderbook::{BookLevels, Orderbook, OrderbookArgs, OrderbookMessage},
    },
    Exchange, Symbol,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use data::{BestPrice, BookUpdate, Snapshot};
use std::sync::{Arc, Mutex};
use tokio::{net::TcpStream, sync::watch};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use self::data::ExchangeInfoBinance;

pub mod data;

pub struct BinanceOrderbook<U> {
    pub orderbook: Arc<Mutex<Orderbook>>,
    pub tx_summary: Arc<Mutex<watch::Sender<OrderbookMessage<U>>>>,
}

#[async_trait]
impl ExchangeOrderbook<Snapshot, BookUpdate> for BinanceOrderbook<BookUpdate> {
    // make sure these have trailing slashes
    const BASE_URL_HTTPS: &'static str = "https://www.binance.us/api/v3/";
    const BASE_URL_WSS: &'static str = "wss://stream.binance.us:9443/ws/";

    async fn new(exchange: Exchange, symbol: Symbol, price_range: u8) -> Result<Self>
    where
        Self: Sized,
    {
        let orderbook = Self::new_orderbook(exchange, symbol, price_range).await?;
        let (tx_summary, _) = watch::channel(OrderbookMessage::BookLevels(BookLevels::default()));
        let exchange_orderbook = Self {
            orderbook: Arc::new(Mutex::new(orderbook)),
            tx_summary: Arc::new(Mutex::new(tx_summary)),
        };
        Ok(exchange_orderbook)
    }

    fn orderbook(&self) -> Arc<Mutex<Orderbook>> {
        self.orderbook.clone()
    }
    fn tx_summary(&self) -> Arc<Mutex<watch::Sender<OrderbookMessage<BookUpdate>>>> {
        self.tx_summary.clone()
    }

    fn rx_summary(&self) -> watch::Receiver<OrderbookMessage<BookUpdate>> {
        self.tx_summary.clone().lock().unwrap().subscribe()
    }

    async fn fetch_orderbook_args(symbol: &Symbol, price_range: u8) -> Result<OrderbookArgs> {
        let (best_price, _) = Self::fetch_prices(symbol).await?;

        println!("base_url_https: {}", Self::base_url_https());
        let (scale_price, scale_quantity) =
            ExchangeInfoBinance::fetch_scales(Self::base_url_https(), symbol).await?;
        let (storage_price_min, storage_price_max) =
            OrderbookArgs::get_min_max(best_price, price_range, scale_price)?;

        let args = OrderbookArgs {
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        };

        tracing::debug!("orderbook args: {:#?}", args);

        Ok(args)
    }

    async fn fetch_prices(symbol: &Symbol) -> Result<(DisplayPrice, DisplayPrice)> {
        let mut url = Self::base_url_https().join("ticker/bookTicker").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();
        let price = BestPrice::fetch(url).await?;
        Ok((price.bid_price, price.ask_price))
    }

    async fn fetch_snapshot(&self) -> Result<Snapshot> {
        let symbol = self.orderbook().lock().unwrap().symbol;
        let mut url = Self::base_url_https().join("depth").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .append_pair("limit", "1000")
            .finish();
        Snapshot::fetch(url).await
    }

    async fn fetch_update_stream(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let symbol = self
            .orderbook()
            .lock()
            .unwrap()
            .symbol
            .to_string()
            .to_lowercase();
        let endpoint = format!("{}@depth@100ms", symbol);
        let url = Self::base_url_wss().join(&endpoint).unwrap();
        let (stream, _) = connect_async(url)
            .await
            .context("Failed to connect to wss endpoint")?;
        Ok(stream)
    }
}
