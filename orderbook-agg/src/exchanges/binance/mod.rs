use std::sync::Arc;

use crate::{
    core::{
        exchangebook::ExchangeOrderbook,
        numtypes::DisplayAmount,
        orderbook::{Orderbook, OrderbookArgs},
    },
    Exchange, Symbol,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use self::data::{BestPrice, BookUpdate, ExchangeInfoBinance, Snapshot};

pub mod data;

pub struct BinanceOrderbook {
    pub orderbook: Arc<Mutex<Orderbook>>,
}

#[async_trait]
impl ExchangeOrderbook<Snapshot, BookUpdate> for BinanceOrderbook {
    // make sure these have trailing slashes
    const BASE_URL_HTTPS: &'static str = "https://www.binance.us/api/v3/";
    const BASE_URL_WSS: &'static str = "wss://stream.binance.us:9443/ws/";

    async fn new(symbol: Symbol, price_range: u8) -> Result<Self>
    where
        Self: Sized,
    {
        let exchange = Exchange::BINANCE;
        let orderbook = Self::new_orderbook(exchange, symbol, price_range).await?;
        let exchange_orderbook = Self {
            orderbook: Arc::new(Mutex::new(orderbook)),
        };
        Ok(exchange_orderbook)
    }

    fn orderbook(&self) -> Arc<Mutex<Orderbook>> {
        self.orderbook.clone()
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

    async fn fetch_prices(symbol: &Symbol) -> Result<(DisplayAmount, DisplayAmount)> {
        let mut url = Self::base_url_https().join("ticker/bookTicker").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();
        let price = BestPrice::fetch(url).await?;
        Ok((price.bid_price, price.ask_price))
    }

    async fn fetch_snapshot(&self) -> Result<Snapshot> {
        let symbol = self.orderbook().lock().await.symbol;
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
            .await
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
