use super::{ExchangeOrderbook, Orderbook, Symbol};
use crate::booksummary::Exchange;
use crate::exchanges::Update;
use anyhow::{Context, Result};
use async_trait::async_trait;
use data::{BestPrice, BookUpdate, ExchangeInfo, Snapshot};
use futures::future::try_join;
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub mod data;

pub struct BinanceOrderbook<U> {
    pub orderbook: Arc<Mutex<Orderbook<U>>>,
}

#[async_trait]
impl ExchangeOrderbook<Snapshot, BookUpdate> for BinanceOrderbook<BookUpdate> {
    // make sure these have trailing slashes
    const BASE_URL_HTTPS: &'static str = "https://www.binance.us/api/v3/";
    const BASE_URL_WSS: &'static str = "wss://stream.binance.us:9443/ws/";

    fn orderbook(&self) -> Arc<Mutex<Orderbook<BookUpdate>>> {
        self.orderbook.clone()
    }

    async fn new(exchange: Exchange, symbol: Symbol, price_range: u8) -> Result<Self> {
        let mut url = Self::base_url_https().join("exchangeInfo").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();

        let (exchange_info, best_bid) = try_join(
            ExchangeInfo::fetch(url),
            Self::fetch_best_bid(exchange, &symbol),
        )
        .await?;

        let scale_price = exchange_info.scale_price()?;

        let (storage_price_min, storage_price_max) =
            Self::get_min_max(best_bid, price_range, scale_price);

        let scale_quantity = exchange_info.scale_quantity()?;

        let orderbook =
            Orderbook::<BookUpdate>::new(exchange, symbol, 0, 0, scale_price, scale_quantity);

        let exchange_order_book = BinanceOrderbook {
            orderbook: Arc::new(Mutex::new(orderbook)),
        };

        Ok(exchange_order_book)
    }

    async fn fetch_best_bid(exchange: Exchange, symbol: &Symbol) -> Result<Decimal> {
        let mut url = Self::base_url_https().join("ticker/bookTicker").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();
        let price = BestPrice::fetch(url).await?;
        Ok(price.bid_price)
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
