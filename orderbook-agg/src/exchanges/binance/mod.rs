use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use std::{
    ops::{Add, Div, Mul},
    sync::{Arc, Mutex},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use self::data::Update;

use super::{
    ExchangeOrderbookMethods, ExchangeOrderbookState, ExchangeSymbolInfo, Orderbook, Symbol,
    SymbolInfo, UpdateState,
};
use crate::booksummary::ExchangeType;
use data::{BestPrice, ExchangeInfo, Snapshot};

pub mod data;

pub struct BinanceOrderbook<U: UpdateState + 'static> {
    pub orderbook: Arc<Mutex<Orderbook<U>>>,
}

impl<U: UpdateState + 'static> BinanceOrderbook<U> {
    pub fn new(exchange_symbol_info: ExchangeSymbolInfo) -> Self {
        let orderbook = Arc::new(Mutex::new(Orderbook::<U>::new(exchange_symbol_info)));
        Self { orderbook }
    }
}

impl<U: UpdateState> ExchangeOrderbookState<U> for BinanceOrderbook<U> {
    // make sure these have trailing slashes
    const BASE_URL_HTTPS: &'static str = "https://www.binance.us/api/v3/";
    const BASE_URL_WSS: &'static str = "wss://stream.binance.us:9443/ws/";

    fn orderbook(&self) -> Arc<Mutex<Orderbook<U>>> {
        self.orderbook.clone()
    }
}

#[async_trait]
impl ExchangeOrderbookMethods<Snapshot, Update> for BinanceOrderbook<Update> {
    async fn fetch_symbol_info(symbol: Symbol, price_range: i64) -> Result<Symbol> {
        let price_range_decimal = Decimal::from_i64(price_range)
            .context("failed to create decimal")?
            .div(Decimal::new(100, 0))
            .add(Decimal::new(1, 0));
        let current_bid_price = Self::fetch_current_bid_price(&symbol).await?;
        let display_price_max = current_bid_price.mul(price_range_decimal);
        let display_price_min = current_bid_price.div(price_range_decimal);

        let symbol = symbol.set_info(SymbolInfo {
            display_price_min,
            display_price_max,
        });
        Ok(symbol)
    }

    async fn fetch_exchange_symbol_info(symbol: Symbol) -> Result<ExchangeSymbolInfo> {
        let mut url = Self::base_url_https().join("exchangeInfo").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();

        let symbol_info = symbol.info();
        let binance_exchange_info = ExchangeInfo::fetch(url).await?;
        let scale_price = binance_exchange_info.scale_price()?;
        let scale_quantity = binance_exchange_info.scale_quantity()?;

        let display_price_min = symbol_info.display_price_min.round_dp(scale_price);
        let display_price_max = symbol_info.display_price_max.round_dp(scale_price);

        let symbol = symbol.set_info(SymbolInfo {
            display_price_min,
            display_price_max,
        });

        let exchange_symbol_info = ExchangeSymbolInfo {
            exchange: ExchangeType::Binance,
            symbol,
            scale_price,
            scale_quantity,
        };
        Ok(exchange_symbol_info)
    }

    async fn fetch_current_bid_price(symbol: &Symbol) -> Result<Decimal> {
        let mut url = Self::base_url_https().join("ticker/bookTicker").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();
        let price = BestPrice::fetch(url).await?;
        Ok(price.bid_price)
    }

    async fn fetch_snapshot(&self) -> Result<Snapshot> {
        let symbol = self.orderbook().lock().unwrap().symbol();
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
            .symbol()
            .to_string()
            .to_lowercase();
        let endpoint = format!("{}@depth@100ms", symbol);
        let url = Self::base_url_wss().join(&endpoint).unwrap();
        let (stream, _) = connect_async(url)
            .await
            .context("Failed to connect to binance wss endpoint")?;
        Ok(stream)
    }

    async fn get_update_from_message(&self, message: Message) -> Result<Update> {
        Update::try_from(message)
    }

    fn update_orderbook<U: UpdateState>(&self, update: U) -> Result<()> {
        unimplemented!()
    }
}
