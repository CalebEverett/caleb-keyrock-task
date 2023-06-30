use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::{
    ops::{Add, Div, Mul},
    sync::{Arc, Mutex},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::{
    ExchangeOrderbookMethods, ExchangeOrderbookProps, ExchangeSymbolInfo, Orderbook, Symbol,
    SymbolInfo, UpdateState,
};
use crate::booksummary::ExchangeType;
use data::{BestPrice, ExchangeInfo, Snapshot};

pub mod data;

pub struct BinanceOrderbook {
    pub orderbook: Orderbook,
}

impl BinanceOrderbook {
    pub fn new(exchange_symbol_info: ExchangeSymbolInfo) -> Result<Self> {
        let orderbook = Self::create_orderbook(exchange_symbol_info)?;
        Ok(Self { orderbook })
    }
}

impl ExchangeOrderbookProps for BinanceOrderbook {
    // make sure these have trailing slashes
    const BASE_URL_HTTPS: &'static str = "https://www.binance.us/api/v3/";
    const BASE_URL_WSS: &'static str = "wss://stream.binance.us:9443/ws/";

    fn orderbook(&self) -> &Orderbook {
        &self.orderbook
    }
}

#[async_trait]
impl ExchangeOrderbookMethods<Snapshot> for BinanceOrderbook {
    async fn fetch_symbol_info(symbol: Symbol, price_range: i64) -> Result<Symbol> {
        let price_range_decimal = Decimal::new(price_range, 2);
        let current_bid_price = Self::fetch_current_bid_price(&symbol).await?;
        let display_price_max = current_bid_price.mul(price_range_decimal);
        let display_price_min = current_bid_price.div(price_range_decimal.add(Decimal::new(1, 0)));

        symbol.set_info(SymbolInfo {
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

        let binance_exchange_info = ExchangeInfo::fetch(url).await?;
        let scale_price = binance_exchange_info.scale_price()?;
        let scale_quantity = binance_exchange_info.scale_quantity()?;

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
        let symbol = self.orderbook.symbol();
        let mut url = Self::base_url_https().join("depth").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .append_pair("limit", "1000")
            .finish();
        Snapshot::fetch(url).await
    }

    async fn fetch_update_stream(
        &self,
    ) -> Result<Result<WebSocketStream<MaybeTlsStream<TcpStream>>>> {
        unimplemented!()
    }

    fn update_orderbook<U: UpdateState>(&self, update: U) -> Result<()> {
        Ok(())
    }
}
