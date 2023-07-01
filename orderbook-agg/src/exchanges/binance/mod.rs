use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use std::ops::{Add, Div, Mul};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing_subscriber::field::display;

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
        let price_range_decimal = Decimal::from_i64(price_range)
            .context("failed to create decimal")?
            .div(Decimal::new(100, 0))
            .add(Decimal::new(1, 0));
        let current_bid_price = Self::fetch_current_bid_price(&symbol).await?;
        let display_price_max = current_bid_price.mul(price_range_decimal);
        let display_price_min = current_bid_price.div(price_range_decimal);
        tracing::info!(
            "current_bid_price: {}, display_price_min: {}, display_price_max: {}",
            current_bid_price,
            display_price_min,
            display_price_max
        );

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
        tracing::info!("symbol: {:?}", symbol,);
        tracing::info!(
            "scale_price: {}, scale_quantity: {}",
            scale_price,
            scale_quantity
        );
        let display_price_min = symbol_info.display_price_min.round_dp(scale_price);
        let display_price_max = symbol_info.display_price_max.round_dp(scale_price);
        tracing::info!(
            "display_price_min: {}, display_price_max: {}",
            display_price_min,
            display_price_max
        );

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
