use std::{
    ops::{Add, Div, Mul},
    str::FromStr,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::{
    booksummary::ExchangeType,
    orderbook::{ExchangeOrderbookMethods, ExchangeOrderbookProps, Orderbook},
    symbol::{ExchangeSymbolInfo, Symbol, SymbolInfo},
    update::Update,
};

// make sure these have trailing slashes
const BASE_URL_HTTPS: &str = "https://www.binance.us/api/v3/";
const BASE_URL_WSS: &str = "wss://stream.binance.us:9443/ws/";

pub struct BinanceOrderbook {
    pub orderbook: Box<Orderbook>,
    pub base_url_https: Url,
    pub base_url_wss: Url,
}

impl BinanceOrderbook {
    pub fn new(exchange_symbol_info: ExchangeSymbolInfo) -> Result<Self> {
        let orderbook = Self::create_orderbook(exchange_symbol_info)?;
        Ok(Self {
            orderbook: Box::new(orderbook),
            base_url_https: Url::parse(BASE_URL_HTTPS).unwrap(),
            base_url_wss: Url::parse(BASE_URL_WSS).unwrap(),
        })
    }
}

impl ExchangeOrderbookProps for BinanceOrderbook {
    fn get_orderbook(&self) -> &Orderbook {
        &self.orderbook
    }
    fn get_base_url_https(&self) -> &Url {
        &self.base_url_https
    }
    fn get_base_url_wss(&self) -> &Url {
        &self.base_url_wss
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceBestPrice {
    pub symbol: Symbol,
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceExchangeInfo {
    pub symbols: Vec<BinanceSymbol>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbol {
    pub symbol: Symbol,
    pub base_asset_precision: u32,
    pub quote_asset_precision: u32,
    pub filters: Vec<Value>,
}

#[async_trait]
impl ExchangeOrderbookMethods for BinanceOrderbook {
    async fn fetch_symbol_info(&self, symbol: Symbol, price_range: i64) -> Result<Symbol> {
        let price_range_decimal = Decimal::new(price_range, 2);
        let current_bid_price = self.fetch_current_bid_price(&symbol).await?;
        let display_price_max = current_bid_price.mul(price_range_decimal);
        let display_price_min = current_bid_price.div(price_range_decimal.add(Decimal::new(1, 0)));

        symbol.set_info(SymbolInfo {
            display_price_min,
            display_price_max,
        });
        Ok(symbol)
    }

    async fn fetch_exchange_symbol_info(&self, symbol: Symbol) -> Result<ExchangeSymbolInfo> {
        let mut url = self.base_url_https.join("exchangeInfo").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();

        let binance_exchange_info = reqwest::get(url)
            .await
            .with_context(|| {
                tracing::error!("Failed to get binance exchange info for {symbol}");
                "Failed to get binance exchange info".to_string()
            })?
            .json::<BinanceExchangeInfo>()
            .await
            .with_context(|| {
                tracing::error!("Failed to deserialize binance exchange info for {symbol}");
                "Failed to deserialize binance exchange info".to_string()
            })?;

        let scale_quantity = binance_exchange_info.symbols[0].base_asset_precision.min(8);
        let tick_sizes = binance_exchange_info.symbols[0]
            .filters
            .iter()
            .filter_map(|filter| {
                let filter_obj = filter.as_object()?;
                if let Some(filter_type) = filter["filterType"].as_str() {
                    if filter_type == "PRICE_FILTER" {
                        filter_obj["tickSize"].as_str()
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<&str>>();

        let tick_size_str = tick_sizes.first().context("Failed to get tick size")?;

        let scale_price = Decimal::from_str(tick_size_str)
            .context("Failed to parse tick size")?
            .normalize()
            .scale();

        let exchange_symbol_info = ExchangeSymbolInfo {
            exchange: ExchangeType::Binance,
            symbol,
            scale_price,
            scale_quantity,
        };
        Ok(exchange_symbol_info)
    }

    async fn fetch_current_bid_price(&self, symbol: &Symbol) -> Result<Decimal> {
        let mut url = self.base_url_https.join("ticker/bookTicker").unwrap();
        url.query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();

        let price = reqwest::get(url)
            .await
            .context("Failed to get price")?
            .json::<BinanceBestPrice>()
            .await
            .context("Failed to deserialize binance best price for {symbol} to json")?;
        Ok(price.bid_price)
    }

    async fn fetch_snapshot(&self) -> Result<Update> {
        unimplemented!()
    }

    async fn fetch_update_stream(
        &self,
    ) -> Result<Result<WebSocketStream<MaybeTlsStream<TcpStream>>>> {
        unimplemented!()
    }

    async fn update_orderbook(&self, update: Update) -> Result<()> {
        unimplemented!()
    }
}
