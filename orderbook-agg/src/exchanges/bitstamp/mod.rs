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
use data::{BestPrice, BookUpdate, Snapshot};
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use self::data::ExchangeInfoBitstamp;

pub mod data;

pub struct BitstampOrderbook {
    pub orderbook: Arc<Mutex<Orderbook>>,
}

#[async_trait]
impl ExchangeOrderbook<Snapshot, BookUpdate> for BitstampOrderbook {
    // make sure these have trailing slashes
    const BASE_URL_HTTPS: &'static str = "https://www.bitstamp.net/api/v2/";
    const BASE_URL_WSS: &'static str = "wss://ws.bitstamp.net/";

    async fn new(symbol: Symbol, price_range: u8) -> Result<Self>
    where
        Self: Sized,
    {
        let exchange = Exchange::BITSTAMP;
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
            ExchangeInfoBitstamp::fetch_scales(Self::base_url_https(), symbol).await?;
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
        let url = Self::base_url_https()
            .join(format!("ticker/{}", symbol.to_string().to_lowercase()).as_str())?;
        let price = BestPrice::fetch(url).await?;
        Ok((price.bid, price.ask))
    }

    async fn fetch_snapshot(&self) -> Result<Snapshot> {
        let symbol = self
            .orderbook()
            .lock()
            .await
            .symbol
            .to_string()
            .to_lowercase();
        let url = Self::base_url_https().join(format!("order_book/{}", symbol).as_str())?;
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

        let subscribe_msg = serde_json::json!({
            "event": "bts:subscribe",
            "data": {
                "channel": format!("diff_order_book_{}", symbol)
            }
        });

        let (mut stream, _) = connect_async(&Self::base_url_wss())
            .await
            .context("Failed to connect to bit stamp wss endpoint")?;

        stream
            .start_send_unpin(Message::Text(subscribe_msg.to_string()))
            .context("Failed to send subscribe message to bitstamp")?;

        Ok(stream)
    }
}
