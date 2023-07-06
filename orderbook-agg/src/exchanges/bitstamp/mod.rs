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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tokio::fs::read;

    use crate::{
        core::{
            numtypes::{ToDisplay, ToStorage},
            orderbook::{Orderbook, OrderbookArgs},
        },
        exchanges::bitstamp::data::Snapshot,
        Exchange, Symbol,
    };

    fn orderbook_setup(
        storage_price_min: u64,
        storage_price_max: u64,
        scale_price: u32,
        scale_quantity: u32,
    ) -> Orderbook {
        let subscriber = tracing_subscriber::fmt()
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        let exchange = Exchange::BITSTAMP;
        let symbol = Symbol::BTCUSDT;

        let orderbook = Orderbook::new(
            exchange,
            symbol,
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        );

        assert_eq!(
            orderbook.bids.len() as u64,
            storage_price_max - storage_price_min + 1
        );

        assert_eq!(
            storage_price_min.to_display(scale_price).unwrap(),
            Decimal::new(storage_price_min as i64, scale_price)
        );

        assert!(orderbook.bids.iter().all(|x| x == &0));
        assert!(orderbook.asks.iter().all(|x| x == &0));

        orderbook
    }

    #[tokio::test]
    async fn it_updates_snapshot_correctly() {
        let scale_price = 0;
        let scale_quantity = 8;

        let snapshot_bytes = read("../tests/fixtures/snapshot_bitstamp.json").await;
        let mut snapshot: Snapshot = serde_json::from_slice(&snapshot_bytes.unwrap()).unwrap();

        let best_bid = snapshot
            .bids
            .iter()
            .max_by(|a, b| a[0].partial_cmp(&b[0]).unwrap())
            .unwrap()
            .clone();

        let best_ask = snapshot
            .asks
            .iter()
            .max_by(|a, b| b[0].partial_cmp(&a[0]).unwrap())
            .unwrap()
            .clone();

        let spread = best_ask[0] - best_bid[0];
        let bids_count = snapshot.bids.len();
        let asks_count = snapshot.asks.len();

        assert!(spread.is_sign_positive());
        assert_eq!(bids_count, 314);
        assert_eq!(asks_count, 160);
        assert_eq!(best_bid[0], Decimal::new(30766, 0));
        assert_eq!(best_ask[0], Decimal::new(30769, 0));

        let price_range = 5;
        let (storage_price_min, storage_price_max) =
            OrderbookArgs::get_min_max(best_bid[0], price_range, 0).unwrap();

        assert_eq!(storage_price_min, 29301);
        assert_eq!(storage_price_max, 32304);

        let mut orderbook = orderbook_setup(
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
        );

        // check count of bids and ask are the same in snapshot and order book
        let bid_count_in_range = snapshot.bids.iter().fold(0u64, |acc, x| {
            if x[1] > Decimal::new(0, scale_price)
                && x[0] >= Decimal::new(storage_price_min as i64, scale_price)
                && x[0] <= Decimal::new(storage_price_max as i64, scale_price)
            {
                acc + 1
            } else {
                return acc;
            }
        });

        let ask_count_in_range = snapshot.asks.iter().fold(0u64, |acc, x| {
            if x[1] > Decimal::new(0, 0)
                && x[0] >= Decimal::new(storage_price_min as i64, scale_price)
                && x[0] <= Decimal::new(storage_price_max as i64, scale_price)
            {
                acc + 1
            } else {
                return acc;
            }
        });

        orderbook.update(&mut snapshot).unwrap();

        let ob_bids_count = orderbook.bids.iter().fold(0u64, |acc, x| {
            if x > &0 {
                acc + 1
            } else {
                return acc;
            }
        });

        let ob_asks_count = orderbook.asks.iter().fold(0u64, |acc, x| {
            if x > &0 {
                acc + 1
            } else {
                return acc;
            }
        });

        assert_eq!(
            bid_count_in_range as u64, ob_bids_count as u64,
            "bid count in snapshot not equal to bid count in updated orderbookd"
        );
        assert_eq!(
            ask_count_in_range as u64, ob_asks_count as u64,
            "ask count in snapshot not equal to bid count in updated orderbookd"
        );

        assert_eq!(
            best_bid[0],
            orderbook.storage_bid_max.to_display(scale_price).unwrap(),
            "best bid in orderbook and snapshot are not equal"
        );
        assert_eq!(
            best_ask[0],
            orderbook.storage_ask_min.to_display(scale_price).unwrap(),
            "best bid in orderbook and snapshot are not equal"
        );

        // quantities are the same in both snapshot and orderbook
        for bid in snapshot.bids.iter() {
            let quantity = bid[1];
            if bid[1] > 0.to_display(scale_quantity).unwrap()
                && bid[0] >= Decimal::new(storage_price_min as i64, scale_price)
                && bid[0] <= Decimal::new(storage_price_max as i64, scale_price)
            {
                let storage_price = bid[0].to_storage(scale_price).unwrap();
                let idx = orderbook.idx(storage_price);
                assert_eq!(
                    quantity,
                    orderbook.bids[idx].to_display(scale_quantity).unwrap(),
                    "snapshot bid quantity not equal to orderbook bid quantity"
                );
            }
        }

        for ask in snapshot.asks.iter() {
            let quantity = ask[1];
            if ask[1] > 0.to_display(scale_quantity).unwrap()
                && ask[0] >= Decimal::new(storage_price_min as i64, scale_price)
                && ask[0] <= Decimal::new(storage_price_max as i64, scale_price)
            {
                let storage_price = ask[0].to_storage(scale_price).unwrap();
                let idx = orderbook.idx(storage_price);
                assert_eq!(
                    quantity,
                    orderbook.asks[idx].to_display(scale_quantity).unwrap(),
                    "snapshot ask quantity not equal to orderbook ask quantity"
                );
            }
        }
    }
}
