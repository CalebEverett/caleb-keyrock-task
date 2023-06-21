use futures::{future::try_join_all, SinkExt};
use orderbook::{ExchangeType, Level, Summary, Symbols};
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    ops::{Mul, Sub},
};
use strum::IntoEnumIterator;
use tokio::net::TcpStream;
use tokio_stream::StreamMap;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tonic::Status;
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

const BASE_WS_BINANCE: &str = "wss://stream.binance.us:9443";
const BASE_WS_BISTAMP: &str = "wss://ws.bitstamp.net";

/// Gets a websocket stream for each exchange and returns a map of them.
pub async fn get_stream(
    symbol: String,
) -> Result<
    StreamMap<ExchangeType, WebSocketStream<MaybeTlsStream<TcpStream>>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut map = StreamMap::new();
    let symbol = symbol.to_lowercase();

    for exchange in ExchangeType::iter() {
        match exchange {
            ExchangeType::Binance => {
                let ws_url_binance = url::Url::parse(BASE_WS_BINANCE)
                    .expect("bad binance url")
                    .join(&format!("ws/{}@depth", symbol))
                    .unwrap();

                let (ws_stream_binance, _) = connect_async(&ws_url_binance)
                    .await
                    .expect(format!("Failed to connect to {}", &ws_url_binance.as_str()).as_str());

                map.insert(ExchangeType::Binance, ws_stream_binance);
            }
            ExchangeType::Bitstamp => {
                let ws_url_bitstamp = url::Url::parse(BASE_WS_BISTAMP).expect("bad bitstamp url");
                let subscribe_msg = serde_json::json!({
                    "event": "bts:subscribe",
                    "data": {
                        "channel": format!("diff_order_book_{}", symbol)
                    }
                });

                let (mut ws_stream_bitstamp, _) = connect_async(&ws_url_bitstamp)
                    .await
                    .expect(format!("Failed to connect to {}", &ws_url_bitstamp.as_str()).as_str());

                ws_stream_bitstamp
                    .start_send_unpin(Message::Text(subscribe_msg.to_string()))
                    .expect("Failed to send subscribe message to bitstamp");

                map.insert(ExchangeType::Bitstamp, ws_stream_bitstamp);
            }
        }
    }
    Ok(map)
}

/// Gets available symbosl from binanc.us.
pub async fn get_symbols_binance(
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let symbols = reqwest::get("https://api.binance.us/api/v3/exchangeInfo")
        .await?
        .json::<serde_json::Value>()
        .await?["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|symbol| symbol["symbol"].as_str().unwrap().to_string())
        .collect::<HashSet<String>>();
    Ok(symbols)
}

/// Gets available symbols bitstamp.
pub async fn get_symbols_bitstamp(
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let symbols = reqwest::get("https://www.bitstamp.net/api/v2/ticker/")
        .await?
        .json::<serde_json::Value>()
        .await?
        .as_array()
        .unwrap()
        .iter()
        .map(|symbol| symbol["pair"].as_str().unwrap().replace("/", ""))
        .collect::<HashSet<String>>();
    Ok(symbols)
}

/// Gets available symbols for a given exchange.
pub async fn get_symbols(
    exchange: ExchangeType,
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    match exchange {
        ExchangeType::Binance => get_symbols_binance().await,
        ExchangeType::Bitstamp => get_symbols_bitstamp().await,
    }
}

/// Gets symbols available on all exchanges.
pub async fn get_symbols_all() -> Result<Symbols, Box<dyn std::error::Error + Send + Sync>> {
    let symbols_vec = try_join_all(
        ExchangeType::iter()
            .map(|exchange| get_symbols(exchange))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("couldn't get symbols");

    let exchange_names: Vec<&str> = ExchangeType::iter().map(|e| e.as_str_name()).collect();
    let symbols_intersection: HashSet<String> =
        symbols_vec
            .into_iter()
            .enumerate()
            .fold(HashSet::new(), |acc, (i, set)| {
                tracing::info!(exchange = &exchange_names[i], symbols_count = set.len());
                if acc.is_empty() {
                    set
                } else {
                    acc.intersection(&set).map(|s| s.to_string()).collect()
                }
            });

    let mut symbols: Vec<String> = symbols_intersection.into_iter().collect();

    symbols.sort();

    tracing::info!(
        exchanges = format!("{}", exchange_names.join(", ")),
        symbols_count = symbols.len()
    );

    Ok(Symbols { symbols })
}

/// Validates a symbol is available on all exchanges.
pub async fn validate_symbol(symbol: &String) -> Result<(), Status> {
    let symbols = get_symbols_all().await.expect("Failed to get symbols");
    if !symbols.symbols.iter().any(|s| s == symbol) {
        tracing::error!("Symbol {} not found on one or more exchanges", symbol);
        return Err(Status::not_found("Symbol not found"));
    }
    Ok(())
}

/// Gets a orderbook snapshot with up to 1000 levels for a given exchange and symbol.
pub async fn get_snapshot(
    exchange: ExchangeType,
    symbol: &String,
) -> Result<(ExchangeType, Snapshot), Box<dyn std::error::Error + Send + Sync>> {
    let url = match exchange {
        ExchangeType::Binance => {
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit=1000",
                symbol
            )
        }
        ExchangeType::Bitstamp => format!(
            "https://www.bitstamp.net/api/v2/order_book/{}/",
            symbol.to_lowercase()
        ),
    };

    tracing::info!(
        "Getting snapshot for {} from {}",
        exchange.as_str_name(),
        url
    );

    let mut snapshot = reqwest::get(url)
        .await
        .expect(format!("Failed to get snapshot for {}", exchange.as_str_name()).as_str())
        .json::<Snapshot>()
        .await
        .expect(format!("Failed to parse json for {}", exchange.as_str_name()).as_str());

    if exchange == ExchangeType::Bitstamp {
        snapshot.bids = snapshot.bids[0..1000 as usize].to_vec();
        snapshot.asks = snapshot.asks[0..1000 as usize].to_vec();
    }

    Ok((exchange, snapshot))
}

/// Gets snapshots for all exchanges for a given symbol.
pub async fn get_snapshots(
    symbol: &String,
) -> Result<Vec<(ExchangeType, Snapshot)>, Box<dyn std::error::Error + Send + Sync>> {
    let snapshots_vec: Vec<(ExchangeType, Snapshot)> = try_join_all(
        ExchangeType::iter()
            .map(|exchange| get_snapshot(exchange, symbol))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("couldn't get snapshots");
    Ok(snapshots_vec)
}

/// Structure to hold a snapshot of an orderbook.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    #[serde(
        alias = "microtimestamp",
        deserialize_with = "deserialize_number_from_string"
    )]
    pub last_update_id: u64,
    #[serde(deserialize_with = "from_str")]
    pub bids: Vec<[f64; 2]>,
    #[serde(deserialize_with = "from_str")]
    pub asks: Vec<[f64; 2]>,
}

fn from_str<'de, D>(deserializer: D) -> Result<Vec<[f64; 2]>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<[&str; 2]> = Deserialize::deserialize(deserializer)?;
    Ok(v.iter()
        .map(|s| {
            [
                s[0].parse::<f64>().expect("Failed to parse"),
                s[1].parse::<f64>().expect("Failed to parse"),
            ]
        })
        .collect::<Vec<[f64; 2]>>())
}

pub type Price = u32;
pub type Amount = u64;

/// Holds levels for each price point. Entries will at most be equal
/// to the number of exchanges.
#[derive(Debug)]
pub struct PricePoint {
    pub bids: HashMap<ExchangeType, Amount>,
    pub asks: HashMap<ExchangeType, Amount>,
}

impl Default for PricePoint {
    fn default() -> Self {
        let exchange_count = ExchangeType::iter().len();

        Self {
            bids: HashMap::with_capacity(exchange_count),
            asks: HashMap::with_capacity(exchange_count),
        }
    }
}

/// Structure to hold update data.
#[derive(Debug)]
pub struct Updates {
    pub exchange: ExchangeType,
    pub last_update_id: u64,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}

/// Deserializes updates from binance.
pub fn updates_binance(
    value: serde_json::Value,
) -> Result<Updates, Box<dyn std::error::Error + Send + Sync>> {
    let last_update_id = value["E"].as_u64().unwrap();

    let bids = value["b"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            [
                b[0].as_str().unwrap().parse::<f64>().unwrap(),
                b[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    let asks = value["a"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            [
                a[0].as_str().unwrap().parse::<f64>().unwrap(),
                a[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    Ok(Updates {
        exchange: ExchangeType::Binance,
        last_update_id,
        bids,
        asks,
    })
}

/// Deserializes updates from bitstamp.
pub fn updates_bitstamp(
    value: &serde_json::Map<String, serde_json::Value>,
) -> Result<Updates, Box<dyn std::error::Error + Send + Sync>> {
    let last_update_id = value["microtimestamp"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let bids = value["bids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            [
                b[0].as_str().unwrap().parse::<f64>().unwrap(),
                b[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    let asks = value["asks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            [
                a[0].as_str().unwrap().parse::<f64>().unwrap(),
                a[1].as_str().unwrap().parse::<f64>().unwrap(),
            ]
        })
        .collect::<Vec<[f64; 2]>>();

    Ok(Updates {
        exchange: ExchangeType::Bitstamp,
        last_update_id,
        bids,
        asks,
    })
}

/// Orderbook structure.
#[derive(Debug, Default)]
pub struct Orderbook {
    pub ask_min: Price,
    pub bid_max: Price,
    pub symbol: String,
    pub price_points: Vec<PricePoint>,
    pub min_price: Price,
    pub max_price: Price,
    limit: u32,
    power_price: u32,
    power_amount: u32,
}

impl Orderbook {
    /// Initializes an already creaated orderbook to facilitate Arc<Mutex<Orderbook>>
    /// on the server.
    pub fn reset(
        &mut self,
        symbol: String,
        limit: u32,
        min_price: f64,
        max_price: f64,
        power_price: Price,
    ) {
        let min_price = min_price.mul(10u32.pow(power_price) as f64) as Price;
        let max_price = max_price.mul(10u32.pow(power_price) as f64) as Price;
        tracing::info!(
            "Resetting orderbook for {} with limit {} and price range {} - {}",
            symbol,
            limit,
            min_price,
            max_price
        );
        let mut pps: Vec<PricePoint> = Vec::with_capacity((max_price - min_price) as usize + 2);

        let mut idx = 0;
        while idx <= (max_price - min_price) as usize {
            pps.push(PricePoint::default());
            idx += 1;
        }

        self.symbol = symbol;
        self.ask_min = u32::MAX;
        self.bid_max = 0;
        self.price_points = pps;
        self.min_price = min_price;
        self.max_price = max_price;
        self.limit = limit;
        self.power_price = power_price;
        self.power_amount = 8;
    }

    /// Gets the index for a given price.
    fn get_idx(&self, price: Price) -> Price {
        price.sub(self.min_price)
    }

    /// Gets storage representation of price from its display price.
    fn get_price(&self, price: f64) -> Price {
        (price.mul(10u32.pow(self.power_price) as f64)) as Price
    }

    /// Gets storage representation of a quantity from its display quantity.
    fn get_amount(&self, amount: f64) -> Amount {
        (amount.mul(10u64.pow(self.power_amount) as f64)) as Amount
    }

    /// Retrieves a price point from storage.
    fn get_price_point(&self, price: Price) -> &PricePoint {
        let idx = self.get_idx(price) as usize;
        &self.price_points[idx]
    }

    /// Retrieves a mutable price point from storage.
    fn get_price_point_mut(&mut self, price: Price) -> &mut PricePoint {
        let idx = self.get_idx(price) as usize;
        &mut self.price_points[idx]
    }

    /// Adds, modifies or removes a bid from the order book.
    pub fn add_bid(
        &mut self,
        exchange: ExchangeType,
        level: [f64; 2],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut level_price = self.get_price(level[0]);
        if level_price > self.max_price || level_price < self.min_price {
            return Err("Price is out of range".into());
        }

        let level_amount = self.get_amount(level[1]);
        let bid_max = self.bid_max;
        let bids = &mut self.get_price_point_mut(level_price).bids;

        if level_amount > 0 {
            bids.insert(exchange, level_amount);
            if level_price > self.bid_max {
                self.bid_max = level_price;
            }
        } else {
            // level.amount == 0.
            bids.remove(&exchange);
            // if removed last bid at the highest price, find the next highest price
            if level_price == bid_max && bids.is_empty() {
                loop {
                    level_price -= 1;
                    if !self.get_price_point_mut(level_price).bids.is_empty() {
                        self.bid_max = level_price;
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Adds, modifies or removes an ask from the order book.
    pub fn add_ask(
        &mut self,
        exchange: ExchangeType,
        level: [f64; 2],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut level_price = self.get_price(level[0]);
        if level_price > self.max_price || level_price < self.min_price {
            return Err("Price is out of range".into());
        }

        let level_amount = self.get_amount(level[1]);
        let ask_min = self.ask_min;
        let asks = &mut self.get_price_point_mut(level_price).asks;

        if level_amount > 0 {
            asks.insert(exchange, level_amount);
            if level_price < self.ask_min {
                self.ask_min = level_price;
            }
        } else {
            // level.amount == 0.
            asks.remove(&exchange);
            // if removed last ask at the lowest price, find the next lowest price
            if level_price == ask_min && asks.is_empty() {
                loop {
                    level_price += 1;
                    if !self.get_price_point_mut(level_price).asks.is_empty() {
                        self.ask_min = level_price;
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes updates to the orderbook from an exchange.
    pub fn update(&mut self, updates: Updates) {
        updates.bids.into_iter().for_each(|b| {
            let price = b[0];
            let amount = b[1];
            let _ = self.add_bid(updates.exchange.clone(), [price, amount]);
        });
        updates.asks.into_iter().for_each(|a| {
            let price = a[0];
            let amount = a[1];
            let _ = self.add_ask(updates.exchange.clone(), [price, amount]);
        });
    }

    /// Processes an orderbook snapshot from an exchange into the orderbook.
    /// TODO: Try to parallelize this - the number of collisions is likely to be low.
    fn add_snapshot(&mut self, exchange: ExchangeType, snapshot: Snapshot) {
        snapshot.bids.into_iter().for_each(|b| {
            let price = b[0];
            let amount = b[1];
            let _ = self.add_bid(exchange, [price, amount]);
        });
        snapshot.asks.into_iter().for_each(|a| {
            let price = a[0];
            let amount = a[1];
            let _ = self.add_ask(exchange, [price, amount]);
        });
    }

    /// Adds snapshots from all exchanges for a given symbol.
    pub async fn add_snapshots(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshots = get_snapshots(&self.symbol).await?;

        snapshots.into_iter().for_each(|(exchange, snapshot)| {
            self.add_snapshot(exchange, snapshot);
        });
        Ok(())
    }

    /// Collect bids for the summary.
    fn get_summary_bids(&self) -> Vec<Level> {
        let mut summary_bids = Vec::<Level>::with_capacity(self.limit as usize);
        let mut counter = 0;
        let mut bid_max = self.bid_max;
        loop {
            let mut ob_bids: Vec<Level> = self
                .get_price_point(bid_max)
                .bids
                .iter()
                .map(|(k, v)| Level {
                    price: (bid_max as f64) / (10u64.pow(self.power_price) as f64),
                    amount: (v.clone() as f64) / (10u64.pow(self.power_amount) as f64),
                    exchange: k.clone() as i32,
                })
                .collect::<Vec<Level>>();

            ob_bids.sort_by(|a, b| b.amount.partial_cmp(&a.amount).unwrap());
            counter += ob_bids.len() as u32;
            summary_bids.append(&mut ob_bids);

            if counter >= self.limit || bid_max == self.min_price {
                break;
            }
            bid_max -= 1;
        }
        summary_bids
    }

    /// Collect asks for the summary.
    fn get_summary_asks(&self) -> Vec<Level> {
        let mut summary_asks = Vec::<Level>::with_capacity(self.limit as usize);
        let mut counter = 0;
        let mut ask_min = self.ask_min;
        loop {
            let mut ob_asks: Vec<Level> = self
                .get_price_point(ask_min)
                .asks
                .iter()
                .map(|(k, v)| Level {
                    price: (ask_min as f64) / (10u64.pow(self.power_price) as f64),
                    amount: (v.clone() as f64) / (10u64.pow(self.power_amount) as f64),
                    exchange: k.clone() as i32,
                })
                .collect::<Vec<Level>>();

            ob_asks.sort_by(|a, b| a.amount.partial_cmp(&b.amount).unwrap());
            counter += ob_asks.len() as u32;
            summary_asks.append(&mut ob_asks);

            if counter >= self.limit || ask_min == self.max_price {
                break;
            }
            ask_min += 1;
        }
        summary_asks
    }

    /// Create the summary.
    /// TODO: Try to parallelize this - the orderbook is only being read.
    pub fn get_summary(&self) -> Summary {
        let summary_asks = self.get_summary_asks();
        let summary_bids = self.get_summary_bids();

        Summary {
            spread: summary_asks[0].price - summary_bids[0].price,
            bids: summary_bids,
            asks: summary_asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory_stats::memory_stats;

    fn print_memory_usage() {
        if let Some(usage) = memory_stats() {
            println!("Current physical memory usage: {}", usage.physical_mem);
            println!("Current virtual memory usage: {}", usage.virtual_mem);
        } else {
            println!("Couldn't get the current memory usage :(");
        }
    }

    #[test]
    fn it_creates_an_orderbook() {
        let symbol = "BTCUSD".to_string();
        let mut ob = Orderbook::default();
        ob.reset(symbol, 10, 25_000.0, 28_000.0, 2);
        print_memory_usage();
    }

    #[test]
    fn adds_a_bid() {
        let symbol = "BTCUSD".to_string();
        let mut ob = Orderbook::default();
        ob.reset(symbol, 10, 24_000.0, 28_000.0, 2);

        let price = 26_000.0;
        let amount = 1.0;
        ob.add_bid(ExchangeType::Binance, [price, amount]).unwrap();
        let level_price = ob.get_price(price);
        let level_amount = ob.get_amount(amount);
        let pp = ob.get_price_point_mut(level_price);
        print_memory_usage();

        assert_eq!(*pp.bids.get(&ExchangeType::Binance).unwrap(), level_amount);
        assert_eq!(ob.bid_max, level_price);
    }

    #[test]
    fn removes_a_bid() {
        let symbol = "BTCUSD".to_string();
        let mut ob = Orderbook::default();
        ob.reset(symbol, 10, 24_000.0, 24_100.0, 2);

        let prices = [24_051.0, 24_050.0];
        let amount = 1.0;
        let level_amount = ob.get_amount(1.0);
        prices.into_iter().for_each(|price| {
            ob.add_bid(ExchangeType::Binance, [price, amount]).unwrap();
        });

        let pp = ob.get_price_point_mut(ob.get_price(prices[0]));

        assert_eq!(*pp.bids.get(&ExchangeType::Binance).unwrap(), level_amount);
        assert_eq!(ob.bid_max, ob.get_price(prices[0]));

        let price = 24_051.0;
        ob.add_bid(ExchangeType::Binance, [price, 0.]).unwrap();
        assert_eq!(ob.bid_max, ob.get_price(prices[1]));
    }
}
