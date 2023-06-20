use orderbook::{ExchangeType, Level, Summary, Symbol};
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    ops::Mul,
};
use strum::IntoEnumIterator;
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    #[serde(
        alias = "microtimestamp",
        deserialize_with = "deserialize_number_from_string"
    )]
    pub last_update_id: i64,
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

pub async fn get_snapshot(
    exchange: ExchangeType,
    symbol: Symbol,
    limit: usize,
) -> Result<Snapshot, Box<dyn std::error::Error + Send + Sync>> {
    let url = match exchange {
        orderbook::ExchangeType::Binance => {
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit={}",
                symbol.symbol, limit
            )
        }
        orderbook::ExchangeType::Bitstamp => format!(
            "https://www.bitstamp.net/api/v2/order_book/{}/",
            symbol.symbol
        ),
    };

    let mut snapshot = reqwest::get(url)
        .await
        .expect("Failed to get snapshot")
        .json::<Snapshot>()
        .await
        .expect("Failed to parse json");

    if exchange == ExchangeType::Bitstamp {
        snapshot.bids = snapshot.bids[0..limit].to_vec();
        snapshot.asks = snapshot.asks[0..limit].to_vec();
    }

    Ok(snapshot)
}

pub type Price = u32;
pub type Amount = u64;

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

pub struct OrderBook {
    pub ask_min: Price,
    pub bid_max: Price,
    pub symbol: Symbol,
    pub price_points: Vec<PricePoint>,
    pub min_price: Price,
    pub max_price: Price,
    power_price: u32,
    power_amount: u32,
}

impl OrderBook {
    pub fn new(symbol: Symbol, min_price: f64, max_price: f64, power_price: Price) -> Self {
        let min_price = min_price.mul(10u32.pow(power_price) as f64) as Price;
        let max_price = max_price.mul(10u32.pow(power_price) as f64) as Price;

        let mut pps: Vec<PricePoint> = Vec::with_capacity((max_price - min_price) as usize + 1);

        let mut idx = 0;
        while idx < (max_price - min_price) as usize + 1 {
            pps.push(PricePoint::default());
            idx += 1;
        }

        Self {
            symbol,
            ask_min: 0,
            bid_max: 0,
            price_points: pps,
            min_price,
            max_price,
            power_price,
            power_amount: 8,
        }
    }

    fn get_idx(&self, price: Price) -> Price {
        price - self.min_price
    }

    fn get_price(&self, price: f64) -> Price {
        (price.mul(10u32.pow(self.power_price) as f64)) as Price
    }

    fn get_amount(&self, amount: f64) -> Amount {
        (amount.mul(10u64.pow(self.power_amount) as f64)) as Amount
    }

    fn get_price_point(&mut self, price: Price) -> &mut PricePoint {
        let idx = self.get_idx(price) as usize;
        &mut self.price_points[idx]
    }

    pub fn add_bid(&mut self, level: Level) -> Result<(), Box<dyn std::error::Error>> {
        let mut level_price = self.get_price(level.price);
        if level_price > self.max_price || level_price < self.min_price {
            return Err("Price is out of range".into());
        }

        let level_amount = self.get_amount(level.amount);
        let bid_max = self.bid_max;
        let bids = &mut self.get_price_point(level_price).bids;

        if level.amount > 0. {
            bids.insert(
                ExchangeType::from_i32(level.exchange).unwrap(),
                level_amount,
            );
            if level_price > self.bid_max {
                self.bid_max = level_price;
            }
        } else {
            // level.amount == 0.
            bids.remove(&ExchangeType::from_i32(level.exchange).unwrap());
            // if removed last bid at the highest price, find the next highest price
            if level_price == bid_max && bids.is_empty() {
                loop {
                    level_price -= 1;
                    if !self.get_price_point(level_price).bids.is_empty() {
                        self.bid_max = level_price;
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn add_ask(&mut self, level: Level) -> Result<(), Box<dyn std::error::Error>> {
        let mut level_price = self.get_price(level.price);
        if level_price > self.max_price || level_price < self.min_price {
            return Err("Price is out of range".into());
        }

        let level_amount = self.get_amount(level.amount);
        let ask_min = self.ask_min;
        let asks = &mut self.get_price_point(level_price).asks;

        if level.amount > 0. {
            asks.insert(
                ExchangeType::from_i32(level.exchange).unwrap(),
                level_amount,
            );
            if level_price < self.ask_min {
                self.ask_min = level_price;
            }
        } else {
            // level.amount == 0.
            asks.remove(&ExchangeType::from_i32(level.exchange).unwrap());
            // if removed last ask at the lowest price, find the next lowest price
            if level_price == ask_min && asks.is_empty() {
                loop {
                    level_price += 1;
                    if !self.get_price_point(level_price).bids.is_empty() {
                        self.ask_min = level_price;
                        break;
                    }
                }
            }
        }

        Ok(())
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
        let symbol = Symbol {
            symbol: "BTCUSD".to_string(),
        };
        OrderBook::new(symbol, 25_000.0, 28_000.0, 2);
        print_memory_usage();
    }

    #[test]
    fn adds_a_bid() {
        let symbol = Symbol {
            symbol: "BTCUSD".to_string(),
        };
        let mut ob = OrderBook::new(symbol, 24_000.0, 28_000.0, 2);

        let price = 26_000.0;
        let amount = 1.0;
        let level = Level {
            price,
            amount,
            exchange: ExchangeType::Binance as i32,
        };
        ob.add_bid(level).unwrap();
        let level_price = ob.get_price(price);
        let level_amount = ob.get_amount(amount);
        let pp = ob.get_price_point(level_price);
        print_memory_usage();

        assert_eq!(*pp.bids.get(&ExchangeType::Binance).unwrap(), level_amount);
        assert_eq!(ob.bid_max, level_price);
    }

    #[test]
    fn removes_a_bid() {
        let symbol = Symbol {
            symbol: "BTCUSD".to_string(),
        };
        let mut ob = OrderBook::new(symbol, 24_000.0, 24_100.0, 2);

        let prices = [24_051.0, 24_050.0];
        let level_amount = ob.get_amount(1.0);
        prices.iter().for_each(|price| {
            let level = Level {
                price: *price,
                amount: 1.0,
                exchange: ExchangeType::Binance as i32,
            };
            ob.add_bid(level).unwrap();
        });

        let pp = ob.get_price_point(ob.get_price(prices[0]));

        assert_eq!(*pp.bids.get(&ExchangeType::Binance).unwrap(), level_amount);
        assert_eq!(ob.bid_max, ob.get_price(prices[0]));

        let price = 24_051.0;
        let level = Level {
            price,
            amount: 0.,
            exchange: ExchangeType::Binance as i32,
        };
        ob.add_bid(level).unwrap();
        assert_eq!(ob.bid_max, ob.get_price(prices[1]));
    }
}
