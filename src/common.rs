use orderbook::{ExchangeType, Level, Summary};
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    ops::{Mul, Sub},
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
    symbol: String,
) -> Result<Snapshot, Box<dyn std::error::Error + Send + Sync>> {
    let url = match exchange {
        orderbook::ExchangeType::Binance => {
            format!(
                "https://www.binance.us/api/v3/depth?symbol={}&limit=1000",
                symbol
            )
        }
        orderbook::ExchangeType::Bitstamp => {
            format!("https://www.bitstamp.net/api/v2/order_book/{}/", symbol)
        }
    };

    let mut snapshot = reqwest::get(url)
        .await
        .expect("Failed to get snapshot")
        .json::<Snapshot>()
        .await
        .expect("Failed to parse json");

    if exchange == ExchangeType::Bitstamp {
        snapshot.bids = snapshot.bids[0..1000].to_vec();
        snapshot.asks = snapshot.asks[0..1000].to_vec();
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

pub struct Orderbook {
    pub ask_min: Price,
    pub bid_max: Price,
    pub symbol: String,
    pub price_points: Vec<PricePoint>,
    pub min_price: Price,
    pub max_price: Price,
    power_price: u32,
    power_amount: u32,
}

impl Orderbook {
    pub fn new(symbol: String, min_price: f64, max_price: f64, power_price: Price) -> Self {
        let min_price = min_price.mul(10u32.pow(power_price) as f64) as Price;
        let max_price = max_price.mul(10u32.pow(power_price) as f64) as Price;

        let mut pps: Vec<PricePoint> = Vec::with_capacity((max_price - min_price) as usize + 1);

        let mut idx = 0;
        while idx < (max_price - min_price) as usize {
            pps.push(PricePoint::default());
            idx += 1;
        }

        Self {
            symbol,
            ask_min: u32::MAX,
            bid_max: 0,
            price_points: pps,
            min_price,
            max_price,
            power_price,
            power_amount: 8,
        }
    }

    fn get_idx(&self, price: Price) -> Price {
        price.sub(self.min_price)
    }

    fn get_price(&self, price: f64) -> Price {
        (price.mul(10u32.pow(self.power_price) as f64)) as Price
    }

    fn get_amount(&self, amount: f64) -> Amount {
        (amount.mul(10u64.pow(self.power_amount) as f64)) as Amount
    }

    fn get_price_point(&self, price: Price) -> &PricePoint {
        let idx = self.get_idx(price) as usize;
        &self.price_points[idx]
    }

    fn get_price_point_mut(&mut self, price: Price) -> &mut PricePoint {
        let idx = self.get_idx(price) as usize;
        &mut self.price_points[idx]
    }

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
    pub fn add_snapshot(&mut self, exchange: ExchangeType, snapshot: Snapshot) {
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

    pub fn get_summary(&mut self, limit: i32) -> Summary {
        let mut summary_bids = Vec::<Level>::with_capacity(limit as usize);
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
            counter += ob_bids.len() as i32;
            summary_bids.append(&mut ob_bids);

            if counter >= limit || bid_max == self.min_price {
                break;
            }
            bid_max -= 1;
        }

        counter = 0;
        let mut ask_min = self.ask_min;
        let mut summary_asks = Vec::<Level>::with_capacity(limit as usize);
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
            counter += ob_asks.len() as i32;
            summary_asks.append(&mut ob_asks);

            if counter >= limit || ask_min == self.max_price {
                break;
            }
            ask_min += 1;
        }
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
        Orderbook::new(symbol, 25_000.0, 28_000.0, 2);
        print_memory_usage();
    }

    #[test]
    fn adds_a_bid() {
        let symbol = "BTCUSD".to_string();
        let mut ob = Orderbook::new(symbol, 24_000.0, 28_000.0, 2);

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
        let mut ob = Orderbook::new(symbol, 24_000.0, 24_100.0, 2);

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
