use crate::booksummary::{ExchangeType, Level, Summary};
use crate::snapshot::{get_snapshots, Snapshot};
use crate::update::Updates;

use std::{
    collections::HashMap,
    ops::{Mul, Sub},
};
use strum::IntoEnumIterator;

type Price = u32;
type Quantity = u64;

/// Holds levels for each price point. Entries will at most be equal
/// to the number of exchanges.
#[derive(Debug)]
pub struct PricePoint {
    pub bids: HashMap<ExchangeType, Quantity>,
    pub asks: HashMap<ExchangeType, Quantity>,
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

/// Orderbook structure.
#[derive(Debug, Default)]
pub struct Orderbook {
    pub ask_min: Price,
    pub bid_max: Price,
    pub symbol: String,
    pub price_points: Vec<PricePoint>,
    pub min_price: Price,
    pub max_price: Price,
    levels: u32,
    decimals: u32,
    power_quantity: u32,
}

impl Orderbook {
    /// Initializes an already creaated orderbook to facilitate Arc<Mutex<Orderbook>>
    /// on the server.
    pub fn reset(
        &mut self,
        symbol: String,
        levels: u32,
        min_price: f64,
        max_price: f64,
        decimals: Price,
    ) {
        let min_price = min_price.mul(10u32.pow(decimals) as f64) as Price;
        let max_price = max_price.mul(10u32.pow(decimals) as f64) as Price;
        tracing::info!(
            "Resetting orderbook for {} with levels {} and price range {} - {}",
            symbol,
            levels,
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
        self.levels = levels;
        self.decimals = decimals;
        self.power_quantity = 8;
    }

    /// Gets the index for a given price.
    fn get_idx(&self, price: Price) -> Price {
        price.sub(self.min_price)
    }

    /// Gets storage representation of price from its display price.
    fn get_price(&self, price: f64) -> Price {
        (price.mul(10u32.pow(self.decimals) as f64)) as Price
    }

    /// Gets storage representation of a quantity from its display quantity.
    fn get_quantity(&self, quantity: f64) -> Quantity {
        (quantity.mul(10u64.pow(self.power_quantity) as f64)) as Quantity
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

        let level_quantity = self.get_quantity(level[1]);
        let bid_max = self.bid_max;
        let bids = &mut self.get_price_point_mut(level_price).bids;

        if level_quantity > 0 {
            bids.insert(exchange, level_quantity);
            if level_price > self.bid_max {
                self.bid_max = level_price;
            }
        } else {
            // level.quantity == 0.
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

        let level_quantity = self.get_quantity(level[1]);
        let ask_min = self.ask_min;
        let asks = &mut self.get_price_point_mut(level_price).asks;

        if level_quantity > 0 {
            asks.insert(exchange, level_quantity);
            if level_price < self.ask_min {
                self.ask_min = level_price;
            }
        } else {
            // level.quantity == 0.
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
            let quantity = b[1];
            let _ = self.add_bid(updates.exchange.clone(), [price, quantity]);
        });
        updates.asks.into_iter().for_each(|a| {
            let price = a[0];
            let quantity = a[1];
            let _ = self.add_ask(updates.exchange.clone(), [price, quantity]);
        });
    }

    /// Processes an orderbook snapshot from an exchange into the orderbook.
    /// TODO: Try to parallelize this - the number of collisions is likely to be low.
    fn add_snapshot(&mut self, exchange: ExchangeType, snapshot: Snapshot) {
        snapshot.bids.into_iter().for_each(|b| {
            let price = b[0];
            let quantity = b[1];
            let _ = self.add_bid(exchange, [price, quantity]);
        });
        snapshot.asks.into_iter().for_each(|a| {
            let price = a[0];
            let quantity = a[1];
            let _ = self.add_ask(exchange, [price, quantity]);
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
        let mut summary_bids = Vec::<Level>::with_capacity(self.levels as usize);
        let mut counter = 0;
        let mut bid_max = self.bid_max;
        loop {
            let mut ob_bids: Vec<Level> = self
                .get_price_point(bid_max)
                .bids
                .iter()
                .map(|(k, v)| Level {
                    price: (bid_max as f64) / (10u64.pow(self.decimals) as f64),
                    quantity: (v.clone() as f64) / (10u64.pow(self.power_quantity) as f64),
                    exchange: k.clone() as i32,
                })
                .collect::<Vec<Level>>();

            ob_bids.sort_by(|a, b| b.quantity.partial_cmp(&a.quantity).unwrap());
            counter += ob_bids.len() as u32;
            summary_bids.append(&mut ob_bids);

            if counter >= self.levels || bid_max == self.min_price {
                break;
            }
            bid_max -= 1;
        }
        summary_bids
    }

    /// Collect asks for the summary.
    fn get_summary_asks(&self) -> Vec<Level> {
        let mut summary_asks = Vec::<Level>::with_capacity(self.levels as usize);
        let mut counter = 0;
        let mut ask_min = self.ask_min;
        loop {
            let mut ob_asks: Vec<Level> = self
                .get_price_point(ask_min)
                .asks
                .iter()
                .map(|(k, v)| Level {
                    price: (ask_min as f64) / (10u64.pow(self.decimals) as f64),
                    quantity: (v.clone() as f64) / (10u64.pow(self.power_quantity) as f64),
                    exchange: k.clone() as i32,
                })
                .collect::<Vec<Level>>();

            ob_asks.sort_by(|a, b| a.quantity.partial_cmp(&b.quantity).unwrap());
            counter += ob_asks.len() as u32;
            summary_asks.append(&mut ob_asks);

            if counter >= self.levels || ask_min == self.max_price {
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
        ob.reset(symbol, 10, 25_000.0, 30_000.0, 2);
        print_memory_usage();
        assert!(false)
    }

    #[test]
    fn adds_a_bid() {
        let symbol = "BTCUSD".to_string();
        let mut ob = Orderbook::default();
        ob.reset(symbol, 10, 24_000.0, 28_000.0, 2);

        let price = 26_000.0;
        let quantity = 1.0;
        ob.add_bid(ExchangeType::Binance, [price, quantity])
            .unwrap();
        let level_price = ob.get_price(price);
        let level_quantity = ob.get_quantity(quantity);
        let pp = ob.get_price_point_mut(level_price);
        print_memory_usage();

        assert_eq!(
            *pp.bids.get(&ExchangeType::Binance).unwrap(),
            level_quantity
        );
        assert_eq!(ob.bid_max, level_price);
    }

    #[test]
    fn removes_a_bid() {
        let symbol = "BTCUSD".to_string();
        let mut ob = Orderbook::default();
        ob.reset(symbol, 10, 24_000.0, 24_100.0, 2);

        let prices = [24_051.0, 24_050.0];
        let quantity = 1.0;
        let level_quantity = ob.get_quantity(1.0);
        prices.into_iter().for_each(|price| {
            ob.add_bid(ExchangeType::Binance, [price, quantity])
                .unwrap();
        });

        let pp = ob.get_price_point_mut(ob.get_price(prices[0]));

        assert_eq!(
            *pp.bids.get(&ExchangeType::Binance).unwrap(),
            level_quantity
        );
        assert_eq!(ob.bid_max, ob.get_price(prices[0]));

        let price = 24_051.0;
        ob.add_bid(ExchangeType::Binance, [price, 0.]).unwrap();
        assert_eq!(ob.bid_max, ob.get_price(prices[1]));
    }
}
