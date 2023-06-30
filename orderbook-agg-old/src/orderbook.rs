use crate::booksummary::{ExchangeType, Level, Summary};
use crate::update::{get_snapshot, Update};
use anyhow::{bail, Context, Result};
use futures::future::try_join_all;

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
    pub symbol: String,
    pub ask_min: Price,
    pub bid_max: Price,
    pub price_points: Vec<PricePoint>,
    pub min_price: Price,
    pub max_price: Price,
    levels: u32,
    decimals: u32,
    power_quantity: u32,
    last_update_ids: Vec<u64>,
}

impl Orderbook {
    pub fn new(
        symbol: String,
        levels: u32,
        price_range: f64,
        decimals: Price,
        snapshots: Vec<Update>,
    ) -> Result<Self> {
        let max_bid = snapshots
            .iter()
            .map(|snapshot| {
                snapshot
                    .bids
                    .iter()
                    .map(|bid| bid[0])
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap()
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let min_price =
            (max_bid * (1. - (price_range / 100.))).mul(10u32.pow(decimals) as f64) as Price;
        let max_price =
            (max_bid * (1. + (price_range / 100.))).mul(10u32.pow(decimals) as f64) as Price;
        tracing::info!(
            "Creating new orderbook for {} with {} level(s) and price from {} to {}",
            symbol,
            levels,
            min_price,
            max_price
        );
        let mut price_points: Vec<PricePoint> =
            Vec::with_capacity((max_price - min_price) as usize + 2);

        let mut idx = 0;
        while idx <= (max_price - min_price) as usize {
            price_points.push(PricePoint::default());
            idx += 1;
        }

        let mut ob = Self {
            symbol,
            ask_min: u32::MAX,
            bid_max: 0,
            price_points,
            min_price,
            max_price,
            levels,
            decimals,
            power_quantity: 8,
            last_update_ids: vec![0; ExchangeType::iter().len()],
        };
        for snapshot in snapshots.into_iter() {
            ob.update(snapshot)?;
        }
        Ok(ob)
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
        &self.price_points[price.sub(self.min_price) as usize]
    }

    /// Retrieves a mutable price point from storage.
    fn get_price_point_mut(&mut self, price: Price) -> &mut PricePoint {
        &mut self.price_points[price.sub(self.min_price) as usize]
    }

    /// Adds, modifies or removes a bid from the order book.
    pub fn add_bid(&mut self, exchange: ExchangeType, level: [f64; 2]) -> Result<()> {
        let mut level_price = self.get_price(level[0]);
        if level_price > self.max_price || level_price < self.min_price {
            return Ok(());
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
    pub fn add_ask(&mut self, exchange: ExchangeType, level: [f64; 2]) -> Result<()> {
        let mut level_price = self.get_price(level[0]);
        if level_price > self.max_price || level_price < self.min_price {
            return Ok(());
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
    /// TODO: Try to parallelize this - the number of collisions is likely to be low.
    pub fn update(&mut self, update: Update) -> Result<()> {
        let idx = update.exchange.context("Update exchange is None")? as usize;
        if update.last_update_id > self.last_update_ids[idx as usize] {
            self.last_update_ids[idx] = update.last_update_id;
        } else {
            tracing::info!(
                "Skipping update from {} with id {}",
                update.exchange.unwrap().as_str_name(),
                update.last_update_id
            );
            return Ok(());
        }
        if let Some(exchange) = update.exchange {
            if exchange == ExchangeType::Bitstamp {
                return Ok(());
            }
        }

        for bid in update.bids.into_iter() {
            self.add_bid(update.exchange.context("exchange is none")?, bid)?
        }

        for ask in update.asks.into_iter() {
            self.add_ask(update.exchange.context("exchange is none")?, ask)?
        }

        Ok(())
    }

    /// Adds snapshots from all exchanges for a given symbol.
    pub async fn get_snapshots(symbol: &String) -> Result<Vec<Update>> {
        try_join_all(
            ExchangeType::iter()
                .map(|exchange| get_snapshot(exchange, symbol))
                .collect::<Vec<_>>(),
        )
        .await
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

        summary_bids.truncate(self.levels as usize);

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

        summary_asks.truncate(self.levels as usize);

        summary_asks
    }

    /// Create the summary.
    /// TODO: Try to parallelize this - the orderbook is only being read.
    pub fn get_summary(&self) -> Result<Summary> {
        let summary_asks = self.get_summary_asks();
        let summary_bids = self.get_summary_bids();

        if summary_asks.is_empty() || summary_bids.is_empty() {
            tracing::error!(
                "Summary spread cannot be calculated with {} bids and {} asks",
                summary_bids.len(),
                summary_asks.len()
            );
            bail!("Summary spread cannot be calculated".to_string());
        }

        let spread = summary_asks[0].price - summary_bids[0].price;

        if summary_asks[0].exchange == summary_bids[0].exchange && spread < 0. {
            tracing::error!(
                "Summary spread of {} should not be negative with bid and ask from the same exchange {:#?} {:#?}",
                spread,
                summary_bids[0],
                summary_asks[0]
            );
            bail!("Summary spread cannot be calculated".to_string());
        }

        Ok(Summary {
            symbol: self.symbol.clone(),
            spread,
            timestamp: 0,
            bids: summary_bids,
            asks: summary_asks,
        })
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
    fn it_converts_prices_correctly() {
        // assert_eq!(1.001f64.mul(10u32.pow(5) as f64) as u32, 100100);
        for z in 1..9 {
            assert_eq!(
                (30000. + z as f64 / 100.).mul(10u32.pow(2) as f64) as u32,
                3000000 + z
            );
            let price = z as f64;
            let price_u32 = price.mul(10u32.pow(8) as f64) as u32;
            let price_f64 = (price_u32 as f64) / (10u32.pow(8) as f64);
            assert_eq!(price, price_f64);
        }

        assert_eq!((92.93529970f64).mul(10u32.pow(8) as f64) as u64, 9293529970);
        let original_prices = vec![
            0.0, 1.0, 1.1, 1.01, 1.001, 1.0001, 1.00001, 1.000001, 1.0000001,
        ];
        let mut price_levels: Vec<Vec<u32>> = vec![vec![0, 100, 110, 101, 100, 100, 100, 100, 100]];
        let decimals_list = (2..9).into_iter().collect::<Vec<u32>>();
        for decimals in decimals_list.clone() {
            let mut price_levels_decimals = vec![];
            for (idx, price) in price_levels[(decimals - 2) as usize].iter().enumerate() {
                let mut level_price = price * 10;
                if idx == decimals as usize + 2 {
                    level_price += 1;
                }
                price_levels_decimals.push(level_price);
            }
            price_levels.push(price_levels_decimals);
        }

        for (i, decimals) in decimals_list.clone().iter().enumerate() {
            let ob = Orderbook::new(
                "TEST".to_string(),
                1,
                11.0,
                decimals.clone(),
                vec![Update {
                    exchange: Some(ExchangeType::Binance),
                    last_update_id: 1234,
                    bids: vec![[1.0, 1.0]],
                    asks: vec![[1.0, 1.0]],
                }],
            )
            .unwrap();
            for (j, price) in original_prices.iter().enumerate() {
                if j + 1 > *decimals as usize {
                    break;
                }
                let level_price = ob.get_price(*price);
                let display_price = (level_price as f64) / (10u64.pow(decimals.clone()) as f64);
                println!(
                    "Decimals: {}, Price: {}, level price: {}, display price: {}, price level: {}, price levels: {:?}, {}, {}",
                    decimals, price, level_price, display_price, price_levels[i][j], price_levels[i], i, j
                );
                assert_eq!(level_price, price_levels[i][j]);
                assert_eq!(display_price, *price as f64);
            }
        }
    }

    #[test]
    fn it_creates_an_orderbook() {
        let symbol = "TEST".to_string();
        let snapshots = vec![Update {
            exchange: Some(ExchangeType::Binance),
            last_update_id: 1234,
            bids: vec![[1.0, 1.0]],
            asks: vec![[1.0, 1.0]],
        }];
        let ob = Orderbook::new(symbol, 10, 10.0, 2, snapshots).unwrap();
        print_memory_usage();
        assert_eq!(ob.price_points.len(), 21);
        let level_price = ob.get_price(1.0);
        let level_quantity = ob.get_quantity(1.0);
        let pp = ob.get_price_point(level_price);
        assert_eq!(
            *pp.bids.get(&ExchangeType::Binance).unwrap(),
            level_quantity
        );
        // change assertion to false to see memory usage
    }

    #[test]
    fn adds_a_bid() {
        let symbol = "TEST".to_string();
        let snapshots = vec![Update {
            exchange: Some(ExchangeType::Binance),
            last_update_id: 1234,
            bids: vec![[25_900.0, 1.0]],
            asks: vec![[1.0, 1.0]],
        }];
        let mut ob = Orderbook::new(symbol, 10, 10.0, 2, snapshots).unwrap();

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
        let symbol = "TEST".to_string();
        let price2 = 26_100.0;
        let price1 = 26_000.0;

        let snapshots = vec![Update {
            exchange: Some(ExchangeType::Binance),
            last_update_id: 1234,
            bids: vec![[price2, 1.0], [price1, 1.0]],
            asks: vec![[1.0, 1.0]],
        }];
        let mut ob = Orderbook::new(symbol, 10, 10.0, 2, snapshots).unwrap();

        let level_quantity = ob.get_quantity(1.0);
        let pp = ob.get_price_point_mut(ob.get_price(price2));

        assert_eq!(
            *pp.bids.get(&ExchangeType::Binance).unwrap(),
            level_quantity
        );
        assert_eq!(ob.bid_max, ob.get_price(price2));

        ob.add_bid(ExchangeType::Binance, [price2, 0.]).unwrap();
        assert_eq!(ob.bid_max, ob.get_price(price1));
    }
}
