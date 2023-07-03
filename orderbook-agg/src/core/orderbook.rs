use super::number_types::*;
use crate::booksummary::Exchange;
use crate::Symbol;
use anyhow::Result;
use rust_decimal::Decimal;
use tracing::instrument;

/// Updates from all exchanges should implement this trait
pub trait Update {
    fn validate(&self) -> Result<()>;
    fn last_update_id(&self) -> u64;
    fn bids_mut(&mut self) -> &mut Vec<[DisplayPrice; 2]>;
    fn asks_mut(&mut self) -> &mut Vec<[DisplayPrice; 2]>;
}

#[derive(Debug)]
pub enum OrderbookMessage<U> {
    Update(U),
    Summary(String),
}

#[derive(Debug)]
pub struct Orderbook {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub storage_ask_min: StoragePrice,
    pub storage_bid_max: StoragePrice,
    pub storage_price_min: StoragePrice,
    pub storage_price_max: StoragePrice,
    pub scale_price: u32,
    pub scale_quantity: u32,
    pub bids: Vec<StorageQuantity>,
    pub asks: Vec<StorageQuantity>,
    pub last_update_id: u64,
}

impl Orderbook {
    #[instrument]
    pub fn new(
        exchange: Exchange,
        symbol: Symbol,
        storage_price_min: StoragePrice,
        storage_price_max: StoragePrice,
        scale_price: u32,
        scale_quantity: u32,
    ) -> Self {
        let capacity = (storage_price_max - storage_price_min) as usize + 1;
        let mut bids: Vec<StorageQuantity> = Vec::with_capacity(capacity);
        let mut asks: Vec<StorageQuantity> = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            bids.push(0);
            asks.push(0);
        }

        Self {
            exchange,
            symbol,
            storage_ask_min: StoragePrice::MIN,
            storage_bid_max: StoragePrice::MAX,
            storage_price_min,
            storage_price_max,
            scale_price,
            scale_quantity,
            bids,
            asks,
            last_update_id: u64::MIN,
        }
    }
    fn display_price(&self, price: StoragePrice) -> Result<DisplayPrice> {
        price.to_display(self.scale_price)
    }
    fn storage_price(&self, price: DisplayPrice) -> Result<StoragePrice> {
        price.to_storage(self.scale_price)
    }
    fn display_quantity(&self, quantity: StorageQuantity) -> Result<DisplayQuantity> {
        quantity.to_display(self.scale_price)
    }
    fn storage_quantity(&self, quantity: DisplayQuantity) -> Result<StorageQuantity> {
        quantity.to_storage(self.scale_price)
    }
    fn bids(&self) -> &Vec<StorageQuantity> {
        &self.bids
    }
    fn bids_mut(&mut self) -> &mut Vec<StorageQuantity> {
        &mut self.bids
    }
    fn asks(&self) -> &Vec<StorageQuantity> {
        &self.asks
    }
    fn asks_mut(&mut self) -> &mut Vec<StorageQuantity> {
        &mut self.asks
    }
    fn idx(&self, storage_price: StoragePrice) -> usize {
        (storage_price - self.storage_price_min) as usize
    }
    /// Adds, modifies or removes a bid from the order book.
    pub fn add_bid(&mut self, level: [Decimal; 2]) -> Result<()> {
        let mut storage_price = self.storage_price(level[0])?;
        if storage_price > self.storage_price_max || storage_price < self.storage_price_min {
            return Ok(());
        }

        let storage_quantity = self.storage_quantity(level[1])?;
        let mut idx = self.idx(storage_price);

        // let mut bids = self.bids.lock().unwrap();

        {
            let bids = self.bids_mut();
            bids[idx] = storage_quantity;
        }

        if storage_quantity > 0 {
            if storage_price > self.storage_bid_max {
                self.storage_bid_max = storage_price;
            }
        } else {
            if storage_price == self.storage_bid_max && storage_quantity == 0 {
                loop {
                    storage_price -= 1;
                    idx = self.idx(storage_price).clone();
                    if &self.bids[idx] > &0 {
                        self.storage_bid_max = storage_price;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    /// Adds, modifies or removes a bid from the order book.
    pub fn add_ask(&mut self, level: [Decimal; 2]) -> Result<()> {
        let mut storage_price = self.storage_price(level[0])?;
        if storage_price > self.storage_price_max || storage_price < self.storage_price_min {
            return Ok(());
        }

        let storage_quantity = self.storage_quantity(level[1])?;
        let mut idx = self.idx(storage_price);

        {
            let asks = self.asks_mut();
            asks[idx] = storage_quantity;
        }

        if storage_quantity > 0 {
            if storage_price < self.storage_ask_min {
                self.storage_ask_min = storage_price;
            }
        } else {
            if storage_price == self.storage_ask_min && storage_quantity == 0 {
                loop {
                    storage_price += 1;
                    idx = self.idx(storage_price);
                    if &self.asks[idx] > &0 {
                        self.storage_ask_min = storage_price;
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

    #[test]
    fn it_converts_numbers_correctly() {
        let ob = Orderbook::new(Exchange::Binance, Symbol::BTCUSDT, 700, 4200, 2, 8);

        let display_price = ob.display_price(4200).unwrap();
        assert_eq!(display_price.to_string(), "42.00");

        let storage_price = ob.storage_price(Decimal::new(u32::MAX as i64, 2)).unwrap();
        assert_eq!(storage_price, u32::MAX);

        let test_num = Decimal::from_i128_with_scale(u64::MAX as i128, 2);
        let storage_quantity = ob.storage_quantity(test_num).unwrap();

        assert_eq!(storage_quantity, u64::MAX);

        if let Err(err) = ob.storage_price(Decimal::new(u32::MAX as i64 + 1, 2)) {
            assert_eq!(err.to_string(), "price is too large");
        } else {
            panic!("greater than u32 should have failed");
        };

        if let Err(err) =
            ob.storage_quantity(Decimal::from_i128_with_scale(u64::MAX as i128 + 1, 8))
        {
            assert_eq!(err.to_string(), "quantity is too large");
        } else {
            panic!("greater than u64 should have failed");
        };

        if let Err(err) = ob.storage_price(Decimal::new(-1, 0)) {
            assert_eq!(err.to_string(), "price sign must be positive");
        } else {
            panic!("negative should have failed");
        };

        if let Err(err) = ob.storage_quantity(Decimal::new(-1, 0)) {
            assert_eq!(err.to_string(), "quantity sign must be positive");
        } else {
            panic!("negative should have failed");
        };
    }

    #[test]
    fn it_converts_extreme_numbers_correctly() {
        let ob = Orderbook::new(Exchange::Binance, Symbol::BTCUSDT, 1, 42, 8, 8);

        assert_eq!(
            ob.storage_price(ob.display_price(ob.storage_price_min).unwrap())
                .unwrap(),
            1
        );

        assert_eq!(
            ob.display_price(ob.storage_price_min).unwrap().to_string(),
            "0.00000001"
        );

        assert_eq!(
            ob.storage_price(ob.display_price(ob.storage_price_max).unwrap())
                .unwrap(),
            42
        );

        assert_eq!(
            ob.storage_quantity(ob.display_price(ob.storage_price_min).unwrap())
                .unwrap(),
            1
        );
        assert_eq!(ob.display_quantity(1).unwrap().to_string(), "0.00000001");
    }
}
