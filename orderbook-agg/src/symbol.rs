use anyhow::{ensure, Context, Result};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{Deserialize, Serialize};

use crate::booksummary::ExchangeType;

pub type StoragePrice = u32;
pub type StorageQuantity = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SymbolInfo {
    pub display_price_min: Decimal,
    pub display_price_max: Decimal,
}

impl Default for SymbolInfo {
    fn default() -> Self {
        Self {
            display_price_min: Decimal::new(0, 0),
            display_price_max: Decimal::MAX,
        }
    }
}

// we have a symbol to make sure we are using the same symbol across all exchanges
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Symbol {
    BTCUSDT { info: SymbolInfo },
    BTCUSD { info: SymbolInfo },
    ETHBTC { info: SymbolInfo },
}

impl Symbol {
    pub fn get_info(&self) -> SymbolInfo {
        match self {
            Symbol::BTCUSDT { info } => *info,
            Symbol::BTCUSD { info } => *info,
            Symbol::ETHBTC { info } => *info,
        }
    }
    pub fn set_info(&self, info: SymbolInfo) -> Self {
        match self {
            Symbol::BTCUSDT { .. } => Symbol::BTCUSDT { info },
            Symbol::BTCUSD { .. } => Symbol::BTCUSD { info },
            Symbol::ETHBTC { .. } => Symbol::ETHBTC { info },
        }
    }
}

impl Default for Symbol {
    fn default() -> Self {
        Symbol::BTCUSDT {
            info: SymbolInfo::default(),
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Symbol::BTCUSDT { .. } => write!(f, "BTCUSDT"),
            Symbol::BTCUSD { .. } => write!(f, "BTCUSD"),
            Symbol::ETHBTC { .. } => write!(f, "ETHBTC"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExchangeSymbolInfo {
    pub exchange: ExchangeType,
    pub symbol: Symbol,
    pub scale_price: u32,
    pub scale_quantity: u32,
}

// TODO: check to see if this is slow
fn display_to_storage(mut display: Decimal, scale_add: u32) -> Result<u32> {
    ensure!(display > Decimal::new(0, 0), "price must be greater than 0");
    let scale = display.scale();
    display.set_scale(scale + scale_add)?;
    display.normalize_assign();
    let mantissa = display.mantissa();
    let bytes = mantissa.to_le_bytes();
    let mut new_bytes: [u8; 4] = [0; 4];
    for (i, b) in bytes.into_iter().enumerate() {
        if i < 4 {
            new_bytes[i] = b;
        } else {
            if b != 0 {
                ensure!(b == 0, "price is too large");
            }
        }
    }
    let storage = u32::from_le_bytes(new_bytes);
    Ok(storage)
}

impl ExchangeSymbolInfo {
    pub fn get_storage_price(&self, display_price: Decimal) -> Result<u32> {
        display_to_storage(display_price, self.scale_price)
    }

    pub fn get_storage_quantity(&self, display_quantity: Decimal) -> Result<u32> {
        display_to_storage(display_quantity, self.scale_quantity)
    }

    pub fn get_storage_price_min(&self) -> Result<u32> {
        let info = self.symbol.get_info();
        self.get_storage_price(info.display_price_min)
    }

    pub fn get_storage_price_max(&self) -> Result<u32> {
        let info = self.symbol.get_info();
        self.get_storage_price(info.display_price_max)
    }

    pub fn get_display_price(&self, storage_price: StoragePrice) -> Decimal {
        let mut decimal = Decimal::from_u32(storage_price).unwrap();
        let scale = decimal.scale();
        decimal.set_scale(scale - self.scale_price).unwrap();
        decimal
    }

    pub fn get_display_quantity(&self, storage_quantity: StorageQuantity) -> Decimal {
        let mut decimal = Decimal::from_u64(storage_quantity).unwrap();
        let scale = decimal.scale();
        decimal.set_scale(scale - self.scale_quantity).unwrap();
        decimal
    }
}
