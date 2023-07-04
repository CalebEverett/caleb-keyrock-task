use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;

/// Converts a pair of strings to a pair of numbers of type T.
pub fn str_pair_to_num<T>(pair: &Value) -> Result<[T; 2]>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let price = pair[0]
        .as_str()
        .context("Failed to get price str")?
        .parse::<T>()
        .context("Failed to parse price str to num")?;
    let qty = pair[1]
        .as_str()
        .context("Failed to get qty str")?
        .parse::<T>()
        .context("Failed to parse price str to num")?;
    Ok([price, qty])
}

/// Converts an array of pairs of strings to a vec of pairs of nums of type T.
pub fn str_vec_to_num_vec<T>(str_vec: &Value) -> Result<Vec<[T; 2]>>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let num_vec = str_vec
        .as_array()
        .context("Failed to get array")?
        .iter()
        .filter_map(|p| str_pair_to_num::<T>(p).ok())
        .collect::<Vec<[T; 2]>>();

    Ok(num_vec)
}

pub type DisplayAmount = Decimal;
impl ToStorage for DisplayAmount {
    fn to_storage(&self, scale: u32) -> Result<StorageAmount> {
        display_to_storage(*self, scale)
    }
}
pub type StorageAmount = u64;
impl ToDisplay for StorageAmount {
    fn to_display(&self, scale: u32) -> Result<DisplayAmount> {
        let mut display_price = Decimal::from(*self);
        display_price.set_scale(scale)?;
        Ok(display_price)
    }
}

pub fn display_to_storage(mut display_quantity: Decimal, scale: u32) -> Result<StorageAmount> {
    ensure!(
        display_quantity.is_sign_positive(),
        "quantity sign must be positive"
    );

    display_quantity = display_quantity.round_dp(scale);
    display_quantity.set_scale(0)?;

    let unpacked = display_quantity.unpack();
    ensure!(unpacked.hi == 0, "quantity is too large");

    let mut storage = unpacked.lo as u64;
    if unpacked.mid > 0 {
        storage += (unpacked.mid as u64) << 32;
    }
    Ok(storage)
}

pub trait ToDisplay {
    fn to_display(&self, scale: u32) -> Result<DisplayAmount>;
}
pub trait ToStorage {
    fn to_storage(&self, scale: u32) -> Result<StorageAmount>;
}
