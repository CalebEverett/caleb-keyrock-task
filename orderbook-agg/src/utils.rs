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

// TODO: check to see if this is slow
pub fn display_to_storage_price(mut display_price: Decimal, scale_add: u32) -> Result<u32> {
    ensure!(
        display_price.is_sign_positive(),
        "price sign must be positive"
    );
    display_price = display_price.trunc_with_scale(scale_add);
    display_price.set_scale(0)?;

    let unpacked = display_price.unpack();
    ensure!(unpacked.mid == 0 && unpacked.hi == 0, "price is too large");

    Ok(unpacked.lo)
}

pub fn display_to_storage_quantity(mut display_quantity: Decimal, scale_add: u32) -> Result<u64> {
    ensure!(
        display_quantity.is_sign_positive(),
        "quantity sign must be positive"
    );

    display_quantity = display_quantity.trunc_with_scale(scale_add);
    display_quantity.set_scale(0)?;

    let unpacked = display_quantity.unpack();
    ensure!(unpacked.hi == 0, "price is too large");

    let storage = unpacked.lo as u64 + (unpacked.mid as u64) << 32;
    Ok(storage)
}
