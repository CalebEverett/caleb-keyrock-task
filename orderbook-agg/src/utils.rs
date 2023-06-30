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
pub fn display_to_storage_price(mut display: Decimal, scale_add: u32) -> Result<u32> {
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

pub fn display_to_storage_quantity(mut display: Decimal, scale_add: u32) -> Result<u64> {
    ensure!(display > Decimal::new(0, 0), "price must be greater than 0");
    let scale = display.scale();
    display.set_scale(scale + scale_add)?;
    display.normalize_assign();

    let mantissa = display.mantissa();
    let bytes = mantissa.to_le_bytes();
    let mut new_bytes: [u8; 8] = [0; 8];
    for (i, b) in bytes.into_iter().enumerate() {
        if i < 4 {
            new_bytes[i] = b;
        } else {
            if b != 0 {
                ensure!(b == 0, "price is too large");
            }
        }
    }
    let storage = u64::from_le_bytes(new_bytes);
    Ok(storage)
}
