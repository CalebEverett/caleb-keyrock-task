use crate::booksummary::ExchangeType;
use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::prelude::*;
use serde_json::Value;

use rust_decimal::Decimal;

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Update {
    pub exchange: ExchangeType,
    #[serde(
        alias = "microtimestamp",
        alias = "E",
        deserialize_with = "deserialize_number_from_string"
    )]
    pub last_update_id: u64,
    #[serde(deserialize_with = "from_str")]
    pub bids: Vec<[Decimal; 2]>,
    #[serde(deserialize_with = "from_str")]
    pub asks: Vec<[Decimal; 2]>,
}

fn from_str<'de, D>(deserializer: D) -> Result<Vec<[Decimal; 2]>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<[&str; 2]> = Deserialize::deserialize(deserializer)?;
    Ok(v.into_iter()
        .map(|s| (s[0].parse::<Decimal>(), s[1].parse::<Decimal>()))
        .filter_map(|p| Some([p.0.ok()?, p.1.ok()?]))
        .collect::<Vec<[Decimal; 2]>>())
}

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
