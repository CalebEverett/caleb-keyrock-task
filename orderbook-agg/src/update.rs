use super::ExchangeUpdate;
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
