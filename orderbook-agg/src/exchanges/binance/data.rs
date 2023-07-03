use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::str::FromStr;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

use crate::{
    core::{num_types::DisplayPrice, orderbook::Update},
    Symbol,
};

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

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub last_update_id: u64,
    #[serde(deserialize_with = "from_str")]
    pub bids: Vec<[Decimal; 2]>,
    #[serde(deserialize_with = "from_str")]
    pub asks: Vec<[Decimal; 2]>,
}

impl Update for Snapshot {
    fn validate(&self, _: u64) -> Result<()> {
        Ok(())
    }
    fn last_update_id(&self) -> u64 {
        self.last_update_id
    }
    fn bids_mut(&mut self) -> &mut Vec<[Decimal; 2]> {
        &mut self.bids
    }

    fn asks_mut(&mut self) -> &mut Vec<[Decimal; 2]> {
        &mut self.asks
    }
}

impl Snapshot {
    pub(crate) async fn fetch(url: Url) -> Result<Self> {
        let snapshot = reqwest::get(url)
            .await
            .context("Failed to get snapshot")?
            .json::<Self>()
            .await
            .context("Failed to deserialize snapshot")?;
        Ok(snapshot)
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BookUpdate {
    #[serde(alias = "U")]
    pub first_update_id: u64,
    #[serde(alias = "u")]
    pub last_update_id: u64,
    #[serde(alias = "b", deserialize_with = "from_str")]
    pub bids: Vec<[DisplayPrice; 2]>,
    #[serde(alias = "a", deserialize_with = "from_str")]
    pub asks: Vec<[DisplayPrice; 2]>,
}

impl Update for BookUpdate {
    fn validate(&self, last_id: u64) -> Result<()> {
        let first_update_id = self.first_update_id;
        if last_id == 0 {
            return Ok(());
        }
        ensure!(
            first_update_id == last_id + 1,
            "failed to validate: first_update_id: {first_update_id} != last_id: {last_id} + 1"
        );
        ensure!(
            last_id < self.first_update_id,
            "failed to validate: last_id: {last_id} >= first_update_id: {first_update_id}"
        );
        Ok(())
    }
    fn last_update_id(&self) -> u64 {
        self.last_update_id
    }
    fn bids_mut(&mut self) -> &mut Vec<[DisplayPrice; 2]> {
        &mut self.bids
    }

    fn asks_mut(&mut self) -> &mut Vec<[DisplayPrice; 2]> {
        &mut self.asks
    }
}

impl TryFrom<Message> for BookUpdate {
    type Error = anyhow::Error;
    fn try_from(item: Message) -> Result<Self> {
        serde_json::from_slice::<Self>(&item.into_data()).context("Failed to deserialize update")
    }
}

impl From<Snapshot> for BookUpdate {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            first_update_id: 1,
            last_update_id: snapshot.last_update_id,
            bids: snapshot.bids,
            asks: snapshot.asks,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BestPrice {
    pub symbol: String,
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
}

impl BestPrice {
    pub(super) async fn fetch(url: Url) -> Result<Self> {
        let price = reqwest::get(url)
            .await
            .context("Failed to get price")?
            .json::<Self>()
            .await
            .context("Failed to deserialize binance best price")?;
        Ok(price)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SymbolData {
    pub symbol: String,
    pub base_asset_precision: u32,
    pub quote_asset_precision: u32,
    pub filters: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExchangeInfoBinance {
    pub symbols: Vec<SymbolData>,
}
impl ExchangeInfoBinance {
    fn symbol(&self) -> Result<&SymbolData> {
        let symbol = self.symbols.first().context("failed to get symbol")?;
        Ok(symbol)
    }

    pub async fn fetch(url: Url, symbol: &Symbol) -> Result<Self> {
        let mut endpoint = url.join("exchangeInfo").unwrap();
        endpoint
            .query_pairs_mut()
            .append_pair("symbol", &symbol.to_string())
            .finish();

        let exchange_info = reqwest::get(endpoint)
            .await
            .context("Failed to get exchange info")?
            .json::<Self>()
            .await
            .context("Failed to deserialize exchange info to json")?;
        Ok(exchange_info)
    }

    fn scale_price(&self) -> Result<u32> {
        let tick_sizes = self
            .symbol()?
            .filters
            .iter()
            .filter_map(|filter| {
                let filter_obj = filter.as_object()?;
                if let Some(filter_type) = filter["filterType"].as_str() {
                    if filter_type == "PRICE_FILTER" {
                        filter_obj["tickSize"].as_str()
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<&str>>();

        let tick_size_str = tick_sizes.first().context("Failed to get tick size")?;

        let scale_price = Decimal::from_str(tick_size_str)
            .context("Failed to parse tick size")?
            .normalize()
            .scale();

        Ok(scale_price)
    }

    fn scale_quantity(&self) -> Result<u32> {
        let scale_quantity = self.symbol()?.base_asset_precision.min(8);

        Ok(scale_quantity)
    }

    pub async fn fetch_scales(url: Url, symbol: &Symbol) -> Result<(u32, u32)> {
        let exinfo = Self::fetch(url, symbol).await?;
        let scale_price = exinfo.scale_price()?;
        let scale_quantity = exinfo.scale_quantity()?;
        Ok((scale_price, scale_quantity))
    }
}
