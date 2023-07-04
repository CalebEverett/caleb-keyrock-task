use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_aux::field_attributes::deserialize_number_from_string;
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
pub struct Snapshot {
    #[serde(
        alias = "microtimestamp",
        deserialize_with = "deserialize_number_from_string"
    )]
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
pub struct BookUpdateData {
    #[serde(
        alias = "microtimestamp",
        deserialize_with = "deserialize_number_from_string"
    )]
    pub last_update_id: u64,
    #[serde(deserialize_with = "from_str")]
    pub bids: Vec<[DisplayPrice; 2]>,
    #[serde(deserialize_with = "from_str")]
    pub asks: Vec<[DisplayPrice; 2]>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BookUpdate {
    data: BookUpdateData,
}

impl Update for BookUpdate {
    fn validate(&self, _: u64) -> Result<()> {
        Ok(())
    }
    fn last_update_id(&self) -> u64 {
        self.data.last_update_id
    }
    fn bids_mut(&mut self) -> &mut Vec<[DisplayPrice; 2]> {
        &mut self.data.bids
    }

    fn asks_mut(&mut self) -> &mut Vec<[DisplayPrice; 2]> {
        &mut self.data.asks
    }
}

impl TryFrom<Message> for BookUpdate {
    type Error = anyhow::Error;
    fn try_from(item: Message) -> Result<Self> {
        match serde_json::from_slice::<Self>(&item.into_data()) {
            Ok(update) => Ok(update),
            Err(e) => {
                tracing::error!("Failed to deserialize update: {}", e);
                bail!("failed to deserialize update")
            }
        }
    }
}

impl From<Snapshot> for BookUpdate {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            data: BookUpdateData {
                last_update_id: snapshot.last_update_id,
                bids: snapshot.bids,
                asks: snapshot.asks,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct BestPrice {
    pub bid: Decimal,
    pub ask: Decimal,
}

impl BestPrice {
    pub(super) async fn fetch(url: Url) -> Result<Self> {
        let price = reqwest::get(url)
            .await
            .context("Failed to get price")?
            .json::<Self>()
            .await
            .context("Failed to deserialize best price")?;
        Ok(price)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SymbolData {
    pub url_symbol: String,
    pub base_decimals: u32,
    pub counter_decimals: u32,
    pub instant_order_counter_decimals: u32,
}

#[derive(Debug)]
pub(super) struct ExchangeInfoBitstamp {
    pub symbol: SymbolData,
}

impl ExchangeInfoBitstamp {
    fn symbol(&self) -> Result<&SymbolData> {
        Ok(&self.symbol)
    }

    pub async fn fetch(url: Url, symbol: &Symbol) -> Result<Self> {
        let endpoint = url.join("trading-pairs-info").unwrap();

        // let symbols = reqwest::get(endpoint.clone())
        //     .await
        //     .context("Failed to get exchange info")?
        //     .text()
        //     .await
        //     .context("Failed to deserialize exchange info to json")?;

        // println!("{}", symbols);

        let symbols = reqwest::get(endpoint)
            .await
            .context("Failed to get exchange info")?
            .json::<Vec<SymbolData>>()
            .await
            .context("Failed to deserialize exchange info to json")?;

        let symbol = symbols
            .into_iter()
            .filter(|s| s.url_symbol == symbol.to_string().to_lowercase())
            .next()
            .context("Failed to get symbol")?;

        Ok(ExchangeInfoBitstamp { symbol })
    }

    fn scale_price(&self) -> Result<u32> {
        let scale_price = self.symbol()?.counter_decimals;
        Ok(scale_price)
    }

    fn scale_quantity(&self) -> Result<u32> {
        let scale_quantity = self.symbol()?.base_decimals;

        Ok(scale_quantity)
    }

    pub async fn fetch_scales(url: Url, symbol: &Symbol) -> Result<(u32, u32)> {
        let exinfo = Self::fetch(url, symbol).await?;
        let scale_price = exinfo.scale_price()?;
        let scale_quantity = exinfo.scale_quantity()?;
        Ok((scale_price, scale_quantity))
    }
}
