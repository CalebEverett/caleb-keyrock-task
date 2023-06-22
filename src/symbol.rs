use crate::booksummary::{ExchangeType, Symbols};
use futures::future::try_join_all;
use std::collections::HashSet;
use strum::IntoEnumIterator;
use tonic::Status;

/// Gets available symbols for a given exchange.
pub async fn get_symbols(
    exchange: ExchangeType,
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let symbols = match exchange {
        ExchangeType::Binance => reqwest::get("https://api.binance.us/api/v3/exchangeInfo")
            .await?
            .json::<serde_json::Value>()
            .await?["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .map(|symbol| symbol["symbol"].as_str().unwrap().to_string())
            .collect::<HashSet<String>>(),
        ExchangeType::Bitstamp => reqwest::get("https://www.bitstamp.net/api/v2/ticker/")
            .await?
            .json::<serde_json::Value>()
            .await?
            .as_array()
            .unwrap()
            .iter()
            .map(|symbol| symbol["pair"].as_str().unwrap().replace("/", ""))
            .collect::<HashSet<String>>(),
    };
    Ok(symbols)
}

/// Gets symbols available on all exchanges.
pub async fn get_symbols_all() -> Result<Symbols, Box<dyn std::error::Error + Send + Sync>> {
    let symbols_vec = try_join_all(
        ExchangeType::iter()
            .map(|exchange| get_symbols(exchange))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("couldn't get symbols");

    let exchange_names: Vec<&str> = ExchangeType::iter().map(|e| e.as_str_name()).collect();
    let symbols_intersection: HashSet<String> =
        symbols_vec
            .into_iter()
            .enumerate()
            .fold(HashSet::new(), |acc, (i, set)| {
                tracing::info!(exchange = &exchange_names[i], symbols_count = set.len());
                if acc.is_empty() {
                    set
                } else {
                    acc.intersection(&set).map(|s| s.to_string()).collect()
                }
            });

    let mut symbols: Vec<String> = symbols_intersection.into_iter().collect();

    symbols.sort();

    tracing::info!(
        exchanges = format!("{}", exchange_names.join(", ")),
        symbols_count = symbols.len()
    );

    Ok(Symbols { symbols })
}

/// Validates a symbol is available on all exchanges.
pub async fn validate_symbol(symbol: &String) -> Result<(), Status> {
    let symbols = get_symbols_all().await.expect("Failed to get symbols");
    if !symbols.symbols.iter().any(|s| s == symbol) {
        tracing::error!("Symbol {} not found on one or more exchanges", symbol);
        return Err(Status::not_found("Symbol not found"));
    }
    Ok(())
}
