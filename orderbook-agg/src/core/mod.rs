//! Contains [Orderbook](order_book::OrderBook) struct and [ExchangeBook](exchange_book::ExchangeBook) trait
//! that get implemented for each [Exchange](crate::Exchange) - [Symbol](crate::Symbol) combination
pub mod exchange_book;
pub mod num_types;
pub mod order_book;
