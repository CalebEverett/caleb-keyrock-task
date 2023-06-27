# terminal-app

A simple terminal app that allows you to display a summary of the bids and asks for a given cryptocurrency pair, along with a graph of the spread over time.

The app is based on [Rust: Playing with tui-rs](https://www.monkeypatch.io/blog/2021-05-31-rust-tui).

## Running the app

### Server

To start the server, run:
`cargo run --bin server`

### App

A command line interface is provided to run the app. It takes the following arguments:

```
--symbol
--levels
--price-range
--decimals
```

The symbol is the currency pair to be included in the orderbook. Levels is the number of levels to be included in the summary. The price range is a float that is a percentage around the starting best ask price the order book will be maintained (i.e. if the current best ask price is 100 and you specify 5.0, the max orderbook price will be 105 and the minimum will be 95). Decimals is the number of decimal places to include in the prices. The order book is managed without decimals - asset quantities are represented as u64 up to 8 decicmal places and prices are represented as u32. The decimals is used to convert the prices to u32 for storage and to convert back to f64 for display.

The following command would run the app for BTCUSD with 10 levels, a price range of 3.0 and 2 decimals:

```
cargo run --bin terminal-app -- --symbol BTCUSD --levels 10 --price-range 3 --decimals 2
```

You can get valid symbols by running:

```
cargo run --bin client -- get-symbols
```

![teminal app screen](./screen.png)
