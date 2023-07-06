# orderbook-agg

gRPC server that streams aggregate orderbook data from multiple exchanges. Orderbooks are are updated separately in background processes with summaries streamed to clients.

1. Make sure your code checks that orderbooks have received all updates (first update id in update streamed from exchange is one greater than local last uodate id).
2. Follow the instructions in the Binance API docs for how to maintain a local orderbook - buffer webstream, update from snapshot and then process webstream updates, checking per above.
3. Include tests to ensure summary is being calculated currectly, especially the spread.
4. Maintain one summary stream and make sure server can accomodate multiple requests to receive streaming updates.
5. Use the precision data provided by the exchange APIs for each currency for both base and quote assets.
6. Don't change the proto file - consider this as an established internal API.
