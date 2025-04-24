### Trades Proxy Server

This is a simple proxy API server for trades on an orderbook. It implements a caching strategy combining time-based bucketing, prefetching, and LRU caching.

## Bottlenecks

- Memory spikes during trading volume spikes
- Random/highly dispersed query patterns = LRU less effective

## Future Enhancements

- Dynamic bucket sizing
- Cache query results
- Background prefetching in separate thread
- Binary search within buckets
- Run different caching strategies in parallel and pick the best for each query

## Running the Code

To run the program with the provided input:

```bash
cargo run < test_input.txt
```

For the full input file:

```bash
cargo run < input.txt
```


To run with logs:

```bash
# Show only errors
RUST_LOG=error cargo run < test_input.txt

# Show warnings and errors
RUST_LOG=warn cargo run < test_input.txt

# Show info, warnings, and errors
RUST_LOG=info cargo run < test_input.txt

# Show all information including debug messages
RUST_LOG=debug cargo run < test_input.txt

# Show everything including trace messages
RUST_LOG=trace cargo run < test_input.txt
```