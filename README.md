# Market Data Downloader

A small Rust CLI to download market data aggregates from providers such as [Polygon.io](https://polygon.io/) and [Twelve Data](https://twelvedata.com/). It supports downloading minute or daily aggregates for equities, indices, and more, and saving them as CSV or JSON.

Data providers:
- Polygon.io (default) — env: POLYGON_API_KEY
- Twelve Data — env: TWELVEDATA_API_KEY

Select provider with `--provider polygon` (default) or `--provider twelvedata`.

## Prerequisites
- Rust and Cargo installed (https://rustup.rs)
- API key for the provider you intend to use:
  - Polygon.io: provide via `--apikey` or set `POLYGON_API_KEY`.
  - Twelve Data: provide via `--apikey` or set `TWELVEDATA_API_KEY`.

## Clone
```
git clone https://github.com/marceloneppel/market-data-downloader.git
cd market-data-downloader
```

## Build
- Debug build:
```
cargo build
```
- Release build:
```
cargo build --release
```

## Run
The binary name is `market-data-downloader`.

Basic examples:

- CSV output (default), explicit API key flag:
```
cargo run -- \
  download \
  --apikey YOUR_POLYGON_KEY \
  --ticker AAPL \
  --from 2024-01-01 \
  --to 2024-01-03 \
  --out aapl.csv
```

- JSON output using environment variable for API key (Polygon):
```
POLYGON_API_KEY=YOUR_POLYGON_KEY \
  cargo run -- \
  download \
  -t I:NDX \
  -f 2024-02-01 \
  -T 2024-02-01 \
  --format json
```
This will save output to `output/I:NDX_2024-02-01_2024-02-01.json` by default.

- Twelve Data example (daily AAPL):
```
TWELVEDATA_API_KEY=YOUR_TWELVEDATA_KEY \
  cargo run -- \
  download \
  -t AAPL \
  -f 2025-01-02 \
  -T 2025-01-02 \
  --granularity day \
  --format csv \
  --provider twelvedata
```

- Minute vs Day granularity (default is minute):
```
cargo run -- download -t AAPL -f 2024-01-01 -T 2024-01-02 --granularity day --apikey YOUR_POLYGON_KEY
```

- Split output by day into per-file CSVs under `output/YYYY/MM`:
```
cargo run -- download -t I:NDX -f 2025-01-01 -T 2025-01-05 --split-by-day --apikey YOUR_POLYGON_KEY
```

- Omit CSV header and limit decimal places:
```
cargo run -- download -t AAPL -f 2025-01-01 -T 2025-01-01 --no-header --max-decimals 4 --apikey YOUR_POLYGON_KEY
```

- Increase verbosity and respect free-tier rate limits (default wait ~12s when paging):
```
cargo run -- download -t AAPL -f 2025-01-01 -T 2025-01-07 -v --rate-limit-wait-secs 12 --apikey YOUR_POLYGON_KEY
```

Notes:
- If `--out` is not specified, files are written under the `output/` directory with an auto-generated name, for example: `output/AAPL_2024-01-01_2024-01-03.csv`.
- For JSON output, the tool writes a single JSON array unless `--split-by-day` is used (which currently supports CSV only).

## Tests
Run unit and integration tests:
```
cargo test
```

The CI configuration is available under `.github/workflows/ci.yml`.

## License
This project is licensed under the terms of the LICENSE file in the repository.
