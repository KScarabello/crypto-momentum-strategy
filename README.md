# Crypto Momentum Research

Small Python research repository for testing a medium-term crypto momentum rotation strategy on 4h or daily candles.

The code is intentionally narrow: load local historical OHLCV data, generate momentum-based allocations, run a friction-aware backtest, and export results.

## Project Purpose

This repo is for strategy research and sanity-checking assumptions, not for execution.

Current focus:
- Cross-sectional momentum rotation in a small crypto universe
- Simple BTC regime filter for risk-on/risk-off
- Reproducible local runs from CSV/parquet files

Note on history depth:
- Kraken OHLC endpoints are convenient but limited for long-horizon 4h research.
- This repo supports alternate historical providers for deeper downloads, then persists data locally.
- Default historical downloader provider is `cryptocompare` for deeper research history.

## Strategy Summary

At each rebalance timestamp:
1. Compute short and medium lookback returns.
2. Combine them into a weighted momentum score.
3. Rank symbols by score and select top N.
4. Apply a BTC regime filter: BTC close must be above its moving average.
5. If regime is on, hold selected symbols equally weighted.
6. If regime is off, hold cash.

Backtest assumptions:
- Long-only
- No leverage
- No shorting
- Proportional transaction costs
- Optional slippage

## Repository Structure

- `config.py`: Research settings (symbols, lookbacks, costs, directories)
- `data/fetch_ohlc.py`: Local-first OHLCV loader, validator, and ccxt downloader helpers
- `data/download_ohlcv.py`: Script to download and persist full local OHLCV history
- `strategy/momentum.py`: Return lookbacks, momentum score, ranking, regime filter
- `backtest/engine.py`: Rotation backtest engine and result container
- `backtest/metrics.py`: Performance and turnover metrics
- `research/run_backtest.py`: End-to-end script that runs the backtest and writes outputs
- `tests/`: Focused pytest suite for strategy, engine, and metrics

## Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Data Placement

Configure paths in `config.py`.

By default, data is read from:
- `data/local/`

Expected columns per file:
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `symbol`

Loader behavior:
- Reads local files first (`.csv` or `.parquet`)
- Normalizes column names
- Parses timestamps as UTC
- Drops duplicate `symbol` + `timestamp` rows
- Returns one combined DataFrame across configured symbols

Typical per-symbol filenames are supported, for example:
- `btc-usd_4h.csv`
- `eth-usd_4h.csv`
- `sol-usd_4h.csv`

## Download Full History

Use the downloader script to fetch historical OHLCV via the configured provider:

```bash
python -m data.download_ohlcv
```

Downloader behavior:
- Uses paginated ccxt `fetch_ohlcv` requests
- Saves one local CSV per symbol in `data_dir` (default `data/local/`)
- Merges with existing local files by default
- Deduplicates on `symbol` + `timestamp`
- Keeps rows sorted by timestamp ascending

Example output files:
- `data/local/btc-usd_4h.csv`
- `data/local/eth-usd_4h.csv`

Configure downloader controls in `config.py`:
- historical data provider (for example `cryptocompare`, `kraken`, or `binance`)
- optional exchange override
- optional since timestamp
- max batch and row safety caps
- per-request limit
- overwrite vs merge mode
- optional backtest downloader fallback (`use_downloader`)

Recommended workflow:
1. Download and persist local history.
2. Run backtests from local files.

## Run the Backtest

```bash
python -m research.run_backtest
```

The script will:
1. Load settings from `config.py`
2. Load OHLCV history for configured symbols/timeframe (local files first)
3. Run the momentum backtest
4. Print concise performance metrics
5. Save output CSV files

Local files are the preferred source for research backtests. Downloader fallback is optional and can be enabled in `config.py`.

## Output Files

Written to `output_dir` (default `outputs/`):
- `equity_curve.csv`: Timestamp and portfolio equity series
- `rebalance_log.csv`: Rebalance decisions, selected symbols, turnover, cost rate
- `holdings_history.csv`: Timestamp-by-symbol weight history

## Out of Scope (Current Version)

- Live trading or broker integrations
- Streaming data ingestion
- Databases or persistence layers beyond local files
- Portfolio optimization sweeps
- Parameter search frameworks
- Production orchestration
