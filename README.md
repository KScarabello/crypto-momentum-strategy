Crypto Momentum Research

Python research framework for developing and evaluating systematic crypto trading strategies using historical OHLCV data.

The repository focuses on reproducible research workflows, execution-aware backtesting, and clean separation between research logic and deployment-specific configuration.

Project Purpose

This repository is designed for strategy research and validation, not production execution.

Core goals:

Evaluate systematic allocation strategies on historical market data
Incorporate realistic execution assumptions (costs, slippage, turnover)
Enable reproducible local backtests from persisted datasets

The codebase emphasizes clarity, modularity, and realistic assumptions over breadth or feature completeness.

Strategy Overview

At a high level, the research workflow:

Compute signals across multiple time horizons
Combine signals into a composite ranking
Select a subset of assets based on relative strength
Optionally apply a market-state filter
Construct a portfolio subject to basic constraints
Rebalance periodically

Backtests are designed to reflect practical constraints, including:

Long-only portfolio construction
No leverage or shorting
Transaction costs and optional slippage
Turnover-aware rebalancing

Specific production parameters and configurations are intentionally excluded from this public repository.

Repository Structure
config.py
Public-safe configuration defaults (demo values only).
Real research parameters are loaded from a local config_private.py (not tracked).
data/fetch_ohlc.py
Local-first OHLCV loader, validation utilities, and downloader integrations
data/download_ohlcv.py
Script for downloading and persisting historical data locally
strategy/
Signal construction, ranking, and filtering logic
backtest/engine.py
Portfolio simulation engine
backtest/metrics.py
Performance and risk metrics
research/run_backtest.py
End-to-end research script
tests/
Focused unit tests for core components
Configuration

The repo uses a two-layer configuration approach:

config.py → public-safe defaults
config_private.py → local, non-public research parameters

If config_private.py is present, it overrides the public defaults at runtime.

This design keeps proprietary research parameters out of version control.

Data

By default, the system reads local OHLCV data from:

data/local/

Expected columns:

timestamp
open
high
low
close
volume
symbol

Loader behavior:

Prefers local files (.csv or .parquet)
Normalizes schema
Parses timestamps as UTC
Deduplicates rows
Returns a unified dataset across symbols
Downloading Historical Data

Run:

python -m data.download_ohlcv

Supports configurable data providers and exchange integrations.

Downloader features:

Batched historical requests
Local persistence per symbol
Merge + deduplication
Configurable limits and safety guards

Recommended workflow:

Download and persist local data
Run research exclusively on local datasets
Running a Backtest

Run:

python -m research.run_backtest

This will:

Load configuration
Load historical data
Execute the backtest
Output summary metrics
Save result artifacts
Outputs

Written to output_dir:

equity_curve.csv
rebalance_log.csv
holdings_history.csv
Scope

This repository intentionally excludes:

Live trading systems
Broker integrations
Streaming pipelines
Production infrastructure
Large-scale parameter optimization
Notes

This project reflects an execution-aware approach to systematic strategy research, with emphasis on:

realistic assumptions
reproducibility
modular design

It is intended as a research tool, not a finished trading system.