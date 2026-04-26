# Quant Research Backtesting Framework

Lightweight Python framework for **systematic trading research** with emphasis on:

- **Execution-aware backtesting**
- **Reproducibility**
- **Modular architecture**

This repository is designed for research workflows and strategy evaluation, not direct production deployment.

---

## Purpose

This project provides a concise pipeline to:

- Load and validate historical OHLCV market data  
- Generate portfolio targets from configurable research signals  
- Simulate strategy behavior under realistic execution assumptions  
- Produce repeatable diagnostics and output artifacts  

Core principles:

- **Deterministic runs**
- **Transparent experiment flow**
- **Clear separation of concerns** across data, strategy, simulation, and reporting  

---

## Strategy Overview

At a conceptual level, the framework supports:

- **Signal generation** from historical observations  
- **Periodic rebalancing** under configurable scheduling  
- **Portfolio constraints** and optional risk controls  
- **Execution-friction modeling** (for example, transaction costs and slippage)  

Specific production parameters and proprietary configurations are intentionally excluded from this public repository.

---

## Repository Structure

- `config.py`  
  Public-safe defaults and configuration entrypoint  

- `data/fetch_ohlc.py`  
  Data loading, normalization, and validation utilities  

- `data/download_ohlcv.py`  
  Historical data download helper  

- `strategy/`  
  Signal construction and ranking components  

- `backtest/engine.py`  
  Execution-aware simulation engine  

- `backtest/metrics.py`  
  Performance and diagnostics calculations  

- `research/`  
  End-to-end research scripts and experiment runners  

- `live/`  
  Execution orchestration and scheduling scaffolding  

- `tests/`  
  Unit tests for strategy, engine, and metrics behavior  

---

## Configuration

Configuration loading supports two layers:

1. `config.py` → public-safe demo defaults  
2. `config_private.py` → local, non-public overrides  

Runtime behavior:

- If `config_private.py` exists, its `SETTINGS` override public defaults  
- Otherwise, the repository uses `config.py` defaults  

This pattern preserves runtime compatibility while keeping sensitive parameters out of source control.

---

## Data Requirements

Default local data path:

- `data/local/`

Expected OHLCV columns:

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `symbol`

Loader behavior:

- Local-first ingestion  
- UTC timestamp normalization  
- Duplicate handling on `(symbol, timestamp)`  
- Multi-asset merge for downstream analysis  

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Running Research

Run the primary backtest workflow:

```bash
python -m research.run_backtest
```

Optional data refresh:

```bash
python -m data.download_ohlcv
```

---

## Outputs

Research runs write artifacts to the configured output directory, typically including:

- Equity curve time series  
- Rebalance/event logs  
- Holdings history  
- Experiment summary tables (where applicable)  

---

## Reproducibility Notes

For consistent, repeatable results:

- Pin dependencies in `requirements.txt`  
- Keep datasets immutable per experiment window  
- Store output artifacts alongside run metadata  
- Track configuration snapshots for each experiment  

---

## Audience

Built for **quant research** and **systematic trading** practitioners seeking a compact, auditable research framework.