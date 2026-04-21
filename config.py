"""Project configuration for momentum research runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Configuration values for a single momentum rotation backtest."""

    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD", "SOL/USD", "AVAX/USD", "XRP/USD")
    timeframe: str = "4h"

    top_n: int = 3
    short_lookback_bars: int = 42
    medium_lookback_bars: int = 180
    short_weight: float = 0.5
    medium_weight: float = 0.5

    btc_symbol: str = "BTC/USD"
    regime_lookback_bars: int = 180
    rebalance_every_bars: int = 6

    transaction_cost_bps: float = 10.0
    slippage_bps: float = 2.0
    initial_capital: float = 10_000.0
    min_history_bars: int | None = 180
    min_eligible_assets: int = 2
    min_median_volume: float | None = None
    max_turnover_per_rebalance: float | None = 1.0

    data_dir: Path = Path("data/local")
    use_downloader: bool = False
    historical_data_provider: str = "cryptocompare"
    historical_fallback_provider: str | None = "kraken"
    historical_exchange_name: str | None = None
    historical_since: str | None = None
    historical_max_batches: int | None = 50
    historical_max_rows: int | None = None
    historical_limit_per_request: int = 2000
    historical_request_pause_seconds: float = 0.15
    historical_overwrite: bool = False
    output_dir: Path = Path("outputs")


SETTINGS = Settings()
