"""Public-safe project configuration defaults.

This file is intentionally safe for open-source publication and includes
demo placeholders only.

Private production or proprietary research parameters should live in
config_private.py (gitignored). If present, config_private.SETTINGS will
override the public defaults below at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Configuration values for demo/public usage."""

    # Demo placeholders only (not production universe)
    symbols: tuple[str, ...] = ("ASSET_A/USD", "ASSET_B/USD", "ASSET_C/USD")
    timeframe: str = "demo_timeframe"

    top_n: int = 2
    short_lookback_bars: int = 12
    medium_lookback_bars: int = 36
    short_weight: float = 0.5
    medium_weight: float = 0.5

    use_regime_filter: bool = False
    max_position_weight: float | None = None
    max_gross_exposure: float | None = None
    # Kept for compatibility with existing code paths.
    btc_symbol: str = "BENCHMARK/USD"
    regime_lookback_bars: int = 30
    rebalance_every_bars: int = 1
    rebalance_hour_utc: int = 0

    transaction_cost_bps: float = 10.0
    slippage_bps: float = 5.0
    initial_capital: float = 10_000.0
    min_history_bars: int | None = 36
    min_eligible_assets: int = 1
    min_median_volume: float | None = None
    max_turnover_per_rebalance: float | None = None

    data_dir: Path = Path("data/local")
    use_downloader: bool = False
    historical_data_provider: str = "demo_provider"
    historical_fallback_provider: str | None = None
    historical_exchange_name: str | None = None
    historical_since: str | None = None
    historical_max_batches: int | None = 5
    historical_max_rows: int | None = None
    historical_limit_per_request: int = 500
    historical_request_pause_seconds: float = 0.1
    historical_overwrite: bool = False
    output_dir: Path = Path("outputs/demo")


SETTINGS = Settings()

# Optional private override: if config_private.py exists, use its SETTINGS.
# This keeps real/proprietary parameters out of the public repository.
try:
    from config_private import SETTINGS as PRIVATE_SETTINGS
except ModuleNotFoundError as exc:
    if exc.name != "config_private":
        raise
    PRIVATE_SETTINGS = None

if PRIVATE_SETTINGS is not None:
    SETTINGS = PRIVATE_SETTINGS
