"""Deterministic signal generator: Dry-run target weight calculator.

This is the first deployment-stage utility for the locked baseline strategy.
It generates target weights for the current bar without placing orders.

This is not a live trading executor—it only computes and prints signal diagnostics.
Use this to verify strategy logic is working before implementing order placement.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from backtest.engine import _apply_gross_exposure_cap, _apply_position_weight_cap, _build_target_weights
from config import SETTINGS
from data.fetch_ohlc import load_ohlcv_history
from strategy.momentum import check_regime_filter, compute_momentum_score, rank_symbols_for_date


def configure_logging() -> None:
    """Configure minimal project logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# Maps timeframe strings to bar duration in hours, used for rebalance-bar detection.
_TIMEFRAME_HOURS: dict[str, int] = {
    "4h": 4,
    "1d": 24,
    "d": 24,
    "daily": 24,
}


def _is_rebalance_bar(
    timestamp: pd.Timestamp,
    timeframe: str,
    rebalance_every_bars: int,
) -> bool:
    """Return True if this bar falls on a scheduled rebalance boundary.

    Logic: rebalance_period_hours = bar_hours × rebalance_every_bars.
    A bar is a rebalance bar when its UTC hour is divisible by that period.
    For 4h × 6 = 24h this means only the 00:00 UTC bar is a rebalance bar.
    """
    bar_hours = _TIMEFRAME_HOURS.get(timeframe.strip().lower())
    if bar_hours is None:
        raise ValueError(f"Unsupported timeframe for rebalance check: {timeframe!r}")
    rebalance_period_hours = bar_hours * rebalance_every_bars
    ts_utc = timestamp.tz_convert("UTC") if timestamp.tzinfo is not None else timestamp
    return int(ts_utc.hour) % rebalance_period_hours == 0


def _is_data_fresh(
    latest_ohlcv_timestamp: pd.Timestamp,
    timeframe: str,
) -> bool:
    """Return True if the latest OHLCV bar is fresh enough for live trading.

    A bar is considered fresh if it is not older than (2 × timeframe hours).
    This prevents trading on stale signals when local data files are not refreshed.
    For 4h bars: Max age = 8 hours. For 1d bars: Max age = 48 hours.
    """
    bar_hours = _TIMEFRAME_HOURS.get(timeframe.strip().lower())
    if bar_hours is None:
        raise ValueError(f"Unsupported timeframe for freshness check: {timeframe!r}")
    now = pd.Timestamp.now(tz="UTC")
    latest_utc = (
        latest_ohlcv_timestamp.tz_convert("UTC")
        if latest_ohlcv_timestamp.tzinfo is not None
        else latest_ohlcv_timestamp
    )
    age_hours = (now - latest_utc).total_seconds() / 3600.0
    max_age_hours = bar_hours * 2.0
    return age_hours <= max_age_hours


def _close_matrix(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Pivot long-format OHLCV to a timestamp x symbol close matrix."""
    close = ohlcv.pivot(index="timestamp", columns="symbol", values="close").sort_index().astype(float)
    close = close.dropna(how="all")
    if close.empty:
        raise ValueError("close matrix is empty after pivot")
    return close


def generate_targets(
    symbols: tuple[str, ...] = SETTINGS.symbols,
    timeframe: str = SETTINGS.timeframe,
    data_dir: str | None = None,
) -> dict[str, object]:
    """Generate target weights for the locked baseline strategy at the latest bar.

    TIMING MODEL: Matches backtest signal-generation timing.
    
    This function:
    1. Uses only COMPLETED bars (loaded from static OHLCV files)
    2. Generates targets using data up to the latest completed bar
    3. Returns the signal timestamp (the decision bar, not execution bar)
    
    In the backtest engine:
    - Signal is generated at bar i using closed data from bar i
    - Signal executes at bar i+1 (one-bar delay to avoid same-bar optimism)
    
    In live trading (current implementation):
    - Signal is generated from latest completed bar using same logic
    - This signal would execute at the next bar boundary (in a queued system)
    - The signal timestamp marks WHEN THE DECISION WAS MADE, not execution time
    
    This ensures consistency: the DECISION uses the same data in both backtest and live.

    Args:
        symbols: Asset universe to use (default: locked baseline 3-asset universe)
        timeframe: OHLCV timeframe (default: locked baseline 4h)
        data_dir: Path to local data directory (default: locked baseline)

    Returns:
        Dictionary with strategy_variant, timestamp, selected_symbols, target_weights, and cash_weight.
    """
    if data_dir is None:
        data_dir = SETTINGS.data_dir

    # Load latest local OHLCV data
    ohlcv = load_ohlcv_history(
        symbols=symbols,
        timeframe=timeframe,
        data_dir=data_dir,
        downloader=None,  # Dry-run: local data only
    )

    if ohlcv.empty:
        raise ValueError("No OHLCV data loaded")

    close = _close_matrix(ohlcv)
    if close.empty:
        raise ValueError("Close matrix is empty")

    latest_ts = close.index[-1]
    data_fresh = _is_data_fresh(latest_ts, timeframe)

    # DECISION POINT: This is the signal bar (the bar where we make the decision).
    # In the backtest, this signal would execute at the NEXT bar (bar i+1).
    # We use only closed data from this bar (no forward-looking information).
    # Timestamp marks DECISION TIME, not execution time (consistent with backtest).

    # Compute momentum score
    momentum_score = compute_momentum_score(
        close=close,
        short_lookback_bars=SETTINGS.short_lookback_bars,
        medium_lookback_bars=SETTINGS.medium_lookback_bars,
        short_weight=SETTINGS.short_weight,
        medium_weight=SETTINGS.medium_weight,
    )

    # Check regime filter (though baseline has it disabled)
    risk_on = True
    if SETTINGS.use_regime_filter:
        risk_on = check_regime_filter(
            close=close,
            rebalance_timestamp=latest_ts,
            btc_symbol=SETTINGS.btc_symbol,
            ma_lookback_bars=SETTINGS.regime_lookback_bars,
        )

    # Rank symbols and select top N
    selected: list[str] = []
    if risk_on:
        ranked = rank_symbols_for_date(
            momentum_score=momentum_score,
            rebalance_timestamp=latest_ts,
            top_n=len(symbols),
        )
        selected = ranked[: SETTINGS.top_n]

    # Build target weights
    target_weights = _build_target_weights(close.columns, selected)

    # Apply position weight cap
    target_weights = _apply_position_weight_cap(
        target_weights=target_weights,
        max_position_weight=SETTINGS.max_position_weight,
    )

    # Apply gross exposure cap
    target_weights = _apply_gross_exposure_cap(
        target_weights=target_weights,
        max_gross_exposure=SETTINGS.max_gross_exposure,
    )

    # Compute implied cash weight
    total_risky = float(target_weights.sum())
    cash_weight = max(0.0, 1.0 - total_risky)

    rebalance_bar = _is_rebalance_bar(latest_ts, timeframe, SETTINGS.rebalance_every_bars)

    return {
        "strategy_variant": "locked_baseline",
        "timestamp": latest_ts,
        "selected_symbols": selected,
        "target_weights": target_weights.to_dict(),
        "cash_weight": cash_weight,
        "total_risky_weight": total_risky,
        "risk_on": risk_on,
        "is_rebalance_bar": rebalance_bar,
        "data_fresh": data_fresh,
    }


def main() -> None:
    """Load data, generate targets, and print dry-run signal."""
    configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("Dry-run signal generator (locked baseline strategy)")

    result = generate_targets()

    print("\n" + "=" * 70)
    print("STRATEGY SIGNAL GENERATION (DRY-RUN)")
    print("=" * 70)
    print(f"\nVariant:            {result['strategy_variant']}")
    print(f"Latest Timestamp:   {result['timestamp']}")
    print(f"Regime (Risk-On):   {result['risk_on']}")
    print(f"\nSelected Symbols:   {', '.join(result['selected_symbols'])}")
    print(f"\nTarget Weights:")
    for symbol, weight in result['target_weights'].items():
        pct = weight * 100.0
        print(f"  {symbol:12s}  {weight:8.6f}  ({pct:6.2f}%)")
    print(f"  {'CASH':12s}  {result['cash_weight']:8.6f}  ({result['cash_weight']*100.0:6.2f}%)")
    print(f"\nTotal Risky Weight: {result['total_risky_weight']:.6f}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
