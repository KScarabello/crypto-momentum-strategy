"""Momentum scoring, ranking, and BTC regime filtering."""

from __future__ import annotations

import pandas as pd


def compute_return_over_lookback(close: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    """Compute percentage return over a trailing lookback window."""
    if lookback_bars <= 0:
        raise ValueError("lookback_bars must be positive")
    if close.empty:
        raise ValueError("close price matrix is empty")
    return close.pct_change(periods=lookback_bars)


def compute_momentum_score(
    close: pd.DataFrame,
    short_lookback_bars: int,
    medium_lookback_bars: int,
    short_weight: float = 0.5,
    medium_weight: float = 0.5,
) -> pd.DataFrame:
    """Compute weighted momentum score from short and medium lookback returns."""
    if short_lookback_bars <= 0 or medium_lookback_bars <= 0:
        raise ValueError("lookback values must be positive")
    if short_weight < 0 or medium_weight < 0:
        raise ValueError("weights must be non-negative")

    weight_sum = short_weight + medium_weight
    if weight_sum <= 0:
        raise ValueError("sum of weights must be positive")

    normalized_short = short_weight / weight_sum
    normalized_medium = medium_weight / weight_sum

    short_ret = compute_return_over_lookback(close, lookback_bars=short_lookback_bars)
    medium_ret = compute_return_over_lookback(close, lookback_bars=medium_lookback_bars)

    score = (normalized_short * short_ret) + (normalized_medium * medium_ret)
    return score


def rank_symbols_for_date(
    momentum_score: pd.DataFrame,
    rebalance_timestamp: pd.Timestamp,
    top_n: int,
) -> list[str]:
    """Rank symbols by momentum at one timestamp and return top selections."""
    if top_n <= 0:
        raise ValueError("top_n must be positive")

    if rebalance_timestamp not in momentum_score.index:
        raise KeyError(f"rebalance_timestamp not found in score index: {rebalance_timestamp}")

    row = momentum_score.loc[rebalance_timestamp]
    ranked = row.dropna().sort_values(ascending=False)
    return ranked.head(top_n).index.tolist()


def check_regime_filter(
    close: pd.DataFrame,
    rebalance_timestamp: pd.Timestamp,
    btc_symbol: str = "BTC/USD",
    ma_lookback_bars: int = 30,
) -> bool:
    """Return True when BTC is above its moving average at the rebalance timestamp."""
    if ma_lookback_bars <= 0:
        raise ValueError("ma_lookback_bars must be positive")
    if btc_symbol not in close.columns:
        raise KeyError(f"BTC symbol not found in close matrix: {btc_symbol}")
    if rebalance_timestamp not in close.index:
        raise KeyError(f"rebalance_timestamp not found in close index: {rebalance_timestamp}")

    btc_close = close[btc_symbol]
    btc_ma = btc_close.rolling(window=ma_lookback_bars, min_periods=ma_lookback_bars).mean()

    current_close = btc_close.loc[rebalance_timestamp]
    current_ma = btc_ma.loc[rebalance_timestamp]

    if pd.isna(current_close) or pd.isna(current_ma):
        return False

    return bool(current_close > current_ma)


def compute_momentum(close: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    """Backward-compatible alias for single-lookback momentum return."""
    return compute_return_over_lookback(close=close, lookback_bars=lookback_bars)


def select_top_n(momentum_row: pd.Series, top_n: int) -> list[str]:
    """Backward-compatible selection helper from one momentum row."""
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    ranked = momentum_row.dropna().sort_values(ascending=False)
    return ranked.head(top_n).index.tolist()
