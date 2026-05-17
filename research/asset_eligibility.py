"""Reusable asset eligibility utilities for research universes."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def _validate_close_matrix(close: pd.DataFrame) -> pd.DataFrame:
    """Validate close-price matrix shape and return float-typed frame."""
    if not isinstance(close, pd.DataFrame):
        raise TypeError("close must be a pandas DataFrame")
    if close.empty:
        raise ValueError("close matrix is empty")
    if close.columns.empty:
        raise ValueError("close matrix must include symbol columns")
    return close.sort_index().astype(float)


def first_valid_timestamp_by_symbol(close: pd.DataFrame) -> pd.Series:
    """Return first non-null close timestamp for each symbol."""
    prices = _validate_close_matrix(close)
    out: dict[str, pd.Timestamp | pd.NaT] = {}
    for symbol in prices.columns:
        valid_index = prices[symbol].dropna().index
        out[str(symbol)] = valid_index.min() if len(valid_index) > 0 else pd.NaT
    return pd.Series(out, dtype="datetime64[ns, UTC]")


def latest_valid_timestamp_by_symbol(close: pd.DataFrame) -> pd.Series:
    """Return latest non-null close timestamp for each symbol."""
    prices = _validate_close_matrix(close)
    out: dict[str, pd.Timestamp | pd.NaT] = {}
    for symbol in prices.columns:
        valid_index = prices[symbol].dropna().index
        out[str(symbol)] = valid_index.max() if len(valid_index) > 0 else pd.NaT
    return pd.Series(out, dtype="datetime64[ns, UTC]")


def build_eligibility_mask(
    close: pd.DataFrame,
    required_lookback_bars: int,
    signal: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build timestamp x symbol eligibility mask.

    A symbol is eligible at timestamp t when:
    1) close[t, symbol] is observed (non-null)
    2) sufficient history exists to support the required lookback
    3) the relevant signal at t is non-null
    """
    if required_lookback_bars <= 0:
        raise ValueError("required_lookback_bars must be positive")

    prices = _validate_close_matrix(close)
    valid_price = prices.notna()
    history_count = valid_price.astype(int).cumsum()

    enough_history = history_count >= (int(required_lookback_bars) + 1)
    if signal is None:
        signal_frame = prices.pct_change(periods=required_lookback_bars)
    else:
        signal_frame = signal.reindex(prices.index).reindex(columns=prices.columns)
    signal_ok = signal_frame.notna()

    return (valid_price & enough_history & signal_ok).astype(bool)


def first_eligible_timestamp_by_symbol(
    close: pd.DataFrame,
    required_lookback_bars: int,
    signal: pd.DataFrame | None = None,
) -> pd.Series:
    """Return first eligible timestamp for each symbol under the given lookback/signal."""
    mask = build_eligibility_mask(
        close=close,
        required_lookback_bars=required_lookback_bars,
        signal=signal,
    )
    out: dict[str, pd.Timestamp | pd.NaT] = {}
    for symbol in mask.columns:
        idx = mask.index[mask[symbol]]
        out[str(symbol)] = idx.min() if len(idx) > 0 else pd.NaT
    return pd.Series(out, dtype="datetime64[ns, UTC]")


def eligibility_summary_table(
    close: pd.DataFrame,
    strategy_lookback_bars: int = 336,
    signal_lookbacks: Iterable[int] = (42, 84, 336),
) -> pd.DataFrame:
    """Build eligibility summary table with per-symbol inception and lookback eligibility."""
    prices = _validate_close_matrix(close)

    first_ts = first_valid_timestamp_by_symbol(prices)
    latest_ts = latest_valid_timestamp_by_symbol(prices)
    row_count = prices.notna().sum(axis=0)

    first_eligible_by_lookback: dict[int, pd.Series] = {}
    for lookback in signal_lookbacks:
        first_eligible_by_lookback[int(lookback)] = first_eligible_timestamp_by_symbol(
            close=prices,
            required_lookback_bars=int(lookback),
        )

    first_eligible_strategy = first_eligible_timestamp_by_symbol(
        close=prices,
        required_lookback_bars=int(strategy_lookback_bars),
    )

    rows: list[dict[str, object]] = []
    for symbol in prices.columns:
        symbol_key = str(symbol)
        row: dict[str, object] = {
            "symbol": symbol_key,
            "first_timestamp": first_ts.get(symbol_key, pd.NaT),
            "latest_timestamp": latest_ts.get(symbol_key, pd.NaT),
            "row_count": int(row_count.get(symbol, 0)),
            "first_eligible_timestamp_for_1w_signal": first_eligible_by_lookback.get(42, pd.Series(dtype="datetime64[ns, UTC]")).get(symbol_key, pd.NaT),
            "first_eligible_timestamp_for_2w_signal": first_eligible_by_lookback.get(84, pd.Series(dtype="datetime64[ns, UTC]")).get(symbol_key, pd.NaT),
            "first_eligible_timestamp_for_8w_signal": first_eligible_by_lookback.get(336, pd.Series(dtype="datetime64[ns, UTC]")).get(symbol_key, pd.NaT),
            "first_eligible_timestamp_for_strategy": first_eligible_strategy.get(symbol_key, pd.NaT),
            "is_eligible_for_strategy": bool(pd.notna(first_eligible_strategy.get(symbol_key, pd.NaT))),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def apply_eligibility_mask(target_weights: pd.DataFrame, eligibility_mask: pd.DataFrame) -> pd.DataFrame:
    """Zero out ineligible target weights while preserving original shape and index/columns."""
    if not isinstance(target_weights, pd.DataFrame):
        raise TypeError("target_weights must be a pandas DataFrame")
    if not isinstance(eligibility_mask, pd.DataFrame):
        raise TypeError("eligibility_mask must be a pandas DataFrame")

    aligned_mask = eligibility_mask.reindex(target_weights.index).reindex(columns=target_weights.columns).fillna(False)
    return target_weights.where(aligned_mask, 0.0)
