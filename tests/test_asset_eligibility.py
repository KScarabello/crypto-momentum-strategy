"""Tests for research asset eligibility helpers."""

from __future__ import annotations

import pandas as pd

from research.asset_eligibility import (
    apply_eligibility_mask,
    build_eligibility_mask,
    eligibility_summary_table,
    first_eligible_timestamp_by_symbol,
)


def test_first_eligible_timestamp_uses_required_lookback() -> None:
    idx = pd.date_range("2024-01-01", periods=7, freq="4h", tz="UTC")
    close = pd.DataFrame(
        {
            "BTC/USD": [float("nan"), float("nan"), 100.0, 101.0, 102.0, 103.0, 104.0],
        },
        index=idx,
    )

    first = first_eligible_timestamp_by_symbol(close=close, required_lookback_bars=2)
    assert first["BTC/USD"] == idx[4]


def test_symbols_with_fewer_than_336_bars_are_ineligible_for_strategy() -> None:
    idx = pd.date_range("2024-01-01", periods=100, freq="4h", tz="UTC")
    close = pd.DataFrame({"BTC/USD": [100.0 + i for i in range(100)]}, index=idx)

    summary = eligibility_summary_table(close=close, strategy_lookback_bars=336)
    row = summary.loc[summary["symbol"] == "BTC/USD"].iloc[0]
    assert row["row_count"] == 100
    assert bool(row["is_eligible_for_strategy"]) is False


def test_no_early_prices_are_backfilled_before_inception_and_mask_is_false_before_eligibility() -> None:
    idx = pd.date_range("2024-01-01", periods=6, freq="4h", tz="UTC")
    close = pd.DataFrame(
        {
            "NEW/USD": [float("nan"), float("nan"), 10.0, 11.0, 12.0, 13.0],
        },
        index=idx,
    )

    mask = build_eligibility_mask(close=close, required_lookback_bars=2)
    assert pd.isna(close.loc[idx[0], "NEW/USD"])
    assert pd.isna(close.loc[idx[1], "NEW/USD"])
    assert bool(mask.loc[idx[0], "NEW/USD"]) is False
    assert bool(mask.loc[idx[1], "NEW/USD"]) is False
    assert bool(mask.loc[idx[4], "NEW/USD"]) is True


def test_apply_eligibility_mask_zeros_out_ineligible_target_weights() -> None:
    idx = pd.date_range("2024-01-01", periods=3, freq="4h", tz="UTC")
    weights = pd.DataFrame(
        {
            "BTC/USD": [0.5, 0.5, 0.5],
            "ETH/USD": [0.5, 0.5, 0.5],
        },
        index=idx,
    )
    mask = pd.DataFrame(
        {
            "BTC/USD": [True, False, True],
            "ETH/USD": [False, False, True],
        },
        index=idx,
    )

    masked = apply_eligibility_mask(target_weights=weights, eligibility_mask=mask)
    assert masked.loc[idx[0], "BTC/USD"] == 0.5
    assert masked.loc[idx[0], "ETH/USD"] == 0.0
    assert masked.loc[idx[1], "BTC/USD"] == 0.0
    assert masked.loc[idx[1], "ETH/USD"] == 0.0
    assert masked.loc[idx[2], "BTC/USD"] == 0.5
    assert masked.loc[idx[2], "ETH/USD"] == 0.5
