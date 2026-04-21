"""Tests for momentum scoring, ranking, and regime filter."""

from __future__ import annotations

import pandas as pd
import pytest

from strategy.momentum import (
    check_regime_filter,
    compute_momentum_score,
    rank_symbols_for_date,
)


def test_compute_momentum_score_on_synthetic_data() -> None:
    index = pd.date_range("2024-01-01", periods=5, freq="D", tz="UTC")
    close = pd.DataFrame(
        {
            "BTC/USD": [100.0, 110.0, 121.0, 133.1, 146.41],
            "ETH/USD": [100.0, 100.0, 100.0, 100.0, 100.0],
        },
        index=index,
    )

    score = compute_momentum_score(
        close=close,
        short_lookback_bars=1,
        medium_lookback_bars=2,
        short_weight=0.5,
        medium_weight=0.5,
    )

    last = index[-1]
    assert score.loc[last, "BTC/USD"] == pytest.approx(0.155)
    assert score.loc[last, "ETH/USD"] == pytest.approx(0.0)


def test_rank_symbols_for_date_selects_strongest() -> None:
    ts = pd.Timestamp("2024-01-10", tz="UTC")
    momentum_score = pd.DataFrame(
        [{"BTC/USD": 0.3, "ETH/USD": 0.1, "SOL/USD": 0.5}], index=[ts]
    )

    selected = rank_symbols_for_date(momentum_score, rebalance_timestamp=ts, top_n=2)
    assert selected == ["SOL/USD", "BTC/USD"]


def test_check_regime_filter_false_when_btc_below_moving_average() -> None:
    index = pd.date_range("2024-01-01", periods=4, freq="D", tz="UTC")
    close = pd.DataFrame(
        {
            "BTC/USD": [10.0, 10.0, 10.0, 9.0],
            "ETH/USD": [5.0, 5.1, 5.2, 5.3],
        },
        index=index,
    )

    is_risk_on = check_regime_filter(
        close=close,
        rebalance_timestamp=index[-1],
        btc_symbol="BTC/USD",
        ma_lookback_bars=3,
    )
    assert is_risk_on is False
