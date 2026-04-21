"""Tests for backtest performance metrics."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from backtest.metrics import (
    annualized_volatility,
    cagr,
    max_drawdown,
    sharpe_ratio,
    summary_metrics,
    total_return,
    turnover_summary_stats,
)


def test_metrics_return_sane_values_for_known_inputs() -> None:
    equity = pd.Series([100.0, 110.0, 121.0])
    returns = pd.Series([0.0, 0.10, 0.10])
    turnover = pd.Series([0.0, 0.5, 0.25])

    assert total_return(equity) == pytest.approx(0.21)
    assert cagr(equity, bars_per_year=2) == pytest.approx(0.21)
    assert max_drawdown(equity) == pytest.approx(0.0)

    vol = annualized_volatility(returns, bars_per_year=2)
    sharpe = sharpe_ratio(returns, bars_per_year=2)
    assert vol >= 0.0
    assert math.isfinite(sharpe)

    tstats = turnover_summary_stats(turnover)
    assert tstats["avg_turnover"] > 0.0
    assert tstats["max_turnover"] == pytest.approx(0.5)


def test_summary_metrics_includes_turnover_when_provided() -> None:
    equity = pd.Series([100.0, 105.0, 103.0, 108.0])
    turnover = pd.Series([0.0, 0.3, 0.1, 0.2])

    metrics = summary_metrics(equity=equity, bars_per_year=365, turnover=turnover)

    assert "total_return" in metrics
    assert "cagr" in metrics
    assert "annualized_volatility" in metrics
    assert "sharpe" in metrics
    assert "max_drawdown" in metrics
    assert "avg_turnover" in metrics
    assert "total_turnover" in metrics


def test_summary_metrics_includes_realism_diagnostics() -> None:
    equity = pd.Series([100.0, 101.0, 103.0, 102.0])
    net_returns = pd.Series([0.0, 0.01, 0.02, -0.01])
    gross_returns = pd.Series([0.0, 0.012, 0.023, -0.008])
    holdings = pd.DataFrame(
        {
            "BTC/USD": [1.0, 1.0, 0.0, 0.5],
            "ETH/USD": [0.0, 0.0, 0.0, 0.5],
        }
    )
    rebalance_log = pd.DataFrame(
        {
            "signal_timestamp": [pd.Timestamp("2024-01-01", tz="UTC")],
            "execution_timestamp": [pd.Timestamp("2024-01-02", tz="UTC")],
        }
    )

    metrics = summary_metrics(
        equity=equity,
        bars_per_year=365,
        returns=net_returns,
        gross_returns=gross_returns,
        holdings_history=holdings,
        rebalance_log=rebalance_log,
    )

    assert "gross_total_return" in metrics
    assert "cost_drag_total_return" in metrics
    assert "pct_time_invested" in metrics
    assert "avg_holdings_count" in metrics
    assert "rebalance_count" in metrics
