"""Tests for the momentum backtest engine."""

from __future__ import annotations

import pandas as pd

from backtest.engine import _is_rebalance_bar_utc_hour, run_momentum_rotation_backtest
from live.generate_targets import _is_rebalance_bar as live_is_rebalance_bar


def _tiny_ohlcv() -> pd.DataFrame:
    timestamps = pd.date_range("2024-01-01 00:00:00+00:00", periods=30, freq="4h", tz="UTC")
    rows: list[dict[str, object]] = []

    price_map = {
        "BTC/USD": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
        "ETH/USD": [50, 50.5, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60],
        "SOL/USD": [20, 20.2, 20.4, 20.6, 20.8, 21.0, 21.3, 21.5, 21.7, 22.0, 22.3, 22.6],
    }

    for symbol, closes in price_map.items():
        for ts, close in zip(timestamps, closes):
            rows.append(
                {
                    "timestamp": ts,
                    "open": float(close),
                    "high": float(close),
                    "low": float(close),
                    "close": float(close),
                    "volume": 1_000.0,
                    "symbol": symbol,
                }
            )

    return pd.DataFrame(rows)


def test_backtest_engine_outputs_are_non_empty() -> None:
    ohlcv = _tiny_ohlcv()

    result = run_momentum_rotation_backtest(
        ohlcv=ohlcv,
        top_n=2,
        rebalance_every_bars=2,
        rebalance_hour_utc=20,
        short_lookback_bars=2,
        medium_lookback_bars=3,
        short_weight=0.5,
        medium_weight=0.5,
        btc_symbol="BTC/USD",
        regime_ma_lookback_bars=3,
        transaction_cost_bps=10.0,
        slippage_bps=1.0,
        initial_capital=10_000.0,
    )

    assert not result.portfolio.empty
    assert not result.rebalance_log.empty
    assert not result.holdings_history.empty
    assert len(result.turnover) == len(result.portfolio)

    assert "equity" in result.portfolio.columns
    assert "strategy_return" in result.portfolio.columns
    assert float(result.portfolio["equity"].iloc[-1]) > 0.0


def test_rebalances_execute_with_one_bar_delay() -> None:
    ohlcv = _tiny_ohlcv()

    result = run_momentum_rotation_backtest(
        ohlcv=ohlcv,
        top_n=2,
        rebalance_every_bars=2,
        rebalance_hour_utc=20,
        short_lookback_bars=2,
        medium_lookback_bars=3,
        regime_ma_lookback_bars=3,
        min_history_bars=3,
        min_eligible_assets=1,
    )

    assert not result.rebalance_log.empty
    assert "signal_timestamp" in result.rebalance_log.columns
    assert "execution_timestamp" in result.rebalance_log.columns

    delayed = (
        pd.to_datetime(result.rebalance_log["execution_timestamp"], utc=True)
        > pd.to_datetime(result.rebalance_log["signal_timestamp"], utc=True)
    )
    assert bool(delayed.all())


def test_backtest_rebalances_only_on_20_utc_signal_bars() -> None:
    ohlcv = _tiny_ohlcv()

    result = run_momentum_rotation_backtest(
        ohlcv=ohlcv,
        top_n=2,
        rebalance_every_bars=6,
        rebalance_hour_utc=20,
        short_lookback_bars=2,
        medium_lookback_bars=3,
        regime_ma_lookback_bars=3,
        min_history_bars=3,
        min_eligible_assets=1,
    )

    assert not result.rebalance_log.empty
    signal_hours = pd.to_datetime(result.rebalance_log["signal_timestamp"], utc=True).dt.hour.unique()
    assert set(signal_hours.tolist()) == {20}


def test_backtest_and_live_rebalance_gate_are_equivalent() -> None:
    timestamps = [
        pd.Timestamp("2026-04-25 20:00:00+00:00"),
        pd.Timestamp("2026-04-25 16:00:00+00:00"),
        pd.Timestamp("2026-04-25 16:00:00", tz="America/New_York"),
    ]

    for ts in timestamps:
        assert _is_rebalance_bar_utc_hour(ts, 20) == live_is_rebalance_bar(ts, 20)
