from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import research.compare_universes as compare


def _sample_ohlcv(symbols: tuple[str, ...], *, include_pre_start: bool = True, end_ts: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        if include_pre_start:
            rows.append(
                {
                    "timestamp": pd.Timestamp("2020-09-22 04:00:00+00:00"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1000.0,
                    "symbol": symbol,
                }
            )
        rows.append(
            {
                "timestamp": pd.Timestamp("2020-09-22 08:00:00+00:00"),
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.0,
                "volume": 1200.0,
                "symbol": symbol,
            }
        )
        rows.append(
            {
                "timestamp": pd.Timestamp("2020-09-22 20:00:00+00:00"),
                "open": 101.5,
                "high": 102.5,
                "low": 100.5,
                "close": 101.5,
                "volume": 1250.0,
                "symbol": symbol,
            }
        )
        end_timestamp = pd.Timestamp(end_ts)
        if end_timestamp == pd.Timestamp("2020-09-22 20:00:00+00:00"):
            continue
        rows.append(
            {
                "timestamp": end_timestamp,
                "open": 102.0,
                "high": 103.0,
                "low": 101.0,
                "close": 102.0,
                "volume": 1300.0,
                "symbol": symbol,
            }
        )
    return pd.DataFrame(rows)


def test_compare_universes_uses_expected_universes_and_shared_range(tmp_path, monkeypatch) -> None:
    load_calls: list[tuple[str, ...]] = []
    run_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    def fake_load_ohlcv_history(symbols, timeframe, data_dir, downloader):
        assert timeframe == "4h"
        load_calls.append(tuple(symbols))
        if len(symbols) == 3:
            return _sample_ohlcv(tuple(symbols), end_ts="2020-09-23 00:00:00+00:00")
        return _sample_ohlcv(tuple(symbols), end_ts="2020-09-22 20:00:00+00:00")

    def fake_run_momentum_rotation_backtest(ohlcv, **_kwargs):
        run_ranges.append((ohlcv["timestamp"].min(), ohlcv["timestamp"].max()))
        index = pd.DatetimeIndex(sorted(ohlcv["timestamp"].unique()))
        portfolio = pd.DataFrame(
            {
                "equity": [10_000.0 + 100.0 * i for i in range(len(index))],
                "strategy_return": [0.0] + [0.01] * (len(index) - 1),
            },
            index=index,
        )
        turnover = pd.Series([0.0] + [0.1] * (len(index) - 1), index=index)
        rebalance_log = pd.DataFrame({"timestamp": list(index[1:])})
        return SimpleNamespace(
            portfolio=portfolio,
            turnover=turnover,
            rebalance_log=rebalance_log,
            holdings_history=pd.DataFrame(index=index),
            gross_return=pd.Series([0.0] * len(index), index=index),
        )

    monkeypatch.setattr(compare, "load_ohlcv_history", fake_load_ohlcv_history)
    monkeypatch.setattr(compare, "run_momentum_rotation_backtest", fake_run_momentum_rotation_backtest)

    output_path = tmp_path / "comparison.csv"
    result = compare.compare_universes(output_path=output_path, save_csv=True)

    assert load_calls == [
        ("BTC/USD", "ETH/USD", "XRP/USD"),
        ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD"),
    ]

    # Both universes should run from common start and be clipped to common end.
    assert run_ranges == [
        (
            pd.Timestamp("2020-09-22 08:00:00+00:00"),
            pd.Timestamp("2020-09-22 20:00:00+00:00"),
        ),
        (
            pd.Timestamp("2020-09-22 08:00:00+00:00"),
            pd.Timestamp("2020-09-22 20:00:00+00:00"),
        ),
    ]

    assert list(result["universe"]) == ["three_asset", "five_asset"]
    assert output_path.exists()


def test_compare_universes_can_skip_csv_output(tmp_path, monkeypatch) -> None:
    def fake_load_ohlcv_history(symbols, timeframe, data_dir, downloader):
        return _sample_ohlcv(tuple(symbols), include_pre_start=False, end_ts="2020-09-22 20:00:00+00:00")

    def fake_run_momentum_rotation_backtest(ohlcv, **_kwargs):
        index = pd.DatetimeIndex(sorted(ohlcv["timestamp"].unique()))
        portfolio = pd.DataFrame(
            {
                "equity": [10_000.0, 10_050.0],
                "strategy_return": [0.0, 0.005],
            },
            index=index,
        )
        turnover = pd.Series([0.0, 0.1], index=index)
        rebalance_log = pd.DataFrame({"timestamp": [index[-1]]})
        return SimpleNamespace(
            portfolio=portfolio,
            turnover=turnover,
            rebalance_log=rebalance_log,
            holdings_history=pd.DataFrame(index=index),
            gross_return=pd.Series([0.0, 0.0], index=index),
        )

    monkeypatch.setattr(compare, "load_ohlcv_history", fake_load_ohlcv_history)
    monkeypatch.setattr(compare, "run_momentum_rotation_backtest", fake_run_momentum_rotation_backtest)

    output_path = tmp_path / "should_not_exist.csv"
    compare.compare_universes(output_path=output_path, save_csv=False)

    assert not output_path.exists()
