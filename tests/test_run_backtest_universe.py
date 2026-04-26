from __future__ import annotations

import inspect
from types import SimpleNamespace

import pandas as pd

import research.run_backtest as run_backtest


def _sample_ohlcv(symbols: tuple[str, ...]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        rows.append(
            {
                "timestamp": pd.Timestamp("2026-04-25 16:00:00+00:00"),
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
                "timestamp": pd.Timestamp("2026-04-25 20:00:00+00:00"),
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.0,
                "volume": 1100.0,
                "symbol": symbol,
            }
        )
    return pd.DataFrame(rows)


def _sample_result(index: pd.DatetimeIndex) -> SimpleNamespace:
    portfolio = pd.DataFrame(
        {
            "equity": [10_000.0, 10_100.0],
            "strategy_return": [0.0, 0.01],
        },
        index=index,
    )
    gross_return = pd.Series([0.0, 0.01], index=index)
    turnover = pd.Series([0.0, 0.1], index=index)
    holdings_history = pd.DataFrame(
        {
            "BTC/USD": [0.5, 0.5],
            "ETH/USD": [0.25, 0.25],
        },
        index=index,
    )
    rebalance_log = pd.DataFrame(
        {
            "timestamp": [index[-1]],
            "turnover": [0.1],
        }
    )
    return SimpleNamespace(
        portfolio=portfolio,
        gross_return=gross_return,
        turnover=turnover,
        holdings_history=holdings_history,
        rebalance_log=rebalance_log,
    )


def test_run_backtest_uses_configured_trading_universe_and_logs_it(monkeypatch, caplog) -> None:
    trading_symbols = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")
    captured: dict[str, object] = {}

    def fake_load_ohlcv_history(symbols, timeframe, data_dir, downloader):
        captured["symbols"] = tuple(symbols)
        return _sample_ohlcv(tuple(symbols))

    def fake_run_backtest(ohlcv, **_kwargs):
        captured["loaded_symbols"] = sorted(ohlcv["symbol"].unique().tolist())
        index = pd.DatetimeIndex(sorted(ohlcv["timestamp"].unique()))
        return _sample_result(index)

    monkeypatch.setattr(run_backtest, "get_trading_symbols", lambda: trading_symbols)
    monkeypatch.setattr(run_backtest, "load_ohlcv_history", fake_load_ohlcv_history)
    monkeypatch.setattr(run_backtest, "run_momentum_rotation_backtest", fake_run_backtest)
    monkeypatch.setattr(run_backtest, "write_outputs", lambda **_kwargs: {
        "equity": pd.Timestamp("2026-01-01"),
        "rebalance_log": pd.Timestamp("2026-01-01"),
        "holdings_history": pd.Timestamp("2026-01-01"),
    })
    monkeypatch.setattr(run_backtest, "summary_metrics", lambda **_kwargs: {
        "cagr": 0.1,
        "sharpe": 1.0,
        "max_drawdown": -0.2,
    })
    monkeypatch.setattr(run_backtest, "print_summary", lambda _metrics: None)
    monkeypatch.setattr(run_backtest, "print_sanity_summary", lambda **_kwargs: None)

    caplog.set_level("INFO")
    run_backtest.main()

    assert captured["symbols"] == trading_symbols
    assert captured["loaded_symbols"] == sorted(list(trading_symbols))
    assert "Backtest trading universe: BTC/USD, ETH/USD, XRP/USD, SOL/USD, AVAX/USD" in caplog.text


def test_run_backtest_path_has_no_hardcoded_three_symbol_list() -> None:
    source = inspect.getsource(run_backtest)
    assert '("BTC/USD", "ETH/USD", "XRP/USD")' not in source
