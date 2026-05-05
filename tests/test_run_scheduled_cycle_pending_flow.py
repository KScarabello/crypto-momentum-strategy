from __future__ import annotations

import types

import pandas as pd

from live import run_scheduled_cycle


def _stub_args() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        broker_source="mock",
        broker_name="kraken",
        live=False,
        notify_email=False,
        min_order_notional=10.0,
        max_order_notional=None,
    )


def test_executes_pending_signal_even_on_non_rebalance_bar(monkeypatch) -> None:
    calls: dict[str, int] = {
        "subprocess": 0,
        "lock_release": 0,
    }

    monkeypatch.setattr(run_scheduled_cycle.argparse.ArgumentParser, "parse_args", lambda self: _stub_args())
    monkeypatch.setattr(run_scheduled_cycle, "load_dotenv", lambda: None)
    monkeypatch.setattr(run_scheduled_cycle, "get_data_symbols", lambda: ("BTC/USD",))
    monkeypatch.setattr(run_scheduled_cycle, "get_trading_symbols", lambda: ("BTC/USD",))
    monkeypatch.setattr(run_scheduled_cycle, "_refresh_ohlcv_data", lambda symbols, timeframe: True)
    monkeypatch.setattr(run_scheduled_cycle, "_verify_data_freshness", lambda: True)
    monkeypatch.setattr(
        run_scheduled_cycle,
        "_load_current_bar_snapshot",
        lambda: {
            "timestamp": pd.Timestamp("2026-05-02 00:00:00+00:00"),
            "is_rebalance_bar": False,
        },
    )
    monkeypatch.setattr(run_scheduled_cycle, "has_pending_signal", lambda: True)
    monkeypatch.setattr(
        run_scheduled_cycle,
        "load_pending_signal",
        lambda: {"timestamp": pd.Timestamp("2026-05-01 20:00:00+00:00")},
    )
    monkeypatch.setattr(run_scheduled_cycle, "_acquire_execution_lock", lambda: True)

    def fake_release() -> None:
        calls["lock_release"] += 1

    def fake_run(cmd, check):
        calls["subprocess"] += 1
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(run_scheduled_cycle, "_release_execution_lock", fake_release)
    monkeypatch.setattr(run_scheduled_cycle.subprocess, "run", fake_run)

    run_scheduled_cycle.main()

    assert calls["subprocess"] == 1
    assert calls["lock_release"] == 1


def test_non_rebalance_without_pending_exits_without_execute(monkeypatch) -> None:
    calls: dict[str, int] = {"subprocess": 0}

    monkeypatch.setattr(run_scheduled_cycle.argparse.ArgumentParser, "parse_args", lambda self: _stub_args())
    monkeypatch.setattr(run_scheduled_cycle, "load_dotenv", lambda: None)
    monkeypatch.setattr(run_scheduled_cycle, "get_data_symbols", lambda: ("BTC/USD",))
    monkeypatch.setattr(run_scheduled_cycle, "get_trading_symbols", lambda: ("BTC/USD",))
    monkeypatch.setattr(run_scheduled_cycle, "_refresh_ohlcv_data", lambda symbols, timeframe: True)
    monkeypatch.setattr(run_scheduled_cycle, "_verify_data_freshness", lambda: True)
    monkeypatch.setattr(
        run_scheduled_cycle,
        "_load_current_bar_snapshot",
        lambda: {
            "timestamp": pd.Timestamp("2026-05-02 00:00:00+00:00"),
            "is_rebalance_bar": False,
        },
    )
    monkeypatch.setattr(run_scheduled_cycle, "has_pending_signal", lambda: False)

    def fake_run(cmd, check):
        calls["subprocess"] += 1
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(run_scheduled_cycle.subprocess, "run", fake_run)

    run_scheduled_cycle.main()

    assert calls["subprocess"] == 0


def test_pending_same_bar_does_not_execute_or_generate(monkeypatch) -> None:
    calls: dict[str, int] = {"subprocess": 0}

    monkeypatch.setattr(run_scheduled_cycle.argparse.ArgumentParser, "parse_args", lambda self: _stub_args())
    monkeypatch.setattr(run_scheduled_cycle, "load_dotenv", lambda: None)
    monkeypatch.setattr(run_scheduled_cycle, "get_data_symbols", lambda: ("BTC/USD",))
    monkeypatch.setattr(run_scheduled_cycle, "get_trading_symbols", lambda: ("BTC/USD",))
    monkeypatch.setattr(run_scheduled_cycle, "_refresh_ohlcv_data", lambda symbols, timeframe: True)
    monkeypatch.setattr(run_scheduled_cycle, "_verify_data_freshness", lambda: True)
    monkeypatch.setattr(
        run_scheduled_cycle,
        "_load_current_bar_snapshot",
        lambda: {
            "timestamp": pd.Timestamp("2026-05-01 20:00:00+00:00"),
            "is_rebalance_bar": True,
        },
    )
    monkeypatch.setattr(run_scheduled_cycle, "has_pending_signal", lambda: True)
    monkeypatch.setattr(
        run_scheduled_cycle,
        "load_pending_signal",
        lambda: {"timestamp": pd.Timestamp("2026-05-01 20:00:00+00:00")},
    )

    def fake_run(cmd, check):
        calls["subprocess"] += 1
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(run_scheduled_cycle.subprocess, "run", fake_run)

    run_scheduled_cycle.main()

    assert calls["subprocess"] == 0
