from __future__ import annotations

import pandas as pd

from live.generate_targets import generate_targets
from live.run_scheduled_cycle import _verify_data_freshness


def test_generate_targets_uses_expanded_trading_universe_when_symbols_not_provided(monkeypatch) -> None:
    trading_symbols = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")

    def fake_load_ohlcv_history(symbols, timeframe, data_dir, downloader):
        assert symbols == trading_symbols
        timestamps = [
            pd.Timestamp("2026-04-25 16:00:00+00:00"),
            pd.Timestamp("2026-04-25 20:00:00+00:00"),
        ]
        rows = []
        for symbol in symbols:
            for idx, ts in enumerate(timestamps):
                price = 100.0 + idx
                rows.append(
                    {
                        "timestamp": ts,
                        "open": price,
                        "high": price + 1.0,
                        "low": price - 1.0,
                        "close": price,
                        "volume": 1_000.0,
                        "symbol": symbol,
                    }
                )
        return pd.DataFrame(rows)

    monkeypatch.setattr("live.generate_targets.get_trading_symbols", lambda: trading_symbols)
    monkeypatch.setattr("live.generate_targets.load_ohlcv_history", fake_load_ohlcv_history)

    result = generate_targets(symbols=None, timeframe="4h", data_dir="data/local")

    assert set(result["target_weights"].keys()) == set(trading_symbols)


def test_scheduled_cycle_uses_expanded_trading_universe_for_freshness_checks(monkeypatch) -> None:
    trading_symbols = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")
    captured: dict[str, tuple[str, ...]] = {}

    def fake_generate_targets(symbols):
        captured["symbols"] = symbols
        return {
            "data_fresh": True,
            "timestamp": pd.Timestamp("2026-04-26 20:00:00+00:00"),
        }

    monkeypatch.setattr("live.run_scheduled_cycle.get_trading_symbols", lambda: trading_symbols)
    monkeypatch.setattr("live.run_scheduled_cycle.generate_targets", fake_generate_targets)

    assert _verify_data_freshness() is True
    assert captured["symbols"] == trading_symbols
