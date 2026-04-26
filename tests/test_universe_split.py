from __future__ import annotations

import sys
from types import SimpleNamespace

import pandas as pd

from config import get_data_symbols, get_trading_symbols
from data import download_ohlcv
from live.execute_orders import _build_prepared_orders


def _sample_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2026-04-25 16:00:00+00:00"),
                pd.Timestamp("2026-04-25 20:00:00+00:00"),
            ],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [10.0, 11.0],
        }
    )


def test_configured_trading_and_data_universes_include_all_five_symbols() -> None:
    expected = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")
    assert get_trading_symbols() == expected
    assert get_data_symbols() == expected


def test_download_all_symbols_refreshes_data_universe(monkeypatch) -> None:
    symbols = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")
    downloaded: list[tuple[str, str]] = []

    fake_exchange = object()

    def fake_update_symbol_ohlcv_incremental(**kwargs):
        downloaded.append((kwargs["symbol"], kwargs["timeframe"]))
        assert kwargs["exchange"] is fake_exchange
        return {
            "symbol": kwargs["symbol"],
            "fetched_rows": 2,
            "dropped_rows": 0,
            "final_rows": 2,
        }

    monkeypatch.setitem(sys.modules, "ccxt", SimpleNamespace(kraken=lambda config: fake_exchange))
    monkeypatch.setattr(download_ohlcv, "update_symbol_ohlcv_incremental", fake_update_symbol_ohlcv_incremental)

    download_ohlcv.download_all_symbols(symbols=symbols, timeframe="4h")

    assert [symbol for symbol, _ in downloaded] == list(symbols)
    assert {timeframe for _, timeframe in downloaded} == {"4h"}


def test_build_prepared_orders_allows_sol_and_avax_when_trading_universe_is_five(monkeypatch) -> None:
    pending_signal = {
        "strategy_variant": "locked_baseline",
        "timestamp": pd.Timestamp("2026-04-25 20:00:00+00:00"),
        "target_weights": {
            "BTC/USD": 0.20,
            "ETH/USD": 0.20,
            "XRP/USD": 0.20,
            "SOL/USD": 0.20,
            "AVAX/USD": 0.20,
        },
    }

    def fake_load_account_state(**_kwargs):
        return SimpleNamespace(equity=1_000.0, positions={}, available_cash=1_000.0)

    monkeypatch.setattr(
        "live.execute_orders.get_trading_symbols",
        lambda: ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD"),
    )
    monkeypatch.setattr("live.execute_orders.load_account_state", fake_load_account_state)

    _, _, prepared = _build_prepared_orders(
        broker_source="mock",
        min_order_notional=10.0,
        api_key=None,
        api_secret=None,
        api_passphrase=None,
        use_pending_signal=pending_signal,
    )

    prepared_symbols = {order.symbol for order in prepared}
    assert "BTC/USD" in prepared_symbols
    assert "ETH/USD" in prepared_symbols
    assert "SOL/USD" in prepared_symbols
    assert "AVAX/USD" in prepared_symbols
