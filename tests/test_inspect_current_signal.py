from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import SETTINGS
from data.fetch_ohlc import local_symbol_file_path
from research.inspect_current_signal import inspect_current_signal


def _write_symbol_csv(data_dir: Path, symbol: str, closes: list[float]) -> None:
    timestamps = pd.date_range("2026-01-01 00:00:00+00:00", periods=len(closes), freq="4h", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": closes,
            "high": [value + 0.5 for value in closes],
            "low": [value - 0.5 for value in closes],
            "close": closes,
            "volume": [1000.0] * len(closes),
            "symbol": [symbol] * len(closes),
        }
    )
    output_path = local_symbol_file_path(symbol=symbol, timeframe="4h", data_dir=data_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def test_inspect_current_signal_ranks_full_data_universe(tmp_path) -> None:
    data_dir = tmp_path / "local"
    symbols = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")
    periods = 220
    growth_map = {
        "BTC/USD": 1.0010,
        "ETH/USD": 1.0015,
        "XRP/USD": 1.0020,
        "SOL/USD": 1.0025,
        "AVAX/USD": 1.0030,
    }
    series_map = {
        symbol: [100.0 * (growth_map[symbol] ** idx) for idx in range(periods)]
        for symbol in symbols
    }

    for symbol in symbols:
        _write_symbol_csv(data_dir=data_dir, symbol=symbol, closes=series_map[symbol])

    snapshot = inspect_current_signal(
        symbols=symbols,
        timeframe="4h",
        data_dir=data_dir,
    )

    ranked_symbols = [row["symbol"] for row in snapshot["rankings"]]

    assert snapshot["timestamp"] == pd.Timestamp("2026-02-06 12:00:00+00:00")
    assert ranked_symbols == ["AVAX/USD", "SOL/USD", "XRP/USD", "ETH/USD", "BTC/USD"]
    assert snapshot["selected_if_full_universe"] == ranked_symbols[: SETTINGS.top_n]
