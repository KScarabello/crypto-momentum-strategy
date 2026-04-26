from __future__ import annotations

import pandas as pd

from data.fetch_ohlc import load_local_symbol_ohlcv, update_symbol_ohlcv_incremental


def _ms(timestamp: str) -> int:
    return int(pd.Timestamp(timestamp, tz="UTC").timestamp() * 1000)


class FakeKrakenExchange:
    def __init__(self, responses: list[list[list[float]]]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int, since: int | None = None):
        self.calls.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "limit": limit,
                "since": since,
            }
        )
        if self._responses:
            return self._responses.pop(0)
        return []


def test_incremental_update_is_idempotent_and_keeps_sorted_unique_data(tmp_path) -> None:
    data_dir = tmp_path / "local"
    data_dir.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2026-04-24 00:00:00+00:00"),
                pd.Timestamp("2026-04-24 04:00:00+00:00"),
            ],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10.0, 11.0],
            "symbol": ["BTC/USD", "BTC/USD"],
        }
    )
    existing.to_csv(data_dir / "btc-usd_4h.csv", index=False)

    exchange = FakeKrakenExchange(
        responses=[
            [
                [_ms("2026-04-24 04:00:00+00:00"), 101.0, 102.0, 100.0, 101.5, 11.0],
                [_ms("2026-04-24 08:00:00+00:00"), 102.0, 103.0, 101.0, 102.5, 12.0],
                [_ms("2026-04-24 12:00:00+00:00"), 103.0, 104.0, 102.0, 0.0, 13.0],
            ],
            [
                [_ms("2026-04-24 08:00:00+00:00"), 102.0, 103.0, 101.0, 102.5, 12.0],
            ],
        ]
    )

    first = update_symbol_ohlcv_incremental(
        symbol="BTC/USD",
        timeframe="4h",
        data_dir=data_dir,
        exchange=exchange,
    )
    second = update_symbol_ohlcv_incremental(
        symbol="BTC/USD",
        timeframe="4h",
        data_dir=data_dir,
        exchange=exchange,
    )

    saved = load_local_symbol_ohlcv(symbol="BTC/USD", timeframe="4h", data_dir=data_dir)

    assert first["fetched_rows"] == 3
    assert first["dropped_rows"] == 1
    assert first["final_rows"] == 3

    assert second["fetched_rows"] == 1
    assert second["dropped_rows"] == 0
    assert second["final_rows"] == 3

    assert exchange.calls[0]["since"] == _ms("2026-04-24 04:00:00+00:00")
    assert exchange.calls[1]["since"] == _ms("2026-04-24 08:00:00+00:00")

    assert saved["timestamp"].is_monotonic_increasing
    assert saved["timestamp"].dt.tz is not None
    assert saved["timestamp"].duplicated().sum() == 0
    assert list(saved["timestamp"]) == [
        pd.Timestamp("2026-04-24 00:00:00+00:00"),
        pd.Timestamp("2026-04-24 04:00:00+00:00"),
        pd.Timestamp("2026-04-24 08:00:00+00:00"),
    ]


def test_incremental_update_creates_new_file_with_kraken_limit_when_missing(tmp_path) -> None:
    data_dir = tmp_path / "local"
    exchange = FakeKrakenExchange(
        responses=[
            [
                [_ms("2026-04-24 00:00:00+00:00"), 50.0, 51.0, 49.0, 50.5, 5.0],
                [_ms("2026-04-24 04:00:00+00:00"), 51.0, 52.0, 50.0, 51.5, 6.0],
            ]
        ]
    )

    result = update_symbol_ohlcv_incremental(
        symbol="SOL/USD",
        timeframe="4h",
        data_dir=data_dir,
        exchange=exchange,
    )

    saved = load_local_symbol_ohlcv(symbol="SOL/USD", timeframe="4h", data_dir=data_dir)

    assert exchange.calls == [
        {
            "symbol": "SOL/USD",
            "timeframe": "4h",
            "limit": 720,
            "since": None,
        }
    ]
    assert result["fetched_rows"] == 2
    assert result["final_rows"] == 2
    assert saved["timestamp"].is_monotonic_increasing
    assert saved["timestamp"].duplicated().sum() == 0