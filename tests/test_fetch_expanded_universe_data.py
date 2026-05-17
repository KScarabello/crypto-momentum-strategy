"""Tests for expanded-universe research data acquisition script."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from data.fetch_ohlc import local_symbol_file_path
from research.fetch_expanded_universe_data import (
    DEBUG_UNIVERSE,
    EXPANDED_RESEARCH_UNIVERSE,
    fetch_expanded_universe_data,
    split_available_symbols,
    symbol_to_local_filename,
)

NORMALIZED_COLS = ["timestamp", "open", "high", "low", "close", "volume", "symbol"]


def _make_ohlcv_frame(symbol: str, start: str, periods: int, freq: str = "4h") -> pd.DataFrame:
    """Build a minimal valid OHLCV DataFrame for testing."""
    timestamps = pd.date_range(start, periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * periods,
            "high": [101.0] * periods,
            "low": [99.0] * periods,
            "close": [100.0] * periods,
            "volume": [1.0] * periods,
            "symbol": [symbol] * periods,
        }
    )


def _make_downloader(batches: list[pd.DataFrame]) -> Any:
    """Return an injectable historical_downloader that yields pre-built batches in sequence.

    Each call returns the next batch in the list.  When exhausted, returns empty DataFrame.
    The downloader merges all batches into one return value (as ccxt_downloader would after
    completing pagination), so we concatenate all batches and return them in one call.
    """
    frames = [f for f in batches if not f.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=NORMALIZED_COLS)

    def downloader(**kwargs: Any) -> pd.DataFrame:
        return combined

    return downloader


def test_symbol_to_local_filename_conversion() -> None:
    assert symbol_to_local_filename("BTC/USD", "4h") == "btc-usd_4h.csv"
    assert symbol_to_local_filename("POL/USD", "4h") == "pol-usd_4h.csv"


def test_unavailable_symbols_are_skipped() -> None:
    fetched, skipped = split_available_symbols(
        requested_symbols=("BTC/USD", "MISSING/USD", "ETH/USD"),
        available_symbols={"BTC/USD", "ETH/USD"},
    )
    assert fetched == ["BTC/USD", "ETH/USD"]
    assert skipped == ["MISSING/USD"]


def test_candidate_universe_contains_original_five_debug_symbols() -> None:
    for symbol in DEBUG_UNIVERSE:
        assert symbol in EXPANDED_RESEARCH_UNIVERSE


def test_dry_run_does_not_write_ohlcv_files(tmp_path: Path) -> None:
    calls: list[str] = []

    def fake_updater(**kwargs):  # pragma: no cover - should not be called in this test
        calls.append("called")
        return {"fetched_rows": 0, "dropped_rows": 0, "final_rows": 0}

    symbols = ("BTC/USD",)
    summary_path = tmp_path / "summary.csv"

    fetch_expanded_universe_data(
        symbols=symbols,
        timeframe="4h",
        dry_run=True,
        data_dir=tmp_path,
        summary_output_path=summary_path,
        available_symbols_override={"BTC/USD"},
        incremental_updater=fake_updater,
    )

    target_file = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    assert not target_file.exists()
    assert calls == []
    assert summary_path.exists()


def test_summary_table_creation_from_local_data(tmp_path: Path) -> None:
    path = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="4h", tz="UTC"),
            "open": [100, 101, 102, 103, 104],
            "high": [101, 102, 103, 104, 105],
            "low": [99, 100, 101, 102, 103],
            "close": [100, 101, 102, 103, 104],
            "volume": [1, 1, 1, 1, 1],
            "symbol": ["BTC/USD"] * 5,
        }
    )
    frame.to_csv(path, index=False)

    summary = fetch_expanded_universe_data(
        symbols=("BTC/USD",),
        timeframe="4h",
        dry_run=True,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"BTC/USD"},
    )

    assert "symbol" in summary.columns
    assert "first_timestamp" in summary.columns
    assert "latest_timestamp" in summary.columns
    assert "row_count" in summary.columns
    assert "first_eligible_timestamp_for_strategy" in summary.columns
    assert "is_eligible_for_strategy" in summary.columns
    row = summary.loc[summary["symbol"] == "BTC/USD"].iloc[0]
    assert int(row["row_count"]) == 5


# ---------------------------------------------------------------------------
# Pagination-specific tests
# ---------------------------------------------------------------------------


def test_since_based_fetch_writes_all_downloaded_rows(tmp_path: Path) -> None:
    """When --since is provided the historical_downloader result is saved in full."""
    batch = _make_ohlcv_frame("BTC/USD", start="2022-01-01", periods=1440)  # 2 full batches
    downloader = _make_downloader([batch])

    summary = fetch_expanded_universe_data(
        symbols=("BTC/USD",),
        timeframe="4h",
        since="2022-01-01",
        dry_run=False,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"BTC/USD"},
        historical_downloader=downloader,
    )

    saved_path = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    assert saved_path.exists()
    saved = pd.read_csv(saved_path)
    assert len(saved) == 1440
    row = summary.loc[summary["symbol"] == "BTC/USD"].iloc[0]
    assert int(row["row_count"]) == 1440


def test_pagination_stops_gracefully_when_downloader_returns_empty(tmp_path: Path) -> None:
    """When the historical downloader returns an empty frame, no file is written for that symbol."""
    downloader = _make_downloader([])

    fetch_expanded_universe_data(
        symbols=("BTC/USD",),
        timeframe="4h",
        since="2022-01-01",
        dry_run=False,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"BTC/USD"},
        historical_downloader=downloader,
    )

    saved_path = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    # merge_and_save writes even empty results; verify summary row_count is 0
    summary_path = tmp_path / "summary.csv"
    assert summary_path.exists()
    summary = pd.read_csv(summary_path)
    row = summary.loc[summary["symbol"] == "BTC/USD"].iloc[0]
    assert int(row["row_count"]) == 0


def test_duplicate_timestamps_deduped_on_merge(tmp_path: Path) -> None:
    """Existing local rows that overlap downloaded rows are deduplicated."""
    existing_frame = _make_ohlcv_frame("BTC/USD", start="2022-01-01", periods=100)
    saved_path = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    saved_path.parent.mkdir(parents=True, exist_ok=True)
    existing_frame.to_csv(saved_path, index=False)

    # Downloader returns full 200-row range that includes 100 overlapping + 100 new
    full_frame = _make_ohlcv_frame("BTC/USD", start="2022-01-01", periods=200)
    downloader = _make_downloader([full_frame])

    fetch_expanded_universe_data(
        symbols=("BTC/USD",),
        timeframe="4h",
        since="2022-01-01",
        dry_run=False,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"BTC/USD"},
        historical_downloader=downloader,
    )

    saved = pd.read_csv(saved_path)
    assert len(saved) == 200  # no duplicate rows


def test_saved_data_is_sorted_by_timestamp(tmp_path: Path) -> None:
    """Output CSV is sorted ascending by timestamp."""
    # Build frame with descending timestamps to verify sorting is applied
    timestamps = pd.date_range("2022-01-01", periods=50, freq="4h", tz="UTC")[::-1]
    unsorted_frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * 50,
            "high": [101.0] * 50,
            "low": [99.0] * 50,
            "close": [100.0] * 50,
            "volume": [1.0] * 50,
            "symbol": ["BTC/USD"] * 50,
        }
    )
    downloader = _make_downloader([unsorted_frame])

    fetch_expanded_universe_data(
        symbols=("BTC/USD",),
        timeframe="4h",
        since="2022-01-01",
        dry_run=False,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"BTC/USD"},
        historical_downloader=downloader,
    )

    saved_path = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    saved = pd.read_csv(saved_path, parse_dates=["timestamp"])
    ts_series = pd.to_datetime(saved["timestamp"], utc=True)
    assert list(ts_series) == sorted(ts_series)


def test_asset_inception_preserved_when_first_ts_later_than_since(tmp_path: Path) -> None:
    """When the exchange only has data from a later date, that becomes the actual first_timestamp."""
    # Request since 2020-01-01 but exchange only has data from 2023-01-01 (late-listing asset)
    late_start_frame = _make_ohlcv_frame("APT/USD", start="2023-01-01", periods=500)
    downloader = _make_downloader([late_start_frame])

    summary = fetch_expanded_universe_data(
        symbols=("APT/USD",),
        timeframe="4h",
        since="2020-01-01",
        dry_run=False,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"APT/USD"},
        historical_downloader=downloader,
    )

    row = summary.loc[summary["symbol"] == "APT/USD"].iloc[0]
    actual_first = pd.to_datetime(row["first_timestamp"], utc=True)
    expected_first = pd.Timestamp("2023-01-01", tz="UTC")
    assert actual_first == expected_first
    assert int(row["row_count"]) == 500


def test_summary_reflects_actual_first_and_eligible_timestamps(tmp_path: Path) -> None:
    """Eligibility timestamps in summary are derived from actual data, not from requested since."""
    # 400 rows starting 2022-01-01: enough for 1-week (42) and 2-week (84) signals but not 8-week (336)
    frame_short = _make_ohlcv_frame("SOL/USD", start="2022-01-01", periods=400)
    downloader_short = _make_downloader([frame_short])

    summary_short = fetch_expanded_universe_data(
        symbols=("SOL/USD",),
        timeframe="4h",
        since="2022-01-01",
        dry_run=False,
        data_dir=tmp_path / "short",
        summary_output_path=tmp_path / "summary_short.csv",
        available_symbols_override={"SOL/USD"},
        historical_downloader=downloader_short,
    )

    row_short = summary_short.loc[summary_short["symbol"] == "SOL/USD"].iloc[0]
    assert int(row_short["row_count"]) == 400
    assert pd.notna(row_short["first_eligible_timestamp_for_1w_signal"])
    assert pd.notna(row_short["first_eligible_timestamp_for_2w_signal"])
    # 400 rows >= 336 so 8w signal is also reachable
    assert pd.notna(row_short["first_eligible_timestamp_for_8w_signal"])
    assert bool(row_short["is_eligible_for_strategy"])


def test_dry_run_with_since_does_not_write_ohlcv_files(tmp_path: Path) -> None:
    """dry_run=True skips downloading/writing even when --since is provided."""
    calls: list[str] = []

    def tracking_downloader(**kwargs):  # pragma: no cover - should not be called
        calls.append("called")
        return pd.DataFrame(columns=NORMALIZED_COLS)

    fetch_expanded_universe_data(
        symbols=("BTC/USD",),
        timeframe="4h",
        since="2022-01-01",
        dry_run=True,
        data_dir=tmp_path,
        summary_output_path=tmp_path / "summary.csv",
        available_symbols_override={"BTC/USD"},
        historical_downloader=tracking_downloader,
    )

    target_file = local_symbol_file_path(symbol="BTC/USD", timeframe="4h", data_dir=tmp_path)
    assert not target_file.exists()
    assert calls == []
