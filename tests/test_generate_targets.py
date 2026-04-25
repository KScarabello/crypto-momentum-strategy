from __future__ import annotations

import pandas as pd

from live.generate_targets import _is_rebalance_bar


def test_rebalance_bar_matches_configured_utc_hour() -> None:
    ts = pd.Timestamp("2026-04-25 20:00:00+00:00")
    assert _is_rebalance_bar(ts, rebalance_hour_utc=20) is True


def test_non_rebalance_bar_for_other_utc_hours() -> None:
    ts = pd.Timestamp("2026-04-25 16:00:00+00:00")
    assert _is_rebalance_bar(ts, rebalance_hour_utc=20) is False


def test_rebalance_hour_uses_utc_after_timezone_conversion() -> None:
    # 16:00 in New York during DST equals 20:00 UTC.
    ts_ny = pd.Timestamp("2026-04-25 16:00:00", tz="America/New_York")
    assert _is_rebalance_bar(ts_ny, rebalance_hour_utc=20) is True


def test_invalid_rebalance_hour_raises() -> None:
    ts = pd.Timestamp("2026-04-25 20:00:00+00:00")
    try:
        _is_rebalance_bar(ts, rebalance_hour_utc=24)
    except ValueError as exc:
        assert "rebalance_hour_utc" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid rebalance_hour_utc")