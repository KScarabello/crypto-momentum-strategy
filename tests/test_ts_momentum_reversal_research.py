"""Tests for diagnostics helpers in TS momentum/reversal research runner."""

from __future__ import annotations

import pandas as pd
import pytest

from research.run_ts_momentum_reversal_research import (
    _count_entry_exit_events,
    _count_exit_reasons,
    _variant_diagnostics_row,
)


def _toy_index() -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=4, freq="4h", tz="UTC")


def test_count_entry_exit_events_counts_transitions_correctly() -> None:
    idx = _toy_index()
    executed = pd.DataFrame(
        {
            "BTC/USD": [0.0, 1.0, 0.0, 1.0],
            "ETH/USD": [0.0, 0.0, 1.0, 0.0],
        },
        index=idx,
    )

    entries, exits = _count_entry_exit_events(executed)
    assert entries == 3
    assert exits == 2


def test_count_exit_reasons_attributes_overextension_and_momentum() -> None:
    idx = _toy_index()
    executed = pd.DataFrame(
        {
            "BTC/USD": [0.0, 1.0, 0.0, 0.0],
            "ETH/USD": [0.0, 1.0, 0.0, 0.0],
        },
        index=idx,
    )
    momentum = pd.DataFrame(
        {
            "BTC/USD": [0.2, 0.2, -0.1, -0.1],
            "ETH/USD": [0.2, 0.2, 0.2, 0.2],
        },
        index=idx,
    )
    over = pd.DataFrame(
        {
            "BTC/USD": [0.05, 0.05, 0.10, 0.10],
            "ETH/USD": [0.05, 0.05, 0.40, 0.40],
        },
        index=idx,
    )

    over_exits, momentum_exits = _count_exit_reasons(
        executed_weights=executed,
        momentum_signal=momentum,
        overextension_signal=over,
        exit_overextension_threshold=0.30,
    )

    assert over_exits == 1
    assert momentum_exits == 1


def test_variant_diagnostics_row_reports_requested_fields() -> None:
    idx = _toy_index()
    target = pd.DataFrame(
        {
            "BTC/USD": [0.0, 1.0, 0.0, 1.0],
            "ETH/USD": [0.0, 0.0, 1.0, 0.0],
        },
        index=idx,
    )
    executed = target.copy()
    turnover = pd.Series([0.0, 1.0, 2.0, 2.0], index=idx, dtype=float)
    momentum = pd.DataFrame(
        {
            "BTC/USD": [0.2, 0.2, -0.2, 0.2],
            "ETH/USD": [-0.1, 0.2, 0.2, -0.1],
        },
        index=idx,
    )
    over = pd.DataFrame(
        {
            "BTC/USD": [0.05, 0.20, 0.10, 0.40],
            "ETH/USD": [0.05, 0.20, 0.40, 0.05],
        },
        index=idx,
    )

    row = _variant_diagnostics_row(
        strategy_name="test_variant",
        target_weights=target,
        executed_weights=executed,
        turnover=turnover,
        momentum_signal=momentum,
        overextension_signal=over,
        entry_overextension_threshold=0.15,
        exit_overextension_threshold=0.30,
    )

    assert row["strategy"] == "test_variant"
    assert row["avg_active_positions"] == pytest.approx(0.75)
    assert row["pct_timestamps_invested"] == pytest.approx(75.0)
    assert row["total_turnover"] == pytest.approx(5.0)
    assert row["avg_turnover"] == pytest.approx(1.25)
    assert row["num_entries"] == 3
    assert row["num_exits"] == 2
    assert row["momentum_qualified_opportunities"] == 5
    assert row["entry_opportunities_blocked_by_overextension"] == 4
    assert row["exits_triggered_by_overextension"] == 0
    assert row["exits_triggered_by_negative_momentum"] == 2
