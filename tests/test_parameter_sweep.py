"""Tests for parameter sweep mode in TS momentum/reversal research runner."""

from __future__ import annotations

import pandas as pd

from research.run_ts_momentum_reversal_research import (
    _build_parameter_sweep_results,
    _parameter_sweep_grid,
)


def test_parameter_sweep_grid_skips_invalid_threshold_pairs() -> None:
    entry_thresholds = [0.05, 0.10]
    exit_thresholds = [0.075, 0.125]

    grid = _parameter_sweep_grid(entry_thresholds, exit_thresholds)

    assert (0.05, 0.075) in grid
    assert (0.05, 0.125) in grid
    assert (0.10, 0.125) in grid
    assert (0.10, 0.075) not in grid
    assert len(grid) == 3


def test_parameter_sweep_results_include_threshold_columns_and_strategies() -> None:
    idx = pd.date_range("2024-01-01", periods=8, freq="4h", tz="UTC")
    close = pd.DataFrame(
        {
            "BTC/USD": [100, 102, 101, 104, 103, 106, 105, 108],
            "ETH/USD": [50, 49, 50, 52, 53, 52, 54, 55],
        },
        index=idx,
        dtype=float,
    )

    # Toy signals for deterministic sweep behavior.
    momentum_signal = close.pct_change(periods=2)
    overextension_signal = close.pct_change(periods=1)

    result = _build_parameter_sweep_results(
        close=close,
        momentum_signal=momentum_signal,
        overextension_signal=overextension_signal,
        bars_per_year=365 * 6,
        initial_capital=10_000.0,
        transaction_cost_bps=10.0,
        slippage_bps=0.0,
        entry_thresholds=[0.05, 0.10],
        exit_thresholds=[0.10],
    )

    assert not result.empty
    assert "entry_threshold" in result.columns
    assert "exit_threshold" in result.columns

    assert set(result["strategy"].unique()) == {
        "momentum_with_entry_filter",
        "momentum_with_exit_signal",
        "momentum_with_entry_and_exit",
    }

    # Only valid combo here is (0.05, 0.10); (0.10, 0.10) also valid.
    # Two combos * three strategies = six rows.
    assert len(result) == 6
