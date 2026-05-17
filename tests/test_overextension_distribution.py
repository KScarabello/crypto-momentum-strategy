"""Tests for overextension signal distribution diagnostics."""

from __future__ import annotations

import pandas as pd
import pytest

from research.run_ts_momentum_reversal_research import _overextension_distribution_table


def test_overextension_distribution_table_reports_symbol_and_overall_stats() -> None:
    idx = pd.date_range("2024-01-01", periods=5, freq="4h", tz="UTC")
    over = pd.DataFrame(
        {
            "BTC/USD": [-0.10, 0.00, 0.05, 0.10, 0.20],
            "ETH/USD": [-0.20, 0.15, 0.25, 0.35, float("nan")],
        },
        index=idx,
    )

    dist = _overextension_distribution_table(over)

    assert list(dist["symbol"]) == ["BTC/USD", "ETH/USD", "ALL"]

    btc = dist.loc[dist["symbol"] == "BTC/USD"].iloc[0]
    assert btc["observations"] == 5
    assert btc["min"] == pytest.approx(-0.10)
    assert btc["median"] == pytest.approx(0.05)
    assert btc["max"] == pytest.approx(0.20)
    assert btc["count_gt_5pct"] == 2
    assert btc["count_gt_10pct"] == 1
    assert btc["count_gt_15pct"] == 1
    assert btc["count_gt_20pct"] == 0

    eth = dist.loc[dist["symbol"] == "ETH/USD"].iloc[0]
    assert eth["observations"] == 4
    assert eth["count_gt_5pct"] == 3
    assert eth["count_gt_10pct"] == 3
    assert eth["count_gt_15pct"] == 2
    assert eth["count_gt_20pct"] == 2
    assert eth["count_gt_25pct"] == 1
    assert eth["count_gt_30pct"] == 1

    overall = dist.loc[dist["symbol"] == "ALL"].iloc[0]
    assert overall["observations"] == 9
    assert overall["min"] == pytest.approx(-0.20)
    assert overall["max"] == pytest.approx(0.35)
    assert overall["count_gt_30pct"] == 1
