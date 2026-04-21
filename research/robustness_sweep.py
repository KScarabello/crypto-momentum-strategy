"""Run a small robustness sweep across key backtest assumptions."""

from __future__ import annotations

from itertools import product

import pandas as pd

from backtest.engine import run_momentum_rotation_backtest
from backtest.metrics import summary_metrics
from config import SETTINGS
from data.fetch_ohlc import load_ohlcv_history
from research.run_backtest import bars_per_year_for_timeframe


def run_sweep() -> pd.DataFrame:
    """Evaluate strategy sensitivity to costs, rebalance cadence, and breadth."""
    ohlcv = load_ohlcv_history(
        symbols=SETTINGS.symbols,
        timeframe=SETTINGS.timeframe,
        data_dir=SETTINGS.data_dir,
    )

    sweep_rows: list[dict[str, float | int]] = []
    bars_per_year = bars_per_year_for_timeframe(SETTINGS.timeframe)

    rebalance_grid = sorted({SETTINGS.rebalance_every_bars, 6, 12})
    top_n_grid = sorted({SETTINGS.top_n, 2, 3})
    cost_grid = [10.0, 20.0]
    slippage_grid = [2.0, 8.0]

    for rebalance_every_bars, top_n, fee_bps, slippage_bps in product(
        rebalance_grid,
        top_n_grid,
        cost_grid,
        slippage_grid,
    ):
        result = run_momentum_rotation_backtest(
            ohlcv=ohlcv,
            top_n=top_n,
            rebalance_every_bars=rebalance_every_bars,
            short_lookback_bars=SETTINGS.short_lookback_bars,
            medium_lookback_bars=SETTINGS.medium_lookback_bars,
            short_weight=SETTINGS.short_weight,
            medium_weight=SETTINGS.medium_weight,
            btc_symbol=SETTINGS.btc_symbol,
            regime_ma_lookback_bars=SETTINGS.regime_lookback_bars,
            transaction_cost_bps=fee_bps,
            slippage_bps=slippage_bps,
            initial_capital=SETTINGS.initial_capital,
            min_history_bars=SETTINGS.min_history_bars,
            min_eligible_assets=SETTINGS.min_eligible_assets,
            min_median_volume=SETTINGS.min_median_volume,
            max_turnover_per_rebalance=SETTINGS.max_turnover_per_rebalance,
        )

        metrics = summary_metrics(
            equity=result.portfolio["equity"],
            returns=result.portfolio["strategy_return"],
            turnover=result.turnover,
            gross_returns=result.gross_return,
            holdings_history=result.holdings_history,
            rebalance_log=result.rebalance_log,
            bars_per_year=bars_per_year,
        )

        sweep_rows.append(
            {
                "rebalance_every_bars": rebalance_every_bars,
                "top_n": top_n,
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
                "total_return": metrics["total_return"],
                "cagr": metrics["cagr"],
                "sharpe": metrics["sharpe"],
                "max_drawdown": metrics["max_drawdown"],
                "avg_turnover": metrics.get("avg_turnover", 0.0),
                "cost_drag_total_return": metrics.get("cost_drag_total_return", 0.0),
            }
        )

    table = pd.DataFrame(sweep_rows).sort_values("sharpe", ascending=False).reset_index(drop=True)
    return table


def main() -> None:
    table = run_sweep()
    output_path = SETTINGS.output_dir / "robustness_sweep.csv"
    SETTINGS.output_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(output_path, index=False)

    print("Robustness Sweep (top 5 by Sharpe)")
    print(table.head(5).to_string(index=False))
    print("\nRobustness Sweep (bottom 5 by Sharpe)")
    print(table.tail(5).to_string(index=False))
    print(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()
