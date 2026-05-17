"""Run research comparison: time-series momentum vs short-term reversal overlays."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from backtest.metrics import summary_metrics
from config import SETTINGS
from data.fetch_ohlc import load_ohlcv_history, pivot_close
from research.run_backtest import bars_per_year_for_timeframe
from research.signals import (
    calculate_momentum_signal,
    calculate_overextension_signal,
)
from research.strategy_variants import (
    momentum_with_entry_filter_and_exit_signal_weights,
    momentum_with_entry_filter_weights,
    momentum_with_exit_signal_weights,
    short_term_reversal_weights,
    time_series_momentum_weights,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_UNIVERSE: tuple[str, ...] = ("BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "AVAX/USD")
DEFAULT_OUTPUT_PATH = Path("research/results/ts_momentum_reversal_metrics.csv")
DEFAULT_DIAGNOSTICS_PATH = Path("research/results/ts_momentum_reversal_diagnostics.csv")
DEFAULT_OVEREXTENSION_DISTRIBUTION_PATH = Path("research/results/overextension_signal_distribution.csv")
DEFAULT_PARAMETER_SWEEP_PATH = Path("research/results/ts_momentum_reversal_parameter_sweep.csv")


def configure_logging() -> None:
    """Configure research logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _normalize_timeframe(timeframe: str | None) -> str:
    """Normalize timeframe and default to 4h for this research workflow."""
    candidate = (timeframe or "").strip().lower()
    if candidate in {"4h", "1d", "d", "daily"}:
        return "1d" if candidate in {"1d", "d", "daily"} else "4h"
    return "4h"


def _align_weights_to_returns(weights: pd.DataFrame, asset_returns: pd.DataFrame) -> pd.DataFrame:
    """Shift target weights by one bar to avoid lookahead execution."""
    executed = weights.shift(1).fillna(0.0)
    return executed.reindex(asset_returns.index).fillna(0.0)


def _simulate_from_weights(
    close: pd.DataFrame,
    target_weights: pd.DataFrame,
    initial_capital: float,
    transaction_cost_bps: float,
    slippage_bps: float,
) -> dict[str, pd.Series]:
    """Convert target weights into net returns/equity using one-bar delayed execution."""
    asset_returns = close.pct_change().fillna(0.0)
    executed_weights = _align_weights_to_returns(target_weights, asset_returns)

    gross_returns = (executed_weights * asset_returns).sum(axis=1)
    turnover = executed_weights.diff().abs().sum(axis=1).fillna(0.0)
    total_cost_bps = float(transaction_cost_bps) + float(slippage_bps)
    cost_rate = turnover * (total_cost_bps / 10_000.0)
    net_returns = gross_returns - cost_rate

    equity = float(initial_capital) * (1.0 + net_returns).cumprod()
    return {
        "gross_returns": gross_returns,
        "net_returns": net_returns,
        "turnover": turnover,
        "equity": equity,
    }


def _format_metrics_row(strategy_name: str, metrics: dict[str, float], final_equity: float) -> dict[str, float | str]:
    """Build one output row from computed summary metrics."""
    return {
        "strategy": strategy_name,
        "cagr": float(metrics.get("cagr", 0.0)),
        "sharpe": float(metrics.get("sharpe", 0.0)),
        "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
        "avg_turnover": float(metrics.get("avg_turnover", 0.0)),
        "total_turnover": float(metrics.get("total_turnover", 0.0)),
        "total_return": float(metrics.get("total_return", 0.0)),
        "final_equity": float(final_equity),
    }


def _count_entry_exit_events(executed_weights: pd.DataFrame) -> tuple[int, int]:
    """Count total entries and exits from executed position state transitions."""
    held = executed_weights > 1e-12
    previous = held.shift(1).fillna(False).astype(bool)
    entries = int((~previous & held).sum().sum())
    exits = int((previous & ~held).sum().sum())
    return entries, exits


def _count_exit_reasons(
    executed_weights: pd.DataFrame,
    momentum_signal: pd.DataFrame,
    overextension_signal: pd.DataFrame,
    exit_overextension_threshold: float,
) -> tuple[int, int]:
    """Count exits associated with overextension and negative momentum conditions."""
    held = executed_weights > 1e-12
    previous = held.shift(1).fillna(False).astype(bool)
    exited_mask = previous & ~held

    aligned_momentum = momentum_signal.reindex(executed_weights.index).reindex(columns=executed_weights.columns)
    aligned_over = overextension_signal.reindex(executed_weights.index).reindex(columns=executed_weights.columns)

    exits_on_overextension = int((exited_mask & (aligned_over > exit_overextension_threshold)).sum().sum())
    exits_on_negative_momentum = int((exited_mask & (aligned_momentum < 0.0)).sum().sum())
    return exits_on_overextension, exits_on_negative_momentum


def _variant_diagnostics_row(
    strategy_name: str,
    target_weights: pd.DataFrame,
    executed_weights: pd.DataFrame,
    turnover: pd.Series,
    momentum_signal: pd.DataFrame,
    overextension_signal: pd.DataFrame,
    entry_overextension_threshold: float,
    exit_overextension_threshold: float,
) -> dict[str, float | int | str]:
    """Build one diagnostics row for a strategy variant."""
    active_positions = (executed_weights > 1e-12).sum(axis=1)
    percent_invested = float((executed_weights.sum(axis=1) > 1e-12).mean() * 100.0)

    entries, exits = _count_entry_exit_events(executed_weights)
    exits_on_overextension, exits_on_negative_momentum = _count_exit_reasons(
        executed_weights=executed_weights,
        momentum_signal=momentum_signal,
        overextension_signal=overextension_signal,
        exit_overextension_threshold=exit_overextension_threshold,
    )

    aligned_momentum = momentum_signal.reindex(target_weights.index).reindex(columns=target_weights.columns)
    aligned_over = overextension_signal.reindex(target_weights.index).reindex(columns=target_weights.columns)

    momentum_qualified = int((aligned_momentum > 0.0).sum().sum())
    blocked_by_entry_filter = int(
        ((aligned_momentum > 0.0) & (aligned_over >= entry_overextension_threshold)).sum().sum()
    )

    return {
        "strategy": strategy_name,
        "avg_active_positions": float(active_positions.mean()),
        "pct_timestamps_invested": percent_invested,
        "total_turnover": float(turnover.sum()),
        "avg_turnover": float(turnover.mean()),
        "num_entries": entries,
        "num_exits": exits,
        "momentum_qualified_opportunities": momentum_qualified,
        "entry_opportunities_blocked_by_overextension": blocked_by_entry_filter,
        "exits_triggered_by_overextension": exits_on_overextension,
        "exits_triggered_by_negative_momentum": exits_on_negative_momentum,
    }


def _distribution_row(label: str, values: pd.Series) -> dict[str, float | int | str]:
    """Build one percentile/threshold summary row for overextension values."""
    clean = values.dropna().astype(float)
    if clean.empty:
        return {
            "symbol": label,
            "observations": 0,
            "min": float("nan"),
            "p05": float("nan"),
            "p25": float("nan"),
            "median": float("nan"),
            "p75": float("nan"),
            "p90": float("nan"),
            "p95": float("nan"),
            "p99": float("nan"),
            "max": float("nan"),
            "count_gt_5pct": 0,
            "count_gt_10pct": 0,
            "count_gt_15pct": 0,
            "count_gt_20pct": 0,
            "count_gt_25pct": 0,
            "count_gt_30pct": 0,
        }

    return {
        "symbol": label,
        "observations": int(clean.shape[0]),
        "min": float(clean.min()),
        "p05": float(clean.quantile(0.05)),
        "p25": float(clean.quantile(0.25)),
        "median": float(clean.quantile(0.50)),
        "p75": float(clean.quantile(0.75)),
        "p90": float(clean.quantile(0.90)),
        "p95": float(clean.quantile(0.95)),
        "p99": float(clean.quantile(0.99)),
        "max": float(clean.max()),
        "count_gt_5pct": int((clean > 0.05).sum()),
        "count_gt_10pct": int((clean > 0.10).sum()),
        "count_gt_15pct": int((clean > 0.15).sum()),
        "count_gt_20pct": int((clean > 0.20).sum()),
        "count_gt_25pct": int((clean > 0.25).sum()),
        "count_gt_30pct": int((clean > 0.30).sum()),
    }


def _overextension_distribution_table(overextension_signal: pd.DataFrame) -> pd.DataFrame:
    """Summarize overextension signal distribution per symbol and overall."""
    rows: list[dict[str, float | int | str]] = []
    for symbol in overextension_signal.columns:
        rows.append(_distribution_row(symbol, overextension_signal[symbol]))

    overall = overextension_signal.stack().dropna()
    rows.append(_distribution_row("ALL", overall))
    return pd.DataFrame(rows)


def _parameter_sweep_grid(
    entry_thresholds: list[float],
    exit_thresholds: list[float],
) -> list[tuple[float, float]]:
    """Return valid (entry, exit) threshold pairs where exit >= entry."""
    grid: list[tuple[float, float]] = []
    for entry in entry_thresholds:
        for exit_ in exit_thresholds:
            if exit_ >= entry:
                grid.append((float(entry), float(exit_)))
    return grid


def _build_parameter_sweep_results(
    close: pd.DataFrame,
    momentum_signal: pd.DataFrame,
    overextension_signal: pd.DataFrame,
    bars_per_year: int,
    initial_capital: float,
    transaction_cost_bps: float,
    slippage_bps: float,
    entry_thresholds: list[float],
    exit_thresholds: list[float],
) -> pd.DataFrame:
    """Run threshold sweep for momentum overlay variants and return one consolidated table."""
    sweep_rows: list[dict[str, float | int | str]] = []
    asset_returns = close.pct_change().fillna(0.0)

    for entry_threshold, exit_threshold in _parameter_sweep_grid(entry_thresholds, exit_thresholds):
        variants: dict[str, pd.DataFrame] = {
            "momentum_with_entry_filter": momentum_with_entry_filter_weights(
                momentum_signal,
                overextension_signal,
                entry_overextension_threshold=entry_threshold,
            ),
            "momentum_with_exit_signal": momentum_with_exit_signal_weights(
                momentum_signal,
                overextension_signal,
                exit_overextension_threshold=exit_threshold,
            ),
            "momentum_with_entry_and_exit": momentum_with_entry_filter_and_exit_signal_weights(
                momentum_signal,
                overextension_signal,
                entry_overextension_threshold=entry_threshold,
                exit_overextension_threshold=exit_threshold,
            ),
        }

        for strategy_name, target_weights in variants.items():
            simulation = _simulate_from_weights(
                close=close,
                target_weights=target_weights,
                initial_capital=initial_capital,
                transaction_cost_bps=transaction_cost_bps,
                slippage_bps=slippage_bps,
            )
            metrics = summary_metrics(
                equity=simulation["equity"],
                returns=simulation["net_returns"],
                turnover=simulation["turnover"],
                bars_per_year=bars_per_year,
            )
            executed_weights = _align_weights_to_returns(target_weights, asset_returns)
            diagnostics = _variant_diagnostics_row(
                strategy_name=strategy_name,
                target_weights=target_weights,
                executed_weights=executed_weights,
                turnover=simulation["turnover"],
                momentum_signal=momentum_signal,
                overextension_signal=overextension_signal,
                entry_overextension_threshold=entry_threshold,
                exit_overextension_threshold=exit_threshold,
            )

            sweep_rows.append(
                {
                    "strategy": strategy_name,
                    "entry_threshold": float(entry_threshold),
                    "exit_threshold": float(exit_threshold),
                    "cagr": float(metrics.get("cagr", 0.0)),
                    "sharpe": float(metrics.get("sharpe", 0.0)),
                    "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
                    "avg_turnover": float(metrics.get("avg_turnover", 0.0)),
                    "total_turnover": float(metrics.get("total_turnover", 0.0)),
                    "total_return": float(metrics.get("total_return", 0.0)),
                    "final_equity": float(simulation["equity"].iloc[-1]),
                    "avg_active_positions": float(diagnostics["avg_active_positions"]),
                    "pct_timestamps_invested": float(diagnostics["pct_timestamps_invested"]),
                    "num_entries": int(diagnostics["num_entries"]),
                    "num_exits": int(diagnostics["num_exits"]),
                    "entry_opportunities_blocked_by_overextension": int(
                        diagnostics["entry_opportunities_blocked_by_overextension"]
                    ),
                    "exits_triggered_by_overextension": int(diagnostics["exits_triggered_by_overextension"]),
                    "exits_triggered_by_negative_momentum": int(
                        diagnostics["exits_triggered_by_negative_momentum"]
                    ),
                }
            )

    return pd.DataFrame(sweep_rows)


def run_parameter_sweep(
    symbols: tuple[str, ...] = DEFAULT_UNIVERSE,
    timeframe: str = "4h",
    output_path: Path = DEFAULT_PARAMETER_SWEEP_PATH,
    momentum_lookback_bars: int = 336,
    overextension_lookback_bars: int = 42,
    initial_capital: float = 10_000.0,
    transaction_cost_bps: float = 10.0,
    slippage_bps: float = 0.0,
    entry_thresholds: list[float] | None = None,
    exit_thresholds: list[float] | None = None,
) -> pd.DataFrame:
    """Run threshold sweep for momentum-overlay strategies and save consolidated CSV."""
    tf = _normalize_timeframe(timeframe)
    bars_per_year = bars_per_year_for_timeframe(tf)

    resolved_entry_thresholds = entry_thresholds or [0.05, 0.075, 0.10, 0.125]
    resolved_exit_thresholds = exit_thresholds or [0.10, 0.125, 0.15, 0.20]

    LOGGER.info("Loading OHLCV for %d symbols at %s (parameter sweep)", len(symbols), tf)
    ohlcv = load_ohlcv_history(
        symbols=symbols,
        timeframe=tf,
        data_dir=SETTINGS.data_dir,
    )
    close = pivot_close(ohlcv)
    momentum_signal = calculate_momentum_signal(close, lookback_bars=momentum_lookback_bars)
    overextension_signal = calculate_overextension_signal(
        close,
        lookback_bars=overextension_lookback_bars,
    )

    sweep = _build_parameter_sweep_results(
        close=close,
        momentum_signal=momentum_signal,
        overextension_signal=overextension_signal,
        bars_per_year=bars_per_year,
        initial_capital=initial_capital,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
        entry_thresholds=resolved_entry_thresholds,
        exit_thresholds=resolved_exit_thresholds,
    )

    if sweep.empty:
        raise ValueError("Parameter sweep produced no rows; check threshold grid inputs")

    sweep = sweep.sort_values(["strategy", "entry_threshold", "exit_threshold"]).reset_index(drop=True)
    print("\nTS Momentum + Reversal Parameter Sweep")
    print(sweep.to_string(index=False, float_format=lambda x: f"{x:.6f}"))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    sweep.to_csv(output_path, index=False)
    LOGGER.info("Saved parameter sweep results to %s", output_path)
    return sweep


def run_research(
    symbols: tuple[str, ...] = DEFAULT_UNIVERSE,
    timeframe: str = "4h",
    output_path: Path = DEFAULT_OUTPUT_PATH,
    diagnostics_output_path: Path = DEFAULT_DIAGNOSTICS_PATH,
    overextension_distribution_output_path: Path = DEFAULT_OVEREXTENSION_DISTRIBUTION_PATH,
    save_csv: bool = True,
    momentum_lookback_bars: int = 336,
    overextension_lookback_bars: int = 42,
    entry_overextension_threshold: float = 0.15,
    exit_overextension_threshold: float = 0.30,
    initial_capital: float = 10_000.0,
    transaction_cost_bps: float = 10.0,
    slippage_bps: float = 0.0,
) -> pd.DataFrame:
    """Run strategy comparison for momentum and reversal variants."""
    tf = _normalize_timeframe(timeframe)
    bars_per_year = bars_per_year_for_timeframe(tf)

    LOGGER.info("Loading OHLCV for %d symbols at %s", len(symbols), tf)
    ohlcv = load_ohlcv_history(
        symbols=symbols,
        timeframe=tf,
        data_dir=SETTINGS.data_dir,
    )
    close = pivot_close(ohlcv)

    momentum_signal = calculate_momentum_signal(close, lookback_bars=momentum_lookback_bars)
    overextension_signal = calculate_overextension_signal(
        close,
        lookback_bars=overextension_lookback_bars,
    )

    weight_variants: dict[str, pd.DataFrame] = {
        "momentum_only": time_series_momentum_weights(momentum_signal),
        "short_term_reversal_only": short_term_reversal_weights(overextension_signal),
        "momentum_with_entry_filter": momentum_with_entry_filter_weights(
            momentum_signal,
            overextension_signal,
            entry_overextension_threshold=entry_overextension_threshold,
        ),
        "momentum_with_exit_signal": momentum_with_exit_signal_weights(
            momentum_signal,
            overextension_signal,
            exit_overextension_threshold=exit_overextension_threshold,
        ),
        "momentum_with_entry_and_exit": momentum_with_entry_filter_and_exit_signal_weights(
            momentum_signal,
            overextension_signal,
            entry_overextension_threshold=entry_overextension_threshold,
            exit_overextension_threshold=exit_overextension_threshold,
        ),
    }
    asset_returns = close.pct_change().fillna(0.0)

    rows: list[dict[str, float | str]] = []
    diagnostics_rows: list[dict[str, float | int | str]] = []
    for strategy_name, target_weights in weight_variants.items():
        simulation = _simulate_from_weights(
            close=close,
            target_weights=target_weights,
            initial_capital=initial_capital,
            transaction_cost_bps=transaction_cost_bps,
            slippage_bps=slippage_bps,
        )
        metrics = summary_metrics(
            equity=simulation["equity"],
            returns=simulation["net_returns"],
            turnover=simulation["turnover"],
            bars_per_year=bars_per_year,
        )
        rows.append(
            _format_metrics_row(
                strategy_name=strategy_name,
                metrics=metrics,
                final_equity=float(simulation["equity"].iloc[-1]),
            )
        )
        executed_weights = _align_weights_to_returns(target_weights, asset_returns)
        diagnostics_rows.append(
            _variant_diagnostics_row(
                strategy_name=strategy_name,
                target_weights=target_weights,
                executed_weights=executed_weights,
                turnover=simulation["turnover"],
                momentum_signal=momentum_signal,
                overextension_signal=overextension_signal,
                entry_overextension_threshold=entry_overextension_threshold,
                exit_overextension_threshold=exit_overextension_threshold,
            )
        )

    comparison = pd.DataFrame(rows).sort_values("sharpe", ascending=False).reset_index(drop=True)
    diagnostics = pd.DataFrame(diagnostics_rows)
    overextension_distribution = _overextension_distribution_table(overextension_signal)

    print("\nTS Momentum + Reversal Research Comparison")
    print(comparison.to_string(index=False, float_format=lambda x: f"{x:.6f}"))
    print("\nTS Momentum + Reversal Diagnostics")
    print(diagnostics.to_string(index=False, float_format=lambda x: f"{x:.6f}"))
    print("\nOverextension Signal Distribution (1-week return)")
    print(overextension_distribution.to_string(index=False, float_format=lambda x: f"{x:.6f}"))

    if save_csv:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        comparison.to_csv(output_path, index=False)
        LOGGER.info("Saved research comparison to %s", output_path)

    diagnostics_output_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics.to_csv(diagnostics_output_path, index=False)
    LOGGER.info("Saved research diagnostics to %s", diagnostics_output_path)

    overextension_distribution_output_path.parent.mkdir(parents=True, exist_ok=True)
    overextension_distribution.to_csv(overextension_distribution_output_path, index=False)
    LOGGER.info(
        "Saved overextension signal distribution to %s",
        overextension_distribution_output_path,
    )

    return comparison


def main() -> None:
    """CLI entrypoint for momentum/reversal research comparison."""
    configure_logging()

    parser = argparse.ArgumentParser(
        description="Run modular time-series momentum and reversal strategy research",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_UNIVERSE),
        help="Universe symbols (default: BTC/USD ETH/USD SOL/USD XRP/USD AVAX/USD)",
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        default="4h",
        help="Data timeframe (default: 4h)",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="CSV output path for strategy metrics",
    )
    parser.add_argument(
        "--diagnostics-output-path",
        type=Path,
        default=DEFAULT_DIAGNOSTICS_PATH,
        help="CSV output path for diagnostics metrics",
    )
    parser.add_argument(
        "--overextension-distribution-output-path",
        type=Path,
        default=DEFAULT_OVEREXTENSION_DISTRIBUTION_PATH,
        help="CSV output path for overextension distribution diagnostics",
    )
    parser.add_argument(
        "--parameter-sweep-output-path",
        type=Path,
        default=DEFAULT_PARAMETER_SWEEP_PATH,
        help="CSV output path for parameter sweep results",
    )
    parser.add_argument(
        "--run-parameter-sweep",
        action="store_true",
        help="Run threshold parameter sweep for momentum overlay variants",
    )
    parser.add_argument(
        "--no-save-csv",
        action="store_true",
        help="Disable writing metrics CSV",
    )
    parser.add_argument("--momentum-lookback", type=int, default=336)
    parser.add_argument("--overextension-lookback", type=int, default=42)
    parser.add_argument("--entry-threshold", type=float, default=0.15)
    parser.add_argument("--exit-threshold", type=float, default=0.30)
    parser.add_argument("--initial-capital", type=float, default=10_000.0)
    parser.add_argument("--transaction-cost-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    args = parser.parse_args()

    if args.run_parameter_sweep:
        run_parameter_sweep(
            symbols=tuple(args.symbols),
            timeframe=args.timeframe,
            output_path=args.parameter_sweep_output_path,
            momentum_lookback_bars=args.momentum_lookback,
            overextension_lookback_bars=args.overextension_lookback,
            initial_capital=args.initial_capital,
            transaction_cost_bps=args.transaction_cost_bps,
            slippage_bps=args.slippage_bps,
        )
        return

    run_research(
        symbols=tuple(args.symbols),
        timeframe=args.timeframe,
        output_path=args.output_path,
        diagnostics_output_path=args.diagnostics_output_path,
        overextension_distribution_output_path=args.overextension_distribution_output_path,
        save_csv=not args.no_save_csv,
        momentum_lookback_bars=args.momentum_lookback,
        overextension_lookback_bars=args.overextension_lookback,
        entry_overextension_threshold=args.entry_threshold,
        exit_overextension_threshold=args.exit_threshold,
        initial_capital=args.initial_capital,
        transaction_cost_bps=args.transaction_cost_bps,
        slippage_bps=args.slippage_bps,
    )


if __name__ == "__main__":
    main()
