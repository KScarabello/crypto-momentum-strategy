"""Compare locked_baseline backtests across multiple symbol universes."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from backtest.engine import run_momentum_rotation_backtest
from backtest.metrics import summary_metrics
from config import SETTINGS
from data.fetch_ohlc import build_historical_downloader, load_ohlcv_history
from research.run_backtest import bars_per_year_for_timeframe

UNIVERSE_MAP: dict[str, tuple[str, ...]] = {
    "three_asset": ("BTC/USD", "ETH/USD", "XRP/USD"),
    "five_asset": ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD"),
}

COMMON_START_TIMESTAMP = pd.Timestamp("2020-09-22 08:00:00+00:00")
DEFAULT_OUTPUT_PATH = Path("outputs/universe_comparison.csv")


def configure_logging() -> None:
    """Configure minimal logging for research comparisons."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_downloader_if_enabled():
    """Build optional historical downloader from current settings."""
    if not SETTINGS.use_downloader:
        return None
    return build_historical_downloader(
        provider=SETTINGS.historical_data_provider,
        exchange_name=SETTINGS.historical_exchange_name,
        since=SETTINGS.historical_since,
        max_batches=SETTINGS.historical_max_batches,
        max_rows=SETTINGS.historical_max_rows,
        limit_per_request=SETTINGS.historical_limit_per_request,
        request_pause_seconds=SETTINGS.historical_request_pause_seconds,
    )


def _load_universe_ohlcv(
    name: str,
    symbols: tuple[str, ...],
    timeframe: str,
    downloader,
) -> pd.DataFrame:
    """Load and clip one universe to the shared start timestamp."""
    logger = logging.getLogger(__name__)
    logger.info("Loading %s universe (%d symbols): %s", name, len(symbols), ", ".join(symbols))
    ohlcv = load_ohlcv_history(
        symbols=symbols,
        timeframe=timeframe,
        data_dir=SETTINGS.data_dir,
        downloader=downloader,
    )
    clipped = ohlcv.loc[ohlcv["timestamp"] >= COMMON_START_TIMESTAMP].copy()
    if clipped.empty:
        raise ValueError(f"No OHLCV data at or after {COMMON_START_TIMESTAMP} for universe {name}")
    return clipped


def compare_universes(
    output_path: Path = DEFAULT_OUTPUT_PATH,
    save_csv: bool = True,
) -> pd.DataFrame:
    """Run locked_baseline backtests for both universes over the same date range."""
    logger = logging.getLogger(__name__)
    timeframe = "4h"
    rebalance_every_bars = 6
    bars_per_year = bars_per_year_for_timeframe(timeframe)

    downloader = _build_downloader_if_enabled()

    loaded: dict[str, pd.DataFrame] = {}
    for name, symbols in UNIVERSE_MAP.items():
        loaded[name] = _load_universe_ohlcv(
            name=name,
            symbols=symbols,
            timeframe=timeframe,
            downloader=downloader,
        )

    common_end = min(frame["timestamp"].max() for frame in loaded.values())
    logger.info("Comparison date range: %s -> %s", COMMON_START_TIMESTAMP, common_end)

    results: list[dict[str, object]] = []
    for name, ohlcv in loaded.items():
        clipped = ohlcv.loc[ohlcv["timestamp"] <= common_end].copy()

        bt = run_momentum_rotation_backtest(
            ohlcv=clipped,
            top_n=SETTINGS.top_n,
            rebalance_every_bars=rebalance_every_bars,
            rebalance_hour_utc=SETTINGS.rebalance_hour_utc,
            short_lookback_bars=SETTINGS.short_lookback_bars,
            medium_lookback_bars=SETTINGS.medium_lookback_bars,
            short_weight=SETTINGS.short_weight,
            medium_weight=SETTINGS.medium_weight,
            btc_symbol=SETTINGS.btc_symbol,
            regime_ma_lookback_bars=SETTINGS.regime_lookback_bars,
            use_regime_filter=SETTINGS.use_regime_filter,
            max_position_weight=SETTINGS.max_position_weight,
            max_gross_exposure=SETTINGS.max_gross_exposure,
            transaction_cost_bps=SETTINGS.transaction_cost_bps,
            slippage_bps=SETTINGS.slippage_bps,
            initial_capital=SETTINGS.initial_capital,
            min_history_bars=SETTINGS.min_history_bars,
            min_eligible_assets=SETTINGS.min_eligible_assets,
            min_median_volume=SETTINGS.min_median_volume,
            max_turnover_per_rebalance=SETTINGS.max_turnover_per_rebalance,
        )

        metrics = summary_metrics(
            equity=bt.portfolio["equity"],
            returns=bt.portfolio["strategy_return"],
            turnover=bt.turnover,
            rebalance_log=bt.rebalance_log,
            bars_per_year=bars_per_year,
        )

        results.append(
            {
                "universe": name,
                "cagr": float(metrics.get("cagr", 0.0)),
                "sharpe": float(metrics.get("sharpe", 0.0)),
                "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
                "avg_turnover": float(metrics.get("avg_turnover", 0.0)),
                "rebalance_count": int(metrics.get("rebalance_count", 0.0)),
                "final_equity": float(bt.portfolio["equity"].iloc[-1]),
            }
        )

    comparison = pd.DataFrame(results)

    display = comparison.copy()
    for col in ["cagr", "sharpe", "max_drawdown", "avg_turnover"]:
        display[col] = display[col].map(lambda x: f"{x:.6f}")
    display["final_equity"] = display["final_equity"].map(lambda x: f"{x:,.2f}")

    print("\nUniverse Comparison (locked_baseline)")
    print(display.to_string(index=False))

    if save_csv:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        comparison.to_csv(output_path, index=False)
        logger.info("Saved universe comparison to %s", output_path)

    return comparison


def main() -> None:
    """CLI entrypoint for universe-comparison backtests."""
    configure_logging()
    parser = argparse.ArgumentParser(description="Compare locked_baseline backtests across symbol universes")
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="CSV output path for comparison results",
    )
    parser.add_argument(
        "--no-save-csv",
        action="store_true",
        help="Disable writing CSV output",
    )
    args = parser.parse_args()

    compare_universes(
        output_path=args.output_path,
        save_csv=not args.no_save_csv,
    )


if __name__ == "__main__":
    main()
