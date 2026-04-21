"""Run the end-to-end momentum rotation research backtest."""

from __future__ import annotations

import logging
from pathlib import Path

from backtest.engine import BacktestResult, run_momentum_rotation_backtest
from backtest.metrics import summary_metrics
from config import SETTINGS
from data.fetch_ohlc import build_historical_downloader, load_ohlcv_history


def configure_logging() -> None:
    """Configure minimal project logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def bars_per_year_for_timeframe(timeframe: str) -> int:
    """Return annualization factor for supported research timeframes."""
    tf = timeframe.strip().lower()
    mapping = {
        "1d": 365,
        "d": 365,
        "daily": 365,
        "4h": 365 * 6,
    }
    if tf not in mapping:
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Use 4h or 1d.")
    return mapping[tf]


def write_outputs(result: BacktestResult, output_dir: Path) -> dict[str, Path]:
    """Write backtest outputs to local CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    equity_path = output_dir / "equity_curve.csv"
    rebalance_path = output_dir / "rebalance_log.csv"
    holdings_path = output_dir / "holdings_history.csv"

    result.portfolio[["equity"]].to_csv(equity_path, index_label="timestamp")
    result.rebalance_log.to_csv(rebalance_path, index=False)
    result.holdings_history.to_csv(holdings_path, index_label="timestamp")

    return {
        "equity": equity_path,
        "rebalance_log": rebalance_path,
        "holdings_history": holdings_path,
    }


def print_summary(metrics: dict[str, float]) -> None:
    """Print concise summary metrics to console."""
    ordered_keys = [
        "gross_total_return",
        "net_total_return",
        "cost_drag_total_return",
        "total_return",
        "cagr",
        "annualized_volatility",
        "sharpe",
        "max_drawdown",
        "pct_time_invested",
        "avg_holdings_count",
        "rebalance_count",
        "avg_turnover",
        "median_turnover",
        "max_turnover",
        "total_turnover",
    ]
    print("\nPerformance Summary")
    for key in ordered_keys:
        if key in metrics:
            print(f"- {key}: {metrics[key]:.6f}")


def print_sanity_summary(result: BacktestResult, ohlcv) -> None:
    """Print concise realism diagnostics alongside headline metrics."""
    equity = result.portfolio["equity"]
    running_max = equity.cummax()
    drawdown = equity / running_max - 1.0
    trough_ts = drawdown.idxmin()
    peak_ts = equity.loc[:trough_ts].idxmax()

    first_tradable = (
        ohlcv.groupby("symbol", as_index=True)["timestamp"].min().sort_values()
    )

    print("\nSanity Checks")
    print(f"- worst_drawdown_peak: {peak_ts}")
    print(f"- worst_drawdown_trough: {trough_ts}")
    print("- first_tradable_dates:")
    for symbol, ts in first_tradable.items():
        print(f"  {symbol}: {ts}")


def main() -> None:
    """Load data, run backtest, print metrics, and save output files."""
    configure_logging()
    logger = logging.getLogger(__name__)
    downloader = None
    if SETTINGS.use_downloader:
        downloader = build_historical_downloader(
            provider=SETTINGS.historical_data_provider,
            exchange_name=SETTINGS.historical_exchange_name,
            since=SETTINGS.historical_since,
            max_batches=SETTINGS.historical_max_batches,
            max_rows=SETTINGS.historical_max_rows,
            limit_per_request=SETTINGS.historical_limit_per_request,
            request_pause_seconds=SETTINGS.historical_request_pause_seconds,
        )

    if SETTINGS.use_downloader:
        logger.info(
            "Downloader fallback enabled (provider=%s, exchange=%s)",
            SETTINGS.historical_data_provider,
            SETTINGS.historical_exchange_name or "auto",
        )
    else:
        logger.info("Downloader fallback disabled; loading local files only")

    logger.info("Loading OHLCV data for %d symbols at %s", len(SETTINGS.symbols), SETTINGS.timeframe)
    ohlcv = load_ohlcv_history(
        symbols=SETTINGS.symbols,
        timeframe=SETTINGS.timeframe,
        data_dir=SETTINGS.data_dir,
        downloader=downloader,
    )

    result = run_momentum_rotation_backtest(
        ohlcv=ohlcv,
        top_n=SETTINGS.top_n,
        rebalance_every_bars=SETTINGS.rebalance_every_bars,
        short_lookback_bars=SETTINGS.short_lookback_bars,
        medium_lookback_bars=SETTINGS.medium_lookback_bars,
        short_weight=SETTINGS.short_weight,
        medium_weight=SETTINGS.medium_weight,
        btc_symbol=SETTINGS.btc_symbol,
        regime_ma_lookback_bars=SETTINGS.regime_lookback_bars,
        transaction_cost_bps=SETTINGS.transaction_cost_bps,
        slippage_bps=SETTINGS.slippage_bps,
        initial_capital=SETTINGS.initial_capital,
        min_history_bars=SETTINGS.min_history_bars,
        min_eligible_assets=SETTINGS.min_eligible_assets,
        min_median_volume=SETTINGS.min_median_volume,
        max_turnover_per_rebalance=SETTINGS.max_turnover_per_rebalance,
    )

    bars_per_year = bars_per_year_for_timeframe(SETTINGS.timeframe)
    metrics = summary_metrics(
        equity=result.portfolio["equity"],
        returns=result.portfolio["strategy_return"],
        gross_returns=result.gross_return,
        turnover=result.turnover,
        holdings_history=result.holdings_history,
        rebalance_log=result.rebalance_log,
        bars_per_year=bars_per_year,
    )

    paths = write_outputs(result=result, output_dir=SETTINGS.output_dir)
    print_summary(metrics)
    print_sanity_summary(result=result, ohlcv=ohlcv)

    logger.info("Saved equity curve to %s", paths["equity"])
    logger.info("Saved rebalance log to %s", paths["rebalance_log"])
    logger.info("Saved holdings history to %s", paths["holdings_history"])


if __name__ == "__main__":
    main()
