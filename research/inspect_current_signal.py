"""Inspect the current momentum snapshot across the full data universe."""

from __future__ import annotations

import logging

import pandas as pd

from config import SETTINGS, get_data_symbols
from data.fetch_ohlc import load_ohlcv_history, pivot_close
from strategy.momentum import check_regime_filter, compute_momentum_score, rank_symbols_for_date


def configure_logging() -> None:
    """Configure minimal logging for diagnostics."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def inspect_current_signal(
    symbols: tuple[str, ...] | None = None,
    timeframe: str = SETTINGS.timeframe,
    data_dir=None,
) -> dict[str, object]:
    """Return a read-only momentum snapshot for the latest completed bar."""
    selected_symbols = symbols or get_data_symbols()
    effective_data_dir = SETTINGS.data_dir if data_dir is None else data_dir

    ohlcv = load_ohlcv_history(
        symbols=selected_symbols,
        timeframe=timeframe,
        data_dir=effective_data_dir,
        downloader=None,
    )
    close = pivot_close(ohlcv)
    latest_ts = close.index[-1]

    momentum_score = compute_momentum_score(
        close=close,
        short_lookback_bars=SETTINGS.short_lookback_bars,
        medium_lookback_bars=SETTINGS.medium_lookback_bars,
        short_weight=SETTINGS.short_weight,
        medium_weight=SETTINGS.medium_weight,
    )

    risk_on = True
    if SETTINGS.use_regime_filter:
        risk_on = check_regime_filter(
            close=close,
            rebalance_timestamp=latest_ts,
            btc_symbol=SETTINGS.btc_symbol,
            ma_lookback_bars=SETTINGS.regime_lookback_bars,
        )

    score_row = momentum_score.loc[latest_ts]
    if isinstance(score_row, pd.DataFrame):
        score_row = score_row.iloc[0]
    ranked_scores = score_row.dropna().sort_values(ascending=False)
    selected_if_full_universe = (
        rank_symbols_for_date(
            momentum_score=momentum_score,
            rebalance_timestamp=latest_ts,
            top_n=SETTINGS.top_n,
        )
        if risk_on
        else []
    )

    rankings = [
        {
            "rank": rank,
            "symbol": symbol,
            "score": float(score),
        }
        for rank, (symbol, score) in enumerate(ranked_scores.items(), start=1)
    ]

    return {
        "timestamp": latest_ts,
        "risk_on": risk_on,
        "rankings": rankings,
        "selected_if_full_universe": selected_if_full_universe,
        "top_n": SETTINGS.top_n,
        "symbols": selected_symbols,
    }


def print_signal_snapshot(snapshot: dict[str, object]) -> None:
    """Print a human-readable momentum snapshot."""
    print("\n" + "=" * 80)
    print("CURRENT MOMENTUM SNAPSHOT")
    print("=" * 80)
    print(f"Latest Completed Bar: {snapshot['timestamp']}")
    print(f"Data Universe:         {', '.join(snapshot['symbols'])}")
    print(f"Risk-On:               {snapshot['risk_on']}")
    print("\nMomentum Rankings:")
    for row in snapshot["rankings"]:
        print(f"  {row['rank']:>2d}. {row['symbol']:8s}  score={row['score']:.6f}")
    print(
        f"\nSelected If Full Universe Were Tradable (top {snapshot['top_n']}): "
        f"{', '.join(snapshot['selected_if_full_universe']) or 'none'}"
    )
    print("=" * 80 + "\n")


def main() -> None:
    """Load local data and print the current full-universe momentum snapshot."""
    configure_logging()
    logger = logging.getLogger(__name__)
    data_symbols = get_data_symbols()
    logger.info("Inspecting current signal across data universe: %s", ", ".join(data_symbols))
    snapshot = inspect_current_signal(symbols=data_symbols)
    print_signal_snapshot(snapshot)


if __name__ == "__main__":
    main()
