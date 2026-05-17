"""Research-only utility to fetch/update expanded-universe 4h OHLCV and summarize eligibility."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from data.fetch_ohlc import (
    ccxt_downloader,
    local_symbol_file_path,
    load_local_symbol_ohlcv,
    merge_and_save_symbol_ohlcv,
    update_symbol_ohlcv_incremental,
)
from research.asset_eligibility import eligibility_summary_table

LOGGER = logging.getLogger(__name__)

EXPANDED_RESEARCH_UNIVERSE: tuple[str, ...] = (
    "BTC/USD",
    "ETH/USD",
    "SOL/USD",
    "XRP/USD",
    "AVAX/USD",
    "ADA/USD",
    "DOGE/USD",
    "LINK/USD",
    "DOT/USD",
    "LTC/USD",
    "BCH/USD",
    "UNI/USD",
    "AAVE/USD",
    "ATOM/USD",
    "NEAR/USD",
    "APT/USD",
    "ARB/USD",
    "OP/USD",
    "INJ/USD",
    "POL/USD",
    "MATIC/USD",
)

DEBUG_UNIVERSE: tuple[str, ...] = ("BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "AVAX/USD")

DEFAULT_SUMMARY_PATH = Path("research/results/expanded_universe_data_summary.csv")


def configure_logging() -> None:
    """Configure readable logging for expanded-universe data runs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_symbols_argument(symbols_arg: str | None) -> tuple[str, ...]:
    """Parse optional comma-separated symbol override list."""
    if symbols_arg is None or not symbols_arg.strip():
        return EXPANDED_RESEARCH_UNIVERSE
    symbols = [token.strip().upper() for token in symbols_arg.split(",") if token.strip()]
    return tuple(symbols)


def symbol_to_local_filename(symbol: str, timeframe: str) -> str:
    """Return canonical local filename for a symbol/timeframe pair."""
    return local_symbol_file_path(symbol=symbol, timeframe=timeframe).name


def split_available_symbols(
    requested_symbols: tuple[str, ...],
    available_symbols: set[str],
) -> tuple[list[str], list[str]]:
    """Split symbols into available vs skipped groups."""
    fetched: list[str] = []
    skipped: list[str] = []
    for symbol in requested_symbols:
        if symbol in available_symbols:
            fetched.append(symbol)
        else:
            skipped.append(symbol)
    return fetched, skipped


def _normalize_exchange_symbols(markets: dict[str, Any]) -> set[str]:
    """Extract normalized spot symbols from ccxt market metadata."""
    available: set[str] = set()
    for symbol, meta in markets.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("active") is False:
            continue
        market_type = str(meta.get("type", "spot")).lower()
        if market_type not in {"spot", ""}:
            continue
        available.add(str(symbol).upper())
    return available


def _load_exchange_available_symbols() -> tuple[Any, set[str]]:
    """Instantiate Kraken exchange and return active available symbols."""
    try:
        import ccxt  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError("ccxt is required for expanded-universe data fetch. Install with: pip install ccxt") from exc

    exchange = ccxt.kraken({"enableRateLimit": True})
    markets = exchange.load_markets()
    available = _normalize_exchange_symbols(markets)
    return exchange, available


def _close_matrix_from_local_files(
    symbols: tuple[str, ...],
    timeframe: str,
    data_dir: Path,
) -> pd.DataFrame:
    """Build a close-price matrix from per-symbol local files without backfilling inception gaps."""
    close_series_by_symbol: dict[str, pd.Series] = {}
    for symbol in symbols:
        frame = load_local_symbol_ohlcv(symbol=symbol, timeframe=timeframe, data_dir=data_dir)
        if frame.empty:
            close_series_by_symbol[symbol] = pd.Series(dtype=float)
            continue
        series = (
            frame[["timestamp", "close"]]
            .dropna(subset=["timestamp", "close"])
            .drop_duplicates(subset=["timestamp"], keep="last")
            .set_index("timestamp")["close"]
            .sort_index()
            .astype(float)
        )
        close_series_by_symbol[symbol] = series

    close = pd.DataFrame(close_series_by_symbol)
    if close.empty:
        return pd.DataFrame(columns=list(symbols))
    close = close.sort_index()
    for symbol in symbols:
        if symbol not in close.columns:
            close[symbol] = pd.Series(index=close.index, dtype=float)
    return close.reindex(columns=list(symbols))


def _log_file_summary(symbol: str, merged: pd.DataFrame) -> None:
    """Log row count and timestamp range for one symbol after save/update."""
    if merged.empty:
        LOGGER.info("No rows saved for %s", symbol)
        return
    first_ts = merged["timestamp"].min()
    last_ts = merged["timestamp"].max()
    LOGGER.info(
        "Saved %s | rows=%d range=%s -> %s",
        symbol,
        len(merged),
        first_ts,
        last_ts,
    )


def _fetch_or_update_symbol(
    symbol: str,
    timeframe: str,
    data_dir: Path,
    exchange: Any,
    since: str | None,
    incremental_updater: Callable[..., dict[str, Any]],
    historical_downloader: Callable[..., pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Fetch/update one symbol and return merged local frame."""
    if since is None:
        result = incremental_updater(
            symbol=symbol,
            timeframe=timeframe,
            data_dir=data_dir,
            exchange=exchange,
            limit=720,
        )
        merged = load_local_symbol_ohlcv(symbol=symbol, timeframe=timeframe, data_dir=data_dir)
        LOGGER.info(
            "Incremental update complete for %s | fetched=%d dropped=%d total=%d",
            symbol,
            int(result.get("fetched_rows", 0)),
            int(result.get("dropped_rows", 0)),
            int(result.get("final_rows", len(merged))),
        )
        return merged

    downloader = historical_downloader if historical_downloader is not None else ccxt_downloader

    existing = load_local_symbol_ohlcv(symbol=symbol, timeframe=timeframe, data_dir=data_dir)
    had_existing_data = not existing.empty
    existing_row_count = len(existing)

    LOGGER.info(
        "Fetching %s | requested_since=%s had_local_data=%s existing_rows=%d",
        symbol,
        since,
        had_existing_data,
        existing_row_count,
    )

    downloaded = downloader(
        symbol=symbol,
        timeframe=timeframe,
        exchange_name="kraken",
        since=since,
        max_batches=400,
        limit_per_request=720,
    )

    rows_downloaded = len(downloaded)
    estimated_batches = max(1, (rows_downloaded + 719) // 720) if rows_downloaded > 0 else 0

    first_returned_ts: pd.Timestamp | None = None
    latest_returned_ts: pd.Timestamp | None = None
    if not downloaded.empty and "timestamp" in downloaded.columns:
        ts_col = pd.to_datetime(downloaded["timestamp"], utc=True, errors="coerce")
        valid_ts = ts_col.dropna()
        if not valid_ts.empty:
            first_returned_ts = valid_ts.min()
            latest_returned_ts = valid_ts.max()

    LOGGER.info(
        "%s | exchange_rows=%d estimated_batches=%d first_returned=%s latest_returned=%s",
        symbol,
        rows_downloaded,
        estimated_batches,
        first_returned_ts,
        latest_returned_ts,
    )

    if since is not None and first_returned_ts is not None:
        since_ts = pd.to_datetime(since, utc=True)
        gap_days = (first_returned_ts - since_ts).total_seconds() / 86400
        if gap_days > 30:
            LOGGER.warning(
                "%s | First returned timestamp (%s) is %.0f days after requested since (%s). "
                "Older history may not be available from this source or current fetch method.",
                symbol,
                first_returned_ts.date(),
                gap_days,
                since_ts.date(),
            )

    merged = merge_and_save_symbol_ohlcv(
        symbol=symbol,
        timeframe=timeframe,
        new_data=downloaded,
        data_dir=data_dir,
        overwrite=False,
    )

    LOGGER.info(
        "%s | final_rows=%d (prior=%d new_after_merge=%d)",
        symbol,
        len(merged),
        existing_row_count,
        len(merged) - existing_row_count,
    )
    return merged


def _print_run_summary(
    requested_symbols: tuple[str, ...],
    fetched_symbols: list[str],
    skipped_symbols: list[str],
    summary: pd.DataFrame,
) -> None:
    """Print consolidated run summary."""
    print("\nExpanded Universe Data Summary")
    print(f"- requested_symbol_count: {len(requested_symbols)}")
    print(f"- fetched_symbol_count: {len(fetched_symbols)}")
    print(f"- skipped_symbol_count: {len(skipped_symbols)}")
    print(f"- fetched_symbols: {', '.join(fetched_symbols) if fetched_symbols else 'none'}")
    print(f"- skipped_symbols: {', '.join(skipped_symbols) if skipped_symbols else 'none'}")
    if not summary.empty:
        display_cols = [
            "symbol",
            "first_timestamp",
            "latest_timestamp",
            "row_count",
            "first_eligible_timestamp_for_strategy",
            "is_eligible_for_strategy",
        ]
        print(summary[display_cols].to_string(index=False))


def fetch_expanded_universe_data(
    symbols: tuple[str, ...],
    timeframe: str = "4h",
    since: str | None = None,
    dry_run: bool = False,
    data_dir: Path = Path("data/local"),
    summary_output_path: Path = DEFAULT_SUMMARY_PATH,
    available_symbols_override: set[str] | None = None,
    incremental_updater: Callable[..., dict[str, Any]] = update_symbol_ohlcv_incremental,
    historical_downloader: Callable[..., pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Fetch/update expanded-universe local OHLCV and return eligibility summary."""
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    exchange: Any | None = None
    if available_symbols_override is None:
        exchange, available_symbols = _load_exchange_available_symbols()
    else:
        available_symbols = {s.upper() for s in available_symbols_override}

    fetched_symbols, skipped_symbols = split_available_symbols(symbols, available_symbols)

    if "POL/USD" in symbols and "POL/USD" not in available_symbols:
        LOGGER.warning("POL/USD unavailable from source; skipping")
    if "MATIC/USD" in symbols and "MATIC/USD" not in available_symbols:
        LOGGER.warning("MATIC/USD unavailable or ambiguous from source; skipping")

    for symbol in skipped_symbols:
        LOGGER.warning("Skipping unavailable symbol: %s", symbol)

    for symbol in fetched_symbols:
        planned_path = local_symbol_file_path(symbol=symbol, timeframe=timeframe, data_dir=data_dir)
        if dry_run:
            LOGGER.info("[DRY-RUN] Would fetch %s -> %s", symbol, planned_path)
            continue

        if exchange is None:
            exchange, _ = _load_exchange_available_symbols()

        merged = _fetch_or_update_symbol(
            symbol=symbol,
            timeframe=timeframe,
            data_dir=data_dir,
            exchange=exchange,
            since=since,
            incremental_updater=incremental_updater,
            historical_downloader=historical_downloader,
        )
        _log_file_summary(symbol, merged)

    close = _close_matrix_from_local_files(symbols=symbols, timeframe=timeframe, data_dir=data_dir)

    if close.empty:
        summary = pd.DataFrame(
            {
                "symbol": list(symbols),
                "first_timestamp": [pd.NaT] * len(symbols),
                "latest_timestamp": [pd.NaT] * len(symbols),
                "row_count": [0] * len(symbols),
                "first_eligible_timestamp_for_1w_signal": [pd.NaT] * len(symbols),
                "first_eligible_timestamp_for_2w_signal": [pd.NaT] * len(symbols),
                "first_eligible_timestamp_for_8w_signal": [pd.NaT] * len(symbols),
                "first_eligible_timestamp_for_strategy": [pd.NaT] * len(symbols),
                "is_eligible_for_strategy": [False] * len(symbols),
            }
        )
    else:
        summary = eligibility_summary_table(
            close=close,
            strategy_lookback_bars=336,
            signal_lookbacks=(42, 84, 336),
        )

    summary = summary.sort_values("symbol").reset_index(drop=True)

    for _, row in summary.iterrows():
        symbol = str(row["symbol"])
        row_count = int(row.get("row_count", 0))
        if row_count < 336:
            LOGGER.warning(
                "%s has fewer than 336 bars (%d) and is not ready for 8-week momentum strategy",
                symbol,
                row_count,
            )

    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_output_path, index=False)
    LOGGER.info("Saved expanded-universe summary to %s", summary_output_path)

    _print_run_summary(
        requested_symbols=symbols,
        fetched_symbols=fetched_symbols,
        skipped_symbols=skipped_symbols,
        summary=summary,
    )

    return summary


def main() -> None:
    """CLI entrypoint for expanded-universe research data fetch/update."""
    configure_logging()

    parser = argparse.ArgumentParser(description="Fetch/update expanded research-universe OHLCV")
    parser.add_argument("--timeframe", type=str, default="4h")
    parser.add_argument("--since", type=str, default=None, help="Optional since date (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Optional comma-separated symbol override list",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check availability and planned file paths without downloading",
    )
    parser.add_argument(
        "--summary-output-path",
        type=Path,
        default=DEFAULT_SUMMARY_PATH,
        help="CSV output path for expanded-universe data summary",
    )
    args = parser.parse_args()

    symbols = parse_symbols_argument(args.symbols)
    fetch_expanded_universe_data(
        symbols=symbols,
        timeframe=args.timeframe,
        since=args.since,
        dry_run=args.dry_run,
        summary_output_path=args.summary_output_path,
    )


if __name__ == "__main__":
    main()
