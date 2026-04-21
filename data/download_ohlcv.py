"""Download and persist historical OHLCV data for research."""

from __future__ import annotations

import logging

from config import SETTINGS
from data.fetch_ohlc import build_historical_downloader, merge_and_save_symbol_ohlcv


def configure_logging() -> None:
    """Configure readable logging for downloader runs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def download_all_symbols() -> None:
    """Download OHLCV for configured symbols and persist per-symbol files."""
    logger = logging.getLogger(__name__)
    primary_downloader = build_historical_downloader(
        provider=SETTINGS.historical_data_provider,
        exchange_name=SETTINGS.historical_exchange_name,
        since=SETTINGS.historical_since,
        max_batches=SETTINGS.historical_max_batches,
        max_rows=SETTINGS.historical_max_rows,
        limit_per_request=SETTINGS.historical_limit_per_request,
        request_pause_seconds=SETTINGS.historical_request_pause_seconds,
    )

    fallback_downloader = None
    if SETTINGS.historical_fallback_provider:
        fallback_downloader = build_historical_downloader(
            provider=SETTINGS.historical_fallback_provider,
            exchange_name=None,
            since=SETTINGS.historical_since,
            max_batches=SETTINGS.historical_max_batches,
            max_rows=SETTINGS.historical_max_rows,
            limit_per_request=SETTINGS.historical_limit_per_request,
            request_pause_seconds=SETTINGS.historical_request_pause_seconds,
        )

    logger.info(
        "Starting OHLCV download: provider=%s exchange=%s timeframe=%s symbols=%d fallback=%s",
        SETTINGS.historical_data_provider,
        SETTINGS.historical_exchange_name or "auto",
        SETTINGS.timeframe,
        len(SETTINGS.symbols),
        SETTINGS.historical_fallback_provider or "none",
    )

    for symbol in SETTINGS.symbols:
        logger.info("Processing symbol %s", symbol)
        try:
            downloaded = primary_downloader(
                symbol=symbol,
                timeframe=SETTINGS.timeframe,
            )
        except Exception as exc:
            if fallback_downloader is None:
                raise
            logger.warning(
                "Primary provider failed for %s (%s). Retrying with fallback provider %s.",
                symbol,
                exc,
                SETTINGS.historical_fallback_provider,
            )
            downloaded = fallback_downloader(
                symbol=symbol,
                timeframe=SETTINGS.timeframe,
            )

        merged = merge_and_save_symbol_ohlcv(
            symbol=symbol,
            timeframe=SETTINGS.timeframe,
            new_data=downloaded,
            data_dir=SETTINGS.data_dir,
            overwrite=SETTINGS.historical_overwrite,
        )

        min_ts = merged["timestamp"].min()
        max_ts = merged["timestamp"].max()
        logger.info(
            "Saved %d rows for %s | %s -> %s",
            len(merged),
            symbol,
            min_ts,
            max_ts,
        )


def main() -> None:
    """Entry point for downloading and persisting OHLCV history."""
    configure_logging()
    download_all_symbols()


if __name__ == "__main__":
    main()
