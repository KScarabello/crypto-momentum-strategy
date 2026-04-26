"""Autonomous scheduled rebalancing orchestrator for live crypto trading.

This is the single entrypoint for automated trading via scheduler (cron).
Call this every 4 hours to execute the complete pipeline:

  1. Refresh local OHLCV data
  2. Check data freshness
  3. Enforce rebalance timing
  4. Execute one-bar-delayed signals
  5. Send notifications

Usage:
  # Dry-run (default, safe)
  $ python -m live.run_scheduled_cycle --broker-source mock

  # Real execution (requires credentials in .env or environment)
  $ python -m live.run_scheduled_cycle --broker-source real --live

Expected to be called by cron every 4 hours:
  0 */4 * * * cd /path/to/crypto-momentum && /path/to/venv/bin/python -m live.run_scheduled_cycle --broker-source real --live

Non-rebalance bars: script logs and exits safely (no orders, no errors).
Decision bar: saves pending signal (one-bar-delayed execution).
Execution bar: executes pending signal and clears it.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

from config import SETTINGS, get_data_symbols, get_trading_symbols
from data.download_ohlcv import download_all_symbols
from live.generate_targets import generate_targets
from live.signal_state import has_pending_signal, load_pending_signal


def configure_logging() -> logging.Logger:
    """Configure detailed logging for autonomous operation."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | [scheduled_cycle] %(message)s",
    )
    return logging.getLogger(__name__)


def _refresh_ohlcv_data(symbols: tuple[str, ...], timeframe: str = "4h") -> bool:
    """Refresh local OHLCV data from configured provider for all symbols.
    
    Args:
        symbols: Tuple of symbols to download and refresh.
        timeframe: Timeframe for OHLCV bars to refresh.
    
    Returns:
        True if refresh succeeded, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info("Refreshing local OHLCV data from configured provider")
    logger.info("Data symbols selected for refresh: %s", ", ".join(symbols))
    
    try:
        download_all_symbols(symbols=symbols, timeframe=timeframe)
        logger.info("OHLCV data refresh completed successfully")
        return True
    except Exception as exc:
        logger.error(f"OHLCV data refresh failed: {exc}")
        return False


def _verify_data_freshness() -> bool:
    """Verify that loaded OHLCV data is fresh enough for trading.
    
    Returns:
        True if data is fresh, False otherwise
    """
    logger = logging.getLogger(__name__)
    trading_symbols = get_trading_symbols()
    logger.info("Trading symbols selected for signal checks: %s", ", ".join(trading_symbols))
    
    try:
        result = generate_targets(symbols=trading_symbols)
        if not result.get("data_fresh", False):
            logger.error(
                f"Stale OHLCV data: latest bar {result['timestamp']} "
                f"is older than maximum acceptable age"
            )
            return False
        logger.info(f"Data freshness verified: latest bar {result['timestamp']}")
        return True
    except Exception as exc:
        logger.error(f"Data freshness check failed: {exc}")
        return False


def _check_rebalance_timing() -> bool | None:
    """Check if current bar is a rebalance bar.
    
    Returns:
        True if rebalance bar, False if non-rebalance (safe to exit), None on error
    """
    logger = logging.getLogger(__name__)
    trading_symbols = get_trading_symbols()
    
    try:
        result = generate_targets(symbols=trading_symbols)
        if result.get("is_rebalance_bar", False):
            logger.info(f"Rebalance bar confirmed: {result['timestamp']}")
            return True
        logger.info(f"Non-rebalance bar: {result['timestamp']}")
        return False
    except Exception as exc:
        logger.error(f"Rebalance timing check failed: {exc}")
        return None


def _check_pending_signal_state() -> str:
    """Check pending signal state and return phase indicator.
    
    Returns:
        "decision": No pending signal, this is decision bar (save it)
        "execution": Pending signal exists, ready to execute
        "wait": Error state
    """
    logger = logging.getLogger(__name__)
    
    if has_pending_signal():
        pending = load_pending_signal()
        logger.info(
            f"Pending signal found from decision bar {pending['timestamp']}. "
            f"Ready for execution on next bar."
        )
        return "execution"
    else:
        logger.info("No pending signal. This will be the decision bar.")
        return "decision"


def main() -> None:
    """Orchestrate the complete autonomous trading cycle."""
    logger = configure_logging()

    parser = argparse.ArgumentParser(
        description="Autonomous scheduled rebalancing cycle for live crypto trading"
    )
    parser.add_argument(
        "--broker-source",
        choices=["mock", "real"],
        default="mock",
        help="Broker source: mock (dry-run) or real (Kraken)",
    )
    parser.add_argument(
        "--broker-name",
        choices=["kraken"],
        default="kraken",
        help="Broker name (currently only kraken is supported)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Enable real order execution (requires --broker-source real and Kraken credentials)",
    )
    parser.add_argument(
        "--notify-email",
        action="store_true",
        help="Send email notifications for signal/execution events",
    )
    parser.add_argument(
        "--min-order-notional",
        type=float,
        default=10.0,
        help="Minimum order notional in USD",
    )
    parser.add_argument(
        "--max-order-notional",
        type=float,
        default=None,
        help="Optional maximum order notional in USD (exceeded orders are capped)",
    )
    args = parser.parse_args()

    # Validate arguments
    if args.live and args.broker_source != "real":
        logger.error("--live requires --broker-source real")
        sys.exit(1)

    # Load environment
    load_dotenv()

    # === STAGE 1: Refresh OHLCV Data ===
    data_symbols = get_data_symbols()
    trading_symbols = get_trading_symbols()
    logger.info("Configured data refresh universe: %s", ", ".join(data_symbols))
    logger.info("Configured trading execution universe: %s", ", ".join(trading_symbols))

    logger.info("=" * 80)
    logger.info("STAGE 1: Refresh OHLCV Data")
    logger.info("=" * 80)

    if not _refresh_ohlcv_data(symbols=data_symbols, timeframe=SETTINGS.timeframe):
        logger.error("OHLCV data refresh failed. Aborting cycle.")
        sys.exit(1)

    # === STAGE 2: Verify Data Freshness ===
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 2: Verify Data Freshness")
    logger.info("=" * 80)

    if not _verify_data_freshness():
        logger.error("Data freshness check failed. Aborting cycle.")
        sys.exit(1)

    # === STAGE 3: Check Rebalance Timing ===
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 3: Check Rebalance Timing")
    logger.info("=" * 80)

    is_rebalance = _check_rebalance_timing()
    if is_rebalance is None:
        logger.error("Rebalance timing check failed. Aborting cycle.")
        sys.exit(1)

    if not is_rebalance:
        logger.info("Non-rebalance bar. Cycle ending safely (no action taken).")
        return

    # === STAGE 4: Check Pending Signal State ===
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 4: Check Pending Signal State")
    logger.info("=" * 80)

    phase = _check_pending_signal_state()

    # === STAGE 5: Call Execute Orders Pipeline ===
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 5: Execute Orders Pipeline")
    logger.info("=" * 80)

    # Build command to call execute_orders.py with same arguments
    cmd = [
        sys.executable,
        "-m",
        "live.execute_orders",
        "--broker-source",
        args.broker_source,
        "--broker-name",
        args.broker_name,
        "--min-order-notional",
        str(args.min_order_notional),
    ]

    if args.max_order_notional is not None:
        cmd.extend([
            "--max-order-notional",
            str(args.max_order_notional),
        ])

    if args.live:
        cmd.append("--live")

    if args.notify_email:
        cmd.append("--notify-email")

    logger.info(f"Calling: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            logger.error(f"Execute orders pipeline exited with code {result.returncode}")
            sys.exit(result.returncode)
    except Exception as exc:
        logger.error(f"Execute orders pipeline failed: {exc}")
        sys.exit(1)

    # === CYCLE COMPLETE ===
    logger.info("")
    logger.info("=" * 80)
    if phase == "decision":
        logger.info("CYCLE COMPLETE: Pending signal saved (decision bar)")
    else:
        logger.info("CYCLE COMPLETE: Orders executed (execution bar)")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
