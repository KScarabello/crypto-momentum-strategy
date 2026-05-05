"""Autonomous scheduled rebalancing orchestrator for live crypto trading.

This is the single entrypoint for automated trading via scheduler (cron).
Call this every 4 hours to execute the complete pipeline:

  1. Refresh local OHLCV data
  2. Check data freshness
    3. Execute due pending signals first (one-bar delay)
    4. If no pending execution is due, enforce rebalance timing
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
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from config import SETTINGS, get_data_symbols, get_trading_symbols
from data.download_ohlcv import download_all_symbols
from live.generate_targets import generate_targets
from live.signal_state import has_pending_signal, load_pending_signal


EXECUTION_LOCK_FILE = Path(".signals") / "scheduled_cycle_execution.lock"


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


def _load_current_bar_snapshot() -> dict[str, object] | None:
    """Load the latest strategy snapshot for timestamp/rebalance decisions."""
    logger = logging.getLogger(__name__)
    trading_symbols = get_trading_symbols()

    try:
        snapshot = generate_targets(symbols=trading_symbols)
        logger.info("Latest bar snapshot loaded: %s", snapshot["timestamp"])
        return snapshot
    except Exception as exc:
        logger.error(f"Failed to load latest bar snapshot: {exc}")
        return None


def _is_after_decision_bar(current_timestamp: object, decision_timestamp: object) -> bool:
    """Return True if current bar is strictly after pending decision bar."""
    current_ts = pd.Timestamp(current_timestamp)
    decision_ts = pd.Timestamp(decision_timestamp)
    return current_ts > decision_ts


def _acquire_execution_lock(ttl_seconds: int = 6 * 60 * 60) -> bool:
    """Acquire single-run execution lock to avoid double execution on overlapping cron runs."""
    logger = logging.getLogger(__name__)
    EXECUTION_LOCK_FILE.parent.mkdir(exist_ok=True)

    now = int(time.time())
    payload = {"pid": os.getpid(), "created_at": now}

    def _try_create() -> bool:
        try:
            fd = os.open(str(EXECUTION_LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            return True
        except FileExistsError:
            return False

    if _try_create():
        logger.info("Execution lock acquired: %s", EXECUTION_LOCK_FILE)
        return True

    try:
        with open(EXECUTION_LOCK_FILE, "r", encoding="utf-8") as handle:
            lock_data = json.load(handle)
        created_at = int(lock_data.get("created_at", 0))
    except Exception:
        created_at = 0

    if created_at > 0 and (now - created_at) > ttl_seconds:
        logger.warning("Found stale execution lock older than %ss; removing it", ttl_seconds)
        try:
            EXECUTION_LOCK_FILE.unlink()
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning("Could not remove stale execution lock: %s", exc)
            return False

        if _try_create():
            logger.info("Execution lock re-acquired after stale lock cleanup")
            return True

    logger.info("Execution lock already held by another run; skipping duplicate execution")
    return False


def _release_execution_lock() -> None:
    """Release single-run execution lock if held by this run."""
    logger = logging.getLogger(__name__)
    try:
        EXECUTION_LOCK_FILE.unlink()
        logger.info("Execution lock released")
    except FileNotFoundError:
        return
    except Exception as exc:
        logger.warning("Failed to release execution lock: %s", exc)


def _build_execute_orders_cmd(args: argparse.Namespace) -> list[str]:
    """Build subprocess command for execute_orders with inherited CLI settings."""
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

    return cmd


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

    # === STAGE 3: Load Current Bar Snapshot ===
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 3: Load Current Bar Snapshot")
    logger.info("=" * 80)

    current_bar = _load_current_bar_snapshot()
    if current_bar is None:
        logger.error("Current bar snapshot failed. Aborting cycle.")
        sys.exit(1)

    # === STAGE 4: Check Pending Signal First ===
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 4: Check Pending Signal First")
    logger.info("=" * 80)

    pending_signal = load_pending_signal() if has_pending_signal() else None
    phase = "none"

    if pending_signal is not None:
        decision_timestamp = pending_signal.get("timestamp")
        current_timestamp = current_bar.get("timestamp")

        if decision_timestamp is None or current_timestamp is None:
            logger.error("Pending/current timestamp missing; refusing execution to avoid unsafe action")
            sys.exit(1)

        if _is_after_decision_bar(current_timestamp=current_timestamp, decision_timestamp=decision_timestamp):
            logger.info(
                "Pending signal detected from %s and current bar is %s; executing now",
                decision_timestamp,
                current_timestamp,
            )

            if not _acquire_execution_lock():
                logger.info("Another run is already executing pending signal. Ending this run safely.")
                return

            cmd = _build_execute_orders_cmd(args)
            logger.info(f"Calling: {' '.join(cmd)}")
            try:
                result = subprocess.run(cmd, check=False)
                if result.returncode != 0:
                    logger.error(f"Execute orders pipeline exited with code {result.returncode}")
                    sys.exit(result.returncode)

                phase = "execution"
            except Exception as exc:
                logger.error(f"Execute orders pipeline failed: {exc}")
                sys.exit(1)
            finally:
                _release_execution_lock()
        else:
            logger.info(
                "Pending signal from %s is not yet executable on current bar %s. No action.",
                decision_timestamp,
                current_timestamp,
            )
            return
    else:
        logger.info("No pending signal found; proceeding to rebalance check.")

    # === STAGE 5: Rebalance Check and Decision Signal ===
    if phase != "execution":
        logger.info("")
        logger.info("=" * 80)
        logger.info("STAGE 5: Rebalance Check and Decision Signal")
        logger.info("=" * 80)

        is_rebalance = bool(current_bar.get("is_rebalance_bar", False))
        if not is_rebalance:
            logger.info("Non-rebalance bar with no executable pending signal. Cycle ending safely.")
            return

        logger.info("Rebalance bar confirmed: %s", current_bar.get("timestamp"))
        cmd = _build_execute_orders_cmd(args)
        logger.info(f"Calling: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, check=False)
            if result.returncode != 0:
                logger.error(f"Execute orders pipeline exited with code {result.returncode}")
                sys.exit(result.returncode)
            phase = "decision"
        except Exception as exc:
            logger.error(f"Execute orders pipeline failed: {exc}")
            sys.exit(1)

    # === CYCLE COMPLETE ===
    logger.info("")
    logger.info("=" * 80)
    if phase == "decision":
        logger.info("CYCLE COMPLETE: Pending signal saved (decision bar)")
    elif phase == "execution":
        logger.info("CYCLE COMPLETE: Orders executed (execution bar)")
    else:
        logger.info("CYCLE COMPLETE: No action")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
