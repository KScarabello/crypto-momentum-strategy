"""Manage pending signal state for one-bar-delayed execution.

This module persists strategy signals locally so that:
1. Signal is generated at bar T and saved as "pending"
2. Signal executes at bar T+1 (one-bar delay, matching backtest model)
3. After execution, pending signal is archived/cleared

Persisted snapshots include: variant, decision timestamp, target weights, selected symbols,
and freshness/rebalance flags so execution can verify preconditions.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

SIGNAL_STATE_DIR = Path(".signals")
PENDING_SIGNAL_FILE = SIGNAL_STATE_DIR / "pending_signal.json"


def _ensure_signal_dir() -> None:
    """Create signal state directory if it does not exist."""
    SIGNAL_STATE_DIR.mkdir(exist_ok=True)


def save_pending_signal(signal_dict: dict[str, Any]) -> None:
    """Save a pending signal snapshot to disk.

    Args:
        signal_dict: Dictionary with keys strategy_variant, timestamp, target_weights,
                     selected_symbols, data_fresh, is_rebalance_bar, etc.
    """
    _ensure_signal_dir()
    with open(PENDING_SIGNAL_FILE, "w") as f:
        json.dump(signal_dict, f, indent=2, default=str)
    LOGGER.info(f"Saved pending signal (decision bar: {signal_dict['timestamp']})")


def load_pending_signal() -> dict[str, Any] | None:
    """Load the pending signal snapshot from disk, if it exists.

    Returns:
        Dictionary with pending signal, or None if no pending signal exists.
    """
    if not PENDING_SIGNAL_FILE.exists():
        return None
    with open(PENDING_SIGNAL_FILE, "r") as f:
        signal = json.load(f)
    LOGGER.info(f"Loaded pending signal (decision bar: {signal['timestamp']})")
    return signal


def clear_pending_signal() -> None:
    """Delete the pending signal snapshot after it has been executed."""
    if PENDING_SIGNAL_FILE.exists():
        PENDING_SIGNAL_FILE.unlink()
        LOGGER.info("Cleared pending signal after execution")


def has_pending_signal() -> bool:
    """Return True if a pending signal exists."""
    return PENDING_SIGNAL_FILE.exists()
