"""CLI utility to safely clear pending live signal state."""

from __future__ import annotations

import logging

from live import signal_state


def configure_logging() -> None:
    """Configure readable logging for maintenance commands."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def clear_pending_signal_state() -> bool:
    """Clear pending signal state if it exists.

    Returns:
        True when pending state existed and was cleared, False when no state existed.
    """
    logger = logging.getLogger(__name__)

    pending = signal_state.load_pending_signal()
    if pending is None:
        message = "No pending signal state found. Nothing to clear."
        logger.info(message)
        print(message)
        return False

    decision_bar = pending.get("timestamp", "unknown")
    logger.info("Pending signal decision bar: %s", decision_bar)
    print(f"Found pending signal from decision bar: {decision_bar}")

    signal_state.clear_pending_signal()
    logger.info("Pending signal state cleared")
    print("Pending signal state cleared.")
    return True


def main() -> None:
    """Entry point for pending-signal clear command."""
    configure_logging()
    clear_pending_signal_state()


if __name__ == "__main__":
    main()
