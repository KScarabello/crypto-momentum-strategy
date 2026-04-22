"""Send a one-off test email for the trading notification utility.

This script is isolated from trading execution and is intended only to verify
SMTP/.env configuration for live.notify_email.send_trade_notification.
"""

from __future__ import annotations

from datetime import datetime, timezone

from live.notify_email import send_trade_notification


def main() -> None:
    """Send a single test trade notification email."""
    send_trade_notification(
        strategy_variant="locked_baseline",
        timestamp=datetime.now(timezone.utc).isoformat(),
        symbol="BTC/USD",
        side="BUY",
        notional_or_quantity=123.45,
        status_text="test notification",
    )
    print("Test email sent successfully.")


if __name__ == "__main__":
    main()
