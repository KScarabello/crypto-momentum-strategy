"""Prepare executable dry-run orders from strategy targets and account state.

This is the final dry-run stage before live order submission. It reuses the
existing target generation, broker-state loading, and trade planning pipeline,
and outputs validated order instructions without submitting any orders.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv

from config import SETTINGS
from live.broker_state import load_account_state
from live.generate_targets import generate_targets
from live.plan_orders import Order, plan_trades


@dataclass(frozen=True)
class PreparedOrder:
    """Executable dry-run order instruction."""

    symbol: str
    side: str
    notional_usd: float


def configure_logging() -> None:
    """Configure minimal project logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def prepare_orders(
    planned_orders: list[Order],
    supported_symbols: set[str],
    min_trade_notional: float,
) -> list[PreparedOrder]:
    """Convert planned trades to executable dry-run orders with safety checks."""
    prepared: list[PreparedOrder] = []

    for order in planned_orders:
        symbol = order.symbol
        side = order.action.lower()
        notional_usd = float(order.delta_notional)

        # Safety filters
        if symbol not in supported_symbols:
            continue
        if side not in {"buy", "sell"}:
            continue
        if notional_usd <= 0:
            continue
        if notional_usd < min_trade_notional:
            continue

        prepared.append(
            PreparedOrder(
                symbol=symbol,
                side=side,
                notional_usd=notional_usd,
            )
        )

    return prepared


def print_prepared_orders_report(
    variant: str,
    timestamp,
    equity: float,
    current_positions: dict[str, float],
    target_weights: dict[str, float],
    prepared_orders: list[PreparedOrder],
) -> None:
    """Print a concise prepared-orders dry-run report."""
    print("\n" + "=" * 95)
    print("PREPARED ORDERS REPORT (DRY-RUN ONLY)")
    print("=" * 95)
    print(f"\nVariant:         {variant}")
    print(f"Timestamp:       {timestamp}")
    print(f"Account Equity:  ${equity:,.2f}")

    print("\nCurrent Positions (USD):")
    if current_positions:
        for symbol in sorted(current_positions.keys()):
            exp = float(current_positions[symbol])
            pct = (exp / equity * 100.0) if equity > 0 else 0.0
            print(f"  {symbol:12s}  ${exp:>13,.2f}  ({pct:>6.2f}%)")
    else:
        print("  none")

    print("\nTarget Weights:")
    for symbol in sorted(target_weights.keys()):
        wt = float(target_weights[symbol])
        exp = equity * wt
        print(f"  {symbol:12s}  {wt:>8.1%}  ${exp:>13,.2f}")
    cash_wt = 1.0 - sum(float(v) for v in target_weights.values())
    print(f"  {'CASH':12s}  {cash_wt:>8.1%}  ${equity * cash_wt:>13,.2f}")

    print("\nPrepared Orders:")
    if not prepared_orders:
        print("  No executable orders after safety filters.")
    else:
        total_buy = 0.0
        total_sell = 0.0
        for i, order in enumerate(prepared_orders, 1):
            print(
                f"  {i}. {order.symbol:12s}  {order.side.upper():4s}  ${order.notional_usd:>12,.2f}"
            )
            if order.side == "buy":
                total_buy += order.notional_usd
            else:
                total_sell += order.notional_usd

        print(f"\n  Total Buy Notional:   ${total_buy:>11,.2f}")
        print(f"  Total Sell Notional:  ${total_sell:>11,.2f}")
        print(f"  Net Turnover:         ${abs(total_buy - total_sell):>11,.2f}")

    print("\nSummary: Orders are prepared only; no broker submission is performed.")
    print("=" * 95 + "\n")


def main() -> None:
    """Run dry-run order preparation pipeline."""
    load_dotenv()
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Prepare dry-run executable orders")
    parser.add_argument(
        "--broker-source",
        choices=["mock", "real"],
        default="mock",
        help="Account-state source to use (default: mock)",
    )
    parser.add_argument(
        "--broker-name",
        choices=["kraken"],
        default="kraken",
        help="Broker name for real mode (default: kraken)",
    )
    parser.add_argument(
        "--min-trade-notional",
        type=float,
        default=100.0,
        help="Minimum notional (USD) for prepared orders (default: 100.0)",
    )
    args = parser.parse_args()

    api_key = None
    api_secret = None
    api_passphrase = None
    if args.broker_source == "real":
        if args.broker_name.lower() != "kraken":
            raise SystemExit("Only --broker-name kraken is supported for real mode")
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        api_passphrase = os.getenv("KRAKEN_API_PASSPHRASE")
        if not api_key or not api_secret:
            raise SystemExit(
                "Missing Kraken credentials for real mode. "
                "Set KRAKEN_API_KEY and KRAKEN_API_SECRET in your environment or .env file."
            )
        print("Using real broker mode: kraken")

    logger.info("Preparing dry-run executable orders")

    strategy_result = generate_targets()
    account_state = load_account_state(
        source=args.broker_source,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )

    planned_orders = plan_trades(
        equity=account_state.equity,
        current_positions=account_state.positions,
        target_weights=strategy_result["target_weights"],
        min_trade_notional=args.min_trade_notional,
    )

    supported_symbols = set(SETTINGS.symbols)
    prepared_orders = prepare_orders(
        planned_orders=planned_orders,
        supported_symbols=supported_symbols,
        min_trade_notional=args.min_trade_notional,
    )

    print_prepared_orders_report(
        variant=strategy_result["strategy_variant"],
        timestamp=strategy_result["timestamp"],
        equity=account_state.equity,
        current_positions=account_state.positions,
        target_weights=strategy_result["target_weights"],
        prepared_orders=prepared_orders,
    )


if __name__ == "__main__":
    main()
