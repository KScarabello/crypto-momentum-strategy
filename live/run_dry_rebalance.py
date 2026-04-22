"""Dry-run rebalance orchestrator: End-to-end pipeline integration.

This script combines strategy targeting, broker state, and trade planning
into a single dry-run rebalance report. No orders are placed; all operations
are deterministic and local.

This is the first end-to-end dry-run pipeline for the locked baseline strategy.
Use this to validate the full rebalance workflow before implementing live execution.
"""

from __future__ import annotations

import argparse
import logging
import os
from dotenv import load_dotenv

from live.broker_state import load_account_state
from live.generate_targets import generate_targets
from live.plan_orders import plan_trades


def configure_logging() -> None:
    """Configure minimal project logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def run_dry_rebalance(
    min_trade_notional: float = 100.0,
    broker_source: str = "mock",
    broker_name: str = "kraken",
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
) -> None:
    """Run end-to-end dry-run rebalance: targets + broker state + trade plan.

    Args:
        min_trade_notional: Minimum trade size to include in plan (in dollars).
        broker_source: Broker source ("mock" or "real"). Default: "mock".
        broker_name: Broker name. Only "kraken" is supported for real mode.
        api_key: Kraken API key (required if source="real").
        api_secret: Kraken API secret (required if source="real").
        api_passphrase: Kraken API passphrase (optional).
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting dry-run rebalance orchestration")
    logger.info(f"Broker source: {broker_source}")

    if broker_source == "real":
        if broker_name.lower() != "kraken":
            raise ValueError("Only --broker-name kraken is supported when --broker-source real")
        print("Using real broker mode: kraken")

    # Step 1: Get strategy targets
    logger.info("Fetching strategy targets from locked baseline strategy")
    strategy_result = generate_targets()
    variant = strategy_result["strategy_variant"]
    timestamp = strategy_result["timestamp"]
    selected_symbols = strategy_result["selected_symbols"]
    target_weights = strategy_result["target_weights"]
    risk_on = strategy_result["risk_on"]

    logger.info(f"Strategy variant: {variant}, timestamp: {timestamp}")
    logger.info(f"Selected symbols: {selected_symbols}")

    # Step 2: Get current broker state
    broker_desc = "mock" if broker_source == "mock" else "Kraken"
    logger.info(f"Loading current broker account state ({broker_desc})")
    try:
        account_state = load_account_state(
            source=broker_source,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
    except Exception as e:
        logger.error(f"Failed to load account state: {e}")
        raise

    equity = account_state.equity
    current_positions = account_state.positions

    logger.info(f"Account equity: ${equity:,.2f}")
    logger.info(f"Current positions: {current_positions}")

    # Step 3: Plan trades
    logger.info("Planning trades to target weights")
    orders = plan_trades(
        equity=equity,
        current_positions=current_positions,
        target_weights=target_weights,
        min_trade_notional=min_trade_notional,
    )

    logger.info(f"Planned {len(orders)} trade(s)")

    # Step 4: Print comprehensive report
    print_rebalance_report(
        variant=variant,
        timestamp=timestamp,
        risk_on=risk_on,
        equity=equity,
        current_positions=current_positions,
        target_weights=target_weights,
        selected_symbols=selected_symbols,
        orders=orders,
    )

    logger.info("Dry-run rebalance orchestration complete")


def print_rebalance_report(
    variant: str,
    timestamp,
    risk_on: bool,
    equity: float,
    current_positions: dict[str, float],
    target_weights: dict[str, float],
    selected_symbols: list[str],
    orders,
) -> None:
    """Print a comprehensive dry-run rebalance report."""
    print("\n" + "=" * 95)
    print("DRY-RUN REBALANCE REPORT: END-TO-END PIPELINE")
    print("=" * 95)

    # Header section
    print(f"\nVariant:         {variant}")
    print(f"Timestamp:       {timestamp}")
    print(f"Risk-On:         {risk_on}")
    print(f"Account Equity:  ${equity:,.2f}")

    # Strategy section
    print(f"\nStrategy Status:")
    print(f"  Selected Symbols:  {', '.join(selected_symbols)}")
    print(f"  Target Weights:")
    for symbol in sorted(target_weights.keys()):
        wt = target_weights[symbol]
        exp = equity * wt
        print(f"    {symbol:12s}  {wt:>8.1%}  ${exp:>13,.2f}")
    cash_wt = 1.0 - sum(target_weights.values())
    cash_exp = equity * cash_wt
    print(f"    {'CASH':12s}  {cash_wt:>8.1%}  ${cash_exp:>13,.2f}")

    # Current state section
    print(f"\nCurrent Positions:")
    current_total = sum(current_positions.values())
    for symbol in sorted(current_positions.keys()):
        exp = current_positions[symbol]
        pct = (exp / equity) * 100.0
        print(f"  {symbol:12s}  ${exp:>13,.2f}  ({pct:>6.2f}%)")
    current_cash = equity - current_total
    current_cash_pct = (current_cash / equity) * 100.0
    print(f"  {'CASH':12s}  ${current_cash:>13,.2f}  ({current_cash_pct:>6.2f}%)")

    # Trade plan section
    print(f"\nProposed Trades:")
    if not orders:
        print("  No trades needed (all positions already at target)")
    else:
        total_buy = 0.0
        total_sell = 0.0
        for i, order in enumerate(orders, 1):
            sign = "+" if order.delta_exposure > 0 else "-"
            print(f"  {i}. {order.symbol:12s}  {order.action:4s}  ${abs(order.delta_notional):>12,.2f}  ({sign}{abs(order.delta_notional):,.2f})")
            if order.action == "BUY":
                total_buy += order.delta_notional
            else:
                total_sell += order.delta_notional

        print(f"\n  Total Buy:       ${total_buy:>11,.2f}")
        print(f"  Total Sell:      ${total_sell:>11,.2f}")
        print(f"  Net Turnover:    ${abs(total_buy - total_sell):>11,.2f}")

    # Summary
    print("\n" + "=" * 95)
    print("Summary: All calculations are dry-run only. No orders have been placed.")
    print("=" * 95 + "\n")


if __name__ == "__main__":
    # Load credentials from a local .env file when present.
    # Keep .env out of version control because it contains secrets.
    load_dotenv()

    parser = argparse.ArgumentParser(description="Run end-to-end dry-run rebalance pipeline")
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
        help="Minimum trade notional to include in plan (default: 100.0)",
    )
    args = parser.parse_args()

    api_key = None
    api_secret = None
    api_passphrase = None
    if args.broker_source == "real":
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        api_passphrase = os.getenv("KRAKEN_API_PASSPHRASE")
        if not api_key or not api_secret:
            raise SystemExit(
                "Missing Kraken credentials for real mode. "
                "Set KRAKEN_API_KEY and KRAKEN_API_SECRET in your environment or .env file."
            )

    run_dry_rebalance(
        min_trade_notional=args.min_trade_notional,
        broker_source=args.broker_source,
        broker_name=args.broker_name,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )
