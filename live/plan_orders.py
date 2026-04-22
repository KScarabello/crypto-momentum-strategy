"""Dry-run order planner: Trade size and direction calculator.

This is the second deployment-stage utility for the locked baseline strategy.
It computes trade sizes and directions without placing orders or contacting brokers.

Use this to validate trading logic and position changes before implementing live execution.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Order:
    """Trade instruction for one symbol."""

    symbol: str
    action: str  # "BUY" or "SELL"
    current_exposure: float
    target_exposure: float
    delta_exposure: float
    delta_notional: float


def compute_target_exposure(equity: float, target_weight: float) -> float:
    """Compute target dollar exposure from account equity and target weight."""
    if equity <= 0:
        raise ValueError("equity must be positive")
    if target_weight < 0:
        raise ValueError("target_weight must be non-negative")
    return equity * target_weight


def plan_trades(
    equity: float,
    current_positions: dict[str, float],
    target_weights: dict[str, float],
    min_trade_notional: float = 1.0,
) -> list[Order]:
    """Plan trades to move from current positions to target weights.

    Args:
        equity: Current account equity in dollars.
        current_positions: Mapping of symbol -> current dollar exposure.
        target_weights: Mapping of symbol -> target portfolio weight.
        min_trade_notional: Minimum trade size to include in plan (in dollars).

    Returns:
        List of Order objects sorted by trade magnitude (largest first).
    """
    if equity <= 0:
        raise ValueError("equity must be positive")
    if min_trade_notional < 0:
        raise ValueError("min_trade_notional must be non-negative")

    # Validate target weights (allow sum < 1.0 for implicit cash)
    total_target = sum(target_weights.values())
    if total_target < 0 or total_target > 1.0:
        raise ValueError(f"target_weights sum must be in [0, 1]: {total_target}")

    all_symbols = set(current_positions.keys()) | set(target_weights.keys())
    orders = []

    for symbol in sorted(all_symbols):
        current_exp = current_positions.get(symbol, 0.0)
        target_wt = target_weights.get(symbol, 0.0)
        target_exp = compute_target_exposure(equity=equity, target_weight=target_wt)

        delta_exp = target_exp - current_exp
        delta_notional = abs(delta_exp)

        # Skip tiny changes below threshold
        if delta_notional < min_trade_notional:
            continue

        action = "BUY" if delta_exp > 0 else "SELL"
        orders.append(
            Order(
                symbol=symbol,
                action=action,
                current_exposure=current_exp,
                target_exposure=target_exp,
                delta_exposure=delta_exp,
                delta_notional=delta_notional,
            )
        )

    # Sort by magnitude (largest changes first)
    orders.sort(key=lambda o: o.delta_notional, reverse=True)
    return orders


def print_trade_plan(orders: list[Order], equity: float, title: str = "DRY-RUN TRADE PLAN") -> None:
    """Print a formatted trade plan with position summaries."""
    print("\n" + "=" * 85)
    print(title)
    print("=" * 85)
    print(f"Account Equity: ${equity:,.2f}\n")

    if not orders:
        print("No trades needed (all positions already at target).\n")
    else:
        print(f"{len(orders)} trade(s) to execute:\n")
        total_buy_notional = 0.0
        total_sell_notional = 0.0

        for i, order in enumerate(orders, 1):
            sign = "+" if order.delta_exposure > 0 else ""
            pct_current = (order.current_exposure / equity * 100.0) if equity > 0 else 0.0
            pct_target = (order.target_exposure / equity * 100.0) if equity > 0 else 0.0

            print(f"{i}. {order.symbol}")
            print(f"   Current:  ${order.current_exposure:>13,.2f}  ({pct_current:>6.2f}%)")
            print(f"   Target:   ${order.target_exposure:>13,.2f}  ({pct_target:>6.2f}%)")
            print(f"   {order.action:6s}:    ${order.delta_notional:>13,.2f}  ({sign}{order.delta_notional:,.2f})")
            print()

            if order.action == "BUY":
                total_buy_notional += order.delta_notional
            else:
                total_sell_notional += order.delta_notional

        print(f"Total Buy Notional:   ${total_buy_notional:>11,.2f}")
        print(f"Total Sell Notional:  ${total_sell_notional:>11,.2f}")
        print(f"Net Turnover:         ${abs(total_buy_notional - total_sell_notional):>11,.2f}")

    print("=" * 85 + "\n")


# Demo / Example usage
if __name__ == "__main__":
    print("\n" + "=" * 85)
    print("EXAMPLE: LOCKED BASELINE STRATEGY REBALANCE")
    print("=" * 85)

    # Simulated account state
    equity = 100_000.0
    current_positions = {
        "BTC/USD": 20_000.0,
        "ETH/USD": 15_000.0,
        "XRP/USD": 10_000.0,
    }

    # Target weights reflecting locked baseline strategy
    # (after momentum selection and 75% gross exposure cap)
    target_weights = {
        "BTC/USD": 0.25,  # 25% of equity
        "ETH/USD": 0.25,  # 25% of equity
        "XRP/USD": 0.25,  # 25% of equity
        # Implicit cash: 25%
    }

    print(f"\nAccount Equity: ${equity:,.2f}")

    print(f"\nCurrent Positions:")
    current_total = sum(current_positions.values())
    current_cash = equity - current_total
    for symbol in sorted(current_positions.keys()):
        exp = current_positions[symbol]
        pct = (exp / equity) * 100.0
        print(f"  {symbol:12s}  ${exp:>13,.2f}  ({pct:>6.2f}%)")
    print(f"  {'CASH':12s}  ${current_cash:>13,.2f}  ({current_cash/equity*100:>6.2f}%)")

    print(f"\nTarget Weights (from strategy):")
    for symbol in sorted(target_weights.keys()):
        wt = target_weights[symbol]
        exp = equity * wt
        print(f"  {symbol:12s}  {wt:>6.1%}  (${exp:>13,.2f})")
    cash_wt = 1.0 - sum(target_weights.values())
    cash_exp = equity * cash_wt
    print(f"  {'CASH':12s}  {cash_wt:>6.1%}  (${cash_exp:>13,.2f})")

    # Plan trades
    orders = plan_trades(
        equity=equity,
        current_positions=current_positions,
        target_weights=target_weights,
        min_trade_notional=100.0,  # Ignore trades smaller than $100
    )

    print_trade_plan(orders, equity=equity, title="TRADE PLAN TO REBALANCE")

    # Show a second example with minimal action
    print("\n" + "=" * 85)
    print("EXAMPLE 2: MINIMAL REBALANCE (HIGH THRESHOLD)")
    print("=" * 85)

    current_positions_2 = {
        "BTC/USD": 24_900.0,
        "ETH/USD": 25_100.0,
        "XRP/USD": 24_950.0,
    }

    orders_2 = plan_trades(
        equity=equity,
        current_positions=current_positions_2,
        target_weights=target_weights,
        min_trade_notional=500.0,  # Only trade if delta > $500
    )

    print_trade_plan(orders_2, equity=equity, title="TRADE PLAN (HIGH THRESHOLD)")
