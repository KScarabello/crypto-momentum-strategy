"""Preview a hypothetical five-asset rebalance without touching live state."""

from __future__ import annotations

import argparse
import logging
import os

from dotenv import load_dotenv

from config import get_data_symbols
from live.broker_state import AccountState, load_account_state
from live.generate_targets import generate_targets
from live.plan_orders import Order, plan_trades


def configure_logging() -> None:
    """Configure minimal logging for preview diagnostics."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _weights_from_positions(
    equity: float,
    positions: dict[str, float],
    symbols: tuple[str, ...],
) -> tuple[dict[str, float], float]:
    """Compute current weights for the requested symbol universe plus cash."""
    if equity <= 0:
        raise ValueError("equity must be positive")

    current_weights = {
        symbol: float(positions.get(symbol, 0.0)) / float(equity)
        for symbol in symbols
    }
    risky_weight = sum(current_weights.values())
    cash_weight = max(0.0, 1.0 - risky_weight)
    return current_weights, cash_weight


def build_hypothetical_five_asset_rebalance(
    min_trade_notional: float = 10.0,
    broker_source: str = "mock",
    broker_name: str = "kraken",
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
) -> dict[str, object]:
    """Build a read-only five-asset rebalance preview using live planning logic."""
    if min_trade_notional < 0:
        raise ValueError("min_trade_notional must be non-negative")
    if broker_source == "real" and broker_name.lower() != "kraken":
        raise ValueError("Only --broker-name kraken is supported when --broker-source real")

    preview_symbols = get_data_symbols()
    strategy_result = generate_targets(symbols=preview_symbols)
    account_state = load_account_state(
        source=broker_source,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        symbols=preview_symbols,
    )

    current_positions = {
        symbol: float(account_state.positions.get(symbol, 0.0))
        for symbol in preview_symbols
    }
    target_weights = {
        symbol: float(strategy_result["target_weights"].get(symbol, 0.0))
        for symbol in preview_symbols
    }
    current_weights, current_cash_weight = _weights_from_positions(
        equity=float(account_state.equity),
        positions=current_positions,
        symbols=preview_symbols,
    )

    orders = plan_trades(
        equity=float(account_state.equity),
        current_positions=current_positions,
        target_weights=target_weights,
        min_trade_notional=min_trade_notional,
    )
    sells = [order for order in orders if order.action == "SELL"]
    buys = [order for order in orders if order.action == "BUY"]

    return {
        "strategy_variant": strategy_result["strategy_variant"],
        "timestamp": strategy_result["timestamp"],
        "risk_on": strategy_result["risk_on"],
        "selected_symbols": strategy_result["selected_symbols"],
        "symbols": preview_symbols,
        "equity": float(account_state.equity),
        "available_cash": float(account_state.available_cash),
        "current_positions": current_positions,
        "current_weights": current_weights,
        "current_cash_weight": current_cash_weight,
        "target_weights": target_weights,
        "target_cash_weight": float(strategy_result["cash_weight"]),
        "sells": sells,
        "buys": buys,
    }


def _print_weight_table(
    equity: float,
    current_positions: dict[str, float],
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    symbols: tuple[str, ...],
    current_cash_weight: float,
    target_cash_weight: float,
) -> None:
    """Print aligned current-vs-target weights for the preview universe."""
    print("\nWeights by Symbol:")
    print("  Symbol       Current Wt   Target Wt   Current $        Target $")
    for symbol in symbols:
        current_notional = float(current_positions.get(symbol, 0.0))
        target_notional = float(target_weights.get(symbol, 0.0)) * float(equity)
        print(
            f"  {symbol:10s}  {current_weights.get(symbol, 0.0):10.2%}  "
            f"{target_weights.get(symbol, 0.0):9.2%}  ${current_notional:12,.2f}  "
            f"${target_notional:12,.2f}"
        )
    print(
        f"  {'CASH':10s}  {current_cash_weight:10.2%}  {target_cash_weight:9.2%}  "
        f"${(equity * current_cash_weight):12,.2f}  ${(equity * target_cash_weight):12,.2f}"
    )


def _print_order_block(title: str, orders: list[Order]) -> None:
    """Print a concise order list block."""
    print(f"\n{title}:")
    if not orders:
        print("  none")
        return

    total = 0.0
    for order in orders:
        total += float(order.delta_notional)
        print(
            f"  {order.symbol:10s}  {order.action:4s}  est_notional=${order.delta_notional:,.2f}  "
            f"current=${order.current_exposure:,.2f}  target=${order.target_exposure:,.2f}"
        )
    print(f"  Total {title.lower()}: ${total:,.2f}")


def print_hypothetical_five_asset_rebalance(preview: dict[str, object]) -> None:
    """Print the hypothetical five-asset dry-run report."""
    print("\n" + "=" * 95)
    print("HYPOTHETICAL FIVE-ASSET DRY RUN — NO ORDERS SUBMITTED")
    print("=" * 95)
    print(f"Variant:         {preview['strategy_variant']}")
    print(f"Timestamp:       {preview['timestamp']}")
    print(f"Risk-On:         {preview['risk_on']}")
    print(f"Selected Names:  {', '.join(preview['selected_symbols']) or 'none'}")
    print(f"Account Equity:  ${preview['equity']:,.2f}")
    print(f"Available Cash:  ${preview['available_cash']:,.2f}")
    print(f"Universe:        {', '.join(preview['symbols'])}")

    _print_weight_table(
        equity=float(preview['equity']),
        current_positions=preview['current_positions'],
        current_weights=preview['current_weights'],
        target_weights=preview['target_weights'],
        symbols=preview['symbols'],
        current_cash_weight=float(preview['current_cash_weight']),
        target_cash_weight=float(preview['target_cash_weight']),
    )
    _print_order_block("Sells", preview['sells'])
    _print_order_block("Buys", preview['buys'])
    print("=" * 95 + "\n")


def main() -> None:
    """CLI entrypoint for the hypothetical five-asset preview."""
    load_dotenv()
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Preview a hypothetical five-asset dry-run rebalance")
    parser.add_argument("--broker-source", choices=["mock", "real"], default="mock")
    parser.add_argument("--broker-name", choices=["kraken"], default="kraken")
    parser.add_argument("--min-trade-notional", type=float, default=10.0)
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

    logger.info("Previewing hypothetical five-asset rebalance using data universe: %s", ", ".join(get_data_symbols()))
    preview = build_hypothetical_five_asset_rebalance(
        min_trade_notional=args.min_trade_notional,
        broker_source=args.broker_source,
        broker_name=args.broker_name,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )
    print_hypothetical_five_asset_rebalance(preview)


if __name__ == "__main__":
    main()
