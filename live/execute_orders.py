"""First live execution module for prepared crypto orders.

Use with extreme caution. Dry-run preview mode is the default safe behavior,
and no orders are submitted unless --live is explicitly provided.

TIMING MODEL: One-bar-delayed execution to match backtest engine.
   Bar T: generate signal, save as pending, exit -> no orders placed
   Bar T+1: load pending signal, execute orders, clear pending -> orders placed
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from dotenv import load_dotenv

from config import SETTINGS
from live.broker_state import load_account_state
from live.generate_targets import generate_targets
from live.notify_email import send_trade_notification
from live.plan_orders import plan_trades
from live.prepare_orders import PreparedOrder, prepare_orders
from live.signal_state import has_pending_signal, load_pending_signal, save_pending_signal, clear_pending_signal


def configure_logging() -> None:
    """Configure minimal project logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_prepared_orders(
    broker_source: str,
    min_order_notional: float,
    api_key: str | None,
    api_secret: str | None,
    api_passphrase: str | None,
    use_pending_signal: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], Any, list[PreparedOrder]]:
    """Build prepared orders from targets, account state, and trade plan.
    
    Args:
        use_pending_signal: If provided, use this saved signal's target_weights instead of 
                           generating new targets. Used for one-bar-delayed execution.
    """
    if use_pending_signal is not None:
        # Use the saved pending signal (one-bar-delayed execution)
        strategy_result = use_pending_signal
    else:
        # Generate new targets (bar T of the signal)
        strategy_result = generate_targets()
    
    account_state = load_account_state(
        source=broker_source,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )

    planned_orders = plan_trades(
        equity=account_state.equity,
        current_positions=account_state.positions,
        target_weights=strategy_result["target_weights"],
        min_trade_notional=min_order_notional,
    )

    supported_symbols = set(SETTINGS.symbols)
    prepared = prepare_orders(
        planned_orders=planned_orders,
        supported_symbols=supported_symbols,
        min_trade_notional=min_order_notional,
    )

    return strategy_result, account_state, prepared


def _is_next_bar(decision_timestamp: Any, current_timestamp: Any, timeframe: str) -> bool:
    """Check if current bar is the next completed bar after the decision timestamp.
    
    For 4h bars: next bar is >= decision_ts + 4 hours
    For 1d bars: next bar is >= decision_ts + 24 hours
    """
    import pandas as pd
    
    dt_decision = pd.Timestamp(decision_timestamp) if not isinstance(decision_timestamp, pd.Timestamp) else decision_timestamp
    dt_current = pd.Timestamp(current_timestamp) if not isinstance(current_timestamp, pd.Timestamp) else current_timestamp
    
    bar_hours = 4 if timeframe == "4h" else 24
    min_advance = pd.Timedelta(hours=bar_hours)
    
    return (dt_current - dt_decision) >= min_advance


def _apply_execution_safeguards(
    prepared_orders: list[PreparedOrder],
    supported_symbols: set[str],
    min_order_notional: float,
    max_order_notional: float,
) -> list[PreparedOrder]:
    """Apply strict execution-time validation and filtering."""
    vetted: list[PreparedOrder] = []

    for order in prepared_orders:
        if order.symbol not in supported_symbols:
            raise ValueError(f"Unsupported symbol rejected: {order.symbol}")
        if order.notional_usd <= 0:
            raise ValueError(f"Non-positive notional rejected for {order.symbol}: {order.notional_usd}")
        if order.notional_usd > max_order_notional:
            print(
                f"  WARNING: {order.symbol} {order.side.upper()} "
                f"${order.notional_usd:,.2f} exceeds --max-order-notional "
                f"${max_order_notional:,.2f} — capped to ${max_order_notional:,.2f}"
            )
            order = PreparedOrder(
                symbol=order.symbol,
                side=order.side,
                notional_usd=max_order_notional,
            )
        if order.notional_usd < min_order_notional:
            continue

        vetted.append(order)

    return vetted


def _print_order_preview(
    variant: str,
    timestamp: str,
    equity: float,
    orders: list[PreparedOrder],
    live: bool,
) -> None:
    """Print clear order preview before any potential submission."""
    print("\n" + "=" * 95)
    print("ORDER EXECUTION PREVIEW")
    print("=" * 95)
    print(f"\nMode:            {'LIVE EXECUTION' if live else 'DRY-RUN PREVIEW'}")
    print(f"Variant:         {variant}")
    print(f"Timestamp:       {timestamp}")
    print(f"Account Equity:  ${equity:,.2f}")

    print("\nOrders:")
    if not orders:
        print("  none")
    else:
        total = 0.0
        for i, order in enumerate(orders, 1):
            print(f"  {i}. {order.symbol:12s}  {order.side.upper():4s}  ${order.notional_usd:>12,.2f}")
            total += order.notional_usd
        print(f"\n  Total notional: ${total:,.2f}")

    print("=" * 95 + "\n")


def _notify_trade_event(
    strategy_variant: str,
    timestamp: str,
    symbol: str,
    side: str,
    notional_usd: float,
    status_text: str,
) -> None:
    """Send a trade notification without interrupting execution on email errors."""
    try:
        send_trade_notification(
            strategy_variant=strategy_variant,
            timestamp=timestamp,
            symbol=symbol,
            side=side,
            notional_or_quantity=notional_usd,
            status_text=status_text,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Email notification failed: %s", exc)


def _submit_kraken_orders_live(
    orders: list[PreparedOrder],
    api_key: str,
    api_secret: str,
    api_passphrase: str | None,
) -> dict[str, list[dict[str, Any]]]:
    """Submit vetted orders to Kraken and collect per-order execution outcomes.

    NOTE: This is the actual live submission path.
    """
    try:
        import ccxt
    except ImportError as exc:
        raise ImportError("ccxt is required for Kraken execution. Install with: pip install ccxt") from exc

    exchange = ccxt.kraken(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "password": api_passphrase or "",
            "enableRateLimit": True,
        }
    )

    # Use one ticker snapshot for deterministic notional->amount conversion when possible.
    symbols = sorted({o.symbol for o in orders})
    tickers: dict[str, Any] = {}
    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Failed to fetch Kraken tickers snapshot, falling back to per-symbol fetch: %s",
            exc,
        )

    results: dict[str, list[dict[str, Any]]] = {"successes": [], "failures": []}
    for order in orders:
        try:
            ticker = tickers.get(order.symbol)
            if not ticker:
                ticker = exchange.fetch_ticker(order.symbol)
            if not ticker:
                raise RuntimeError(f"Missing ticker for order symbol: {order.symbol}")

            price = ticker.get("last") or ticker.get("close")
            if price is None or float(price) <= 0:
                raise RuntimeError(f"Invalid market price for {order.symbol}: {price}")

            amount = float(order.notional_usd) / float(price)
            if amount <= 0:
                raise RuntimeError(f"Computed non-positive base amount for {order.symbol}: {amount}")

            # LIVE SUBMISSION: market order to Kraken.
            response = exchange.create_order(
                symbol=order.symbol,
                type="market",
                side=order.side,
                amount=amount,
            )
            results["successes"].append(
                {
                    "symbol": order.symbol,
                    "side": order.side,
                    "notional_usd": order.notional_usd,
                    "amount": amount,
                    "response": response,
                }
            )
        except Exception as exc:
            results["failures"].append(
                {
                    "symbol": order.symbol,
                    "side": order.side,
                    "notional_usd": order.notional_usd,
                    "error": str(exc),
                }
            )

    return results


def _available_cash_usd(account_state: Any) -> float:
    """Estimate available USD cash as equity minus risky position exposure."""
    return max(0.0, float(account_state.equity) - float(sum(account_state.positions.values())))


def main() -> None:
    """Execute prepared orders with one-bar-delayed timing to match backtest.
    
    Two-phase flow:
    - Bar T: Generate signal, save as pending, exit (no orders placed)
    - Bar T+1: Load pending signal, execute orders, clear pending (orders placed)
    """
    load_dotenv()
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Preview or execute prepared Kraken orders")
    parser.add_argument("--broker-source", choices=["mock", "real"], default="mock")
    parser.add_argument("--broker-name", choices=["kraken"], default="kraken")
    parser.add_argument("--live", action="store_true", help="Enable real order execution")
    parser.add_argument(
        "--notify-email",
        action="store_true",
        help="Send email notifications for preview/submission events",
    )
    parser.add_argument("--min-order-notional", type=float, default=10.0)
    parser.add_argument("--max-order-notional", type=float, default=25.0)
    args = parser.parse_args()

    if args.min_order_notional <= 0:
        raise SystemExit("--min-order-notional must be > 0")
    if args.max_order_notional <= 0:
        raise SystemExit("--max-order-notional must be > 0")
    if args.max_order_notional < args.min_order_notional:
        raise SystemExit("--max-order-notional must be >= --min-order-notional")

    if args.broker_name.lower() != "kraken":
        raise SystemExit("Only --broker-name kraken is supported")

    if args.live and args.broker_source != "real":
        raise SystemExit("--live requires --broker-source real")

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

    # === PHASE 1: Load current bar and check preconditions ===
    # This determines what phase of the one-bar-delay cycle we're in.
    current_bar = generate_targets()

    if not current_bar.get("is_rebalance_bar", True):
        logger.info("[SKIP] Non-rebalance bar: %s. No action taken.", current_bar["timestamp"])
        return

    if not current_bar.get("data_fresh", True):
        print(
            f"\nStale data: OHLCV data is not fresh enough (latest bar: {current_bar['timestamp']}, "
            f"max age for {SETTINGS.timeframe} bars: {2 * 4}h). Refusing execution. "
            f"Please refresh local OHLCV files."
        )
        return

    # === PHASE 2: Check for pending signal from previous bar ===
    pending_signal = load_pending_signal() if has_pending_signal() else None

    if pending_signal is not None:
        # We have a pending signal from the previous bar.
        # Check if current bar is the next bar (eligible for execution).
        is_execution_bar = _is_next_bar(
            decision_timestamp=pending_signal["timestamp"],
            current_timestamp=current_bar["timestamp"],
            timeframe=SETTINGS.timeframe,
        )

        if not is_execution_bar:
            print(
                f"\nWaiting for execution bar: pending signal from {pending_signal['timestamp']}, "
                f"current bar {current_bar['timestamp']}, will execute on next rebalance bar."
            )
            return

        logger.info("[EXECUTION] Executing pending signal from %s", pending_signal["timestamp"])

        # Use the pending signal for execution (not current bar).
        strategy_result, account_state, prepared_orders = _build_prepared_orders(
            broker_source=args.broker_source,
            min_order_notional=args.min_order_notional,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            use_pending_signal=pending_signal,
        )
    else:
        # No pending signal. This is the decision bar (phase 1 of delay).
        # Generate signal, save as pending, and exit.
        logger.info(
            "[DECISION] Signal generated at %s. Saved for next bar execution.",
            current_bar["timestamp"],
        )
        save_pending_signal(current_bar)

        print(
            f"\nSignal generated at bar {current_bar['timestamp']} (decision bar). "
            f"Will execute on the next completed bar. Exiting."
        )
        return

    # === PHASE 3: Build and execute pending signal ===
    vetted_orders = _apply_execution_safeguards(
        prepared_orders=prepared_orders,
        supported_symbols=set(SETTINGS.symbols),
        min_order_notional=args.min_order_notional,
        max_order_notional=args.max_order_notional,
    )

    _print_order_preview(
        variant=strategy_result["strategy_variant"],
        timestamp=str(strategy_result["timestamp"]),
        equity=float(account_state.equity),
        orders=vetted_orders,
        live=args.live,
    )

    if args.notify_email:
        preview_status = "live preview" if args.live else "dry-run preview"
        if vetted_orders:
            for order in vetted_orders:
                _notify_trade_event(
                    strategy_variant=strategy_result["strategy_variant"],
                    timestamp=str(strategy_result["timestamp"]),
                    symbol=order.symbol,
                    side=order.side,
                    notional_usd=order.notional_usd,
                    status_text=preview_status,
                )
        else:
            _notify_trade_event(
                strategy_variant=strategy_result["strategy_variant"],
                timestamp=str(strategy_result["timestamp"]),
                symbol="N/A",
                side="BUY",
                notional_usd=0.0,
                status_text=f"{preview_status}: no vetted orders",
            )

    if not args.live:
        logger.info("Dry-run mode: no orders submitted")
        # Dry-run preview must not mutate pending signal state.
        return

    if not vetted_orders:
        logger.info("No vetted orders to submit in live mode")
        clear_pending_signal()  # Clear pending if no orders
        return

    logger.warning("LIVE MODE ENABLED: submitting orders to Kraken")
    sell_orders = [o for o in vetted_orders if o.side.lower() == "sell"]
    buy_orders = [o for o in vetted_orders if o.side.lower() == "buy"]

    logger.info("Execution plan: %d SELL order(s), %d BUY order(s)", len(sell_orders), len(buy_orders))

    execution_results: dict[str, list[dict[str, Any]]] = {"successes": [], "failures": []}
    try:
        # PHASE 1: Execute SELL orders first.
        sell_results: dict[str, list[dict[str, Any]]] = {"successes": [], "failures": []}
        if sell_orders:
            logger.info("[SELL] Executing %d sell orders", len(sell_orders))
            sell_results = _submit_kraken_orders_live(
                orders=sell_orders,
                api_key=api_key or "",
                api_secret=api_secret or "",
                api_passphrase=api_passphrase,
            )
            for s in sell_results["successes"]:
                s["phase"] = "sell"
            for f in sell_results["failures"]:
                f["phase"] = "sell"
            logger.info(
                "SELL phase complete: %d succeeded, %d failed",
                len(sell_results["successes"]),
                len(sell_results["failures"]),
            )
        else:
            logger.info("SELL phase skipped: no sell orders")

        execution_results["successes"].extend(sell_results["successes"])
        execution_results["failures"].extend(sell_results["failures"])

        # PHASE 2: Refresh broker state and compute real available cash after sells.
        refreshed_state = load_account_state(
            source=args.broker_source,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
        refreshed_equity = float(refreshed_state.equity)
        available_cash = _available_cash_usd(refreshed_state)
        logger.info(
            "[EXECUTION] Post-sell refresh: equity=$%.2f, available_cash=$%.2f",
            refreshed_equity,
            available_cash,
        )

        # PHASE 3: Scale BUY orders to real available cash and execute.
        scaled_buy_orders: list[PreparedOrder] = []
        if buy_orders:
            logger.info("[BUY] Executing %d buy orders", len(buy_orders))
            total_buy_notional = float(sum(o.notional_usd for o in buy_orders))
            scale = 1.0
            if total_buy_notional > 0 and total_buy_notional > available_cash:
                scale = max(0.0, available_cash / total_buy_notional)
                logger.warning(
                    "[BUY] Scaling applied: requested=$%.2f, available=$%.2f, scale=%.4f",
                    total_buy_notional,
                    available_cash,
                    scale,
                )

            for order in buy_orders:
                scaled_notional = order.notional_usd * scale
                if scaled_notional < args.min_order_notional:
                    logger.info(
                        "BUY order dropped after scaling below min notional: %s %.2f < %.2f",
                        order.symbol,
                        scaled_notional,
                        args.min_order_notional,
                    )
                    continue
                scaled_buy_orders.append(
                    PreparedOrder(
                        symbol=order.symbol,
                        side=order.side,
                        notional_usd=min(scaled_notional, args.max_order_notional),
                    )
                )

            buy_results: dict[str, list[dict[str, Any]]] = {"successes": [], "failures": []}
            if scaled_buy_orders:
                buy_results = _submit_kraken_orders_live(
                    orders=scaled_buy_orders,
                    api_key=api_key or "",
                    api_secret=api_secret or "",
                    api_passphrase=api_passphrase,
                )
                for s in buy_results["successes"]:
                    s["phase"] = "buy"
                for f in buy_results["failures"]:
                    f["phase"] = "buy"
                logger.info(
                    "BUY phase complete: %d succeeded, %d failed",
                    len(buy_results["successes"]),
                    len(buy_results["failures"]),
                )
            else:
                logger.info("BUY phase skipped: no buy orders after scaling and min-notional filter")

            execution_results["successes"].extend(buy_results["successes"])
            execution_results["failures"].extend(buy_results["failures"])
        else:
            logger.info("BUY phase skipped: no buy orders")
    finally:
        # Always clear pending after any live execution attempt to avoid stale retries.
        clear_pending_signal()

    successes = execution_results["successes"]
    failures = execution_results["failures"]

    if successes:
        print("Submitted orders:")
        for i, success in enumerate(successes, 1):
            response = success.get("response", {})
            order_id = response.get("id", "unknown")
            symbol = response.get("symbol", success.get("symbol", "unknown"))
            side = response.get("side", success.get("side", "unknown"))
            amount = response.get("amount", success.get("amount", "unknown"))
            status = response.get("status", "unknown")
            phase = success.get("phase", "unknown")
            print(f"  {i}. phase={phase} id={order_id} symbol={symbol} side={side} amount={amount} status={status}")

    if failures:
        print("Failed orders:")
        for i, failure in enumerate(failures, 1):
            phase = failure.get("phase", "unknown")
            print(
                f"  {i}. phase={phase} symbol={failure['symbol']} side={failure['side']} "
                f"notional=${failure['notional_usd']:.2f} error={failure['error']}"
            )

    if args.notify_email:
        for success in successes:
            _notify_trade_event(
                strategy_variant=strategy_result["strategy_variant"],
                timestamp=str(strategy_result["timestamp"]),
                symbol=success["symbol"],
                side=success["side"],
                notional_usd=success["notional_usd"],
                status_text=f"submitted ({success.get('phase', 'unknown')} phase)",
            )
        for failure in failures:
            _notify_trade_event(
                strategy_variant=strategy_result["strategy_variant"],
                timestamp=str(strategy_result["timestamp"]),
                symbol=failure["symbol"],
                side=failure["side"],
                notional_usd=failure["notional_usd"],
                status_text=f"failed ({failure.get('phase', 'unknown')} phase): {failure['error']}",
            )

    if failures and successes:
        logger.error(
            "PARTIAL EXECUTION: %d succeeded, %d failed. Pending signal cleared.",
            len(successes),
            len(failures),
        )
        raise SystemExit(
            f"Partial execution: {len(successes)} succeeded, {len(failures)} failed. "
            "Pending signal was cleared."
        )

    if failures and not successes:
        logger.error(
            "ALL ORDERS FAILED: %d failed. Pending signal cleared.",
            len(failures),
        )
        raise SystemExit(
            f"All orders failed ({len(failures)}). Pending signal was cleared."
        )

    logger.info("ALL ORDERS SUCCEEDED: %d submitted. Pending signal cleared.", len(successes))


if __name__ == "__main__":
    main()
