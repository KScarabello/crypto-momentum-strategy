from __future__ import annotations

import sys
import types

from live.execute_orders import _plan_cash_aware_buy_orders, _submit_kraken_orders_live
from live.prepare_orders import PreparedOrder


def test_cash_aware_buy_planner_scales_total_buys_to_spendable_cash() -> None:
    buy_orders = [
        PreparedOrder(symbol="ETH/USD", side="buy", notional_usd=25.0),
        PreparedOrder(symbol="XRP/USD", side="buy", notional_usd=25.0),
    ]

    planned, skipped, summary = _plan_cash_aware_buy_orders(
        buy_orders=buy_orders,
        available_cash=30.0,
        min_order_notional=10.0,
        max_order_notional=None,
        cash_buffer_usd=1.0,
    )

    assert len(planned) == 2
    assert skipped == []
    assert round(sum(order.notional_usd for order in planned), 2) == 29.0
    assert round(summary["spendable_cash"], 2) == 29.0


def test_cash_aware_buy_planner_can_support_only_some_buys() -> None:
    buy_orders = [
        PreparedOrder(symbol="ETH/USD", side="buy", notional_usd=25.0),
        PreparedOrder(symbol="XRP/USD", side="buy", notional_usd=10.0),
    ]

    planned, skipped, summary = _plan_cash_aware_buy_orders(
        buy_orders=buy_orders,
        available_cash=16.0,
        min_order_notional=10.0,
        max_order_notional=None,
        cash_buffer_usd=1.0,
    )

    assert len(planned) == 1
    assert planned[0].symbol == "ETH/USD"
    assert round(planned[0].notional_usd, 2) == 10.71
    assert len(skipped) == 1
    assert skipped[0]["symbol"] == "XRP/USD"
    assert "below min order notional" in skipped[0]["reason"]
    assert round(summary["allocated_cash"], 2) == 10.71


def test_cash_aware_buy_planner_skips_buy_below_min_notional() -> None:
    buy_orders = [
        PreparedOrder(symbol="ETH/USD", side="buy", notional_usd=12.0),
    ]

    planned, skipped, summary = _plan_cash_aware_buy_orders(
        buy_orders=buy_orders,
        available_cash=9.0,
        min_order_notional=10.0,
        max_order_notional=None,
        cash_buffer_usd=1.0,
    )

    assert planned == []
    assert len(skipped) == 1
    assert skipped[0]["symbol"] == "ETH/USD"
    assert round(summary["spendable_cash"], 2) == 8.0


def test_submit_kraken_orders_live_collects_failures_without_aborting(monkeypatch) -> None:
    class FakeExchange:
        def fetch_tickers(self, symbols):
            return {
                symbol: {"last": 10.0}
                for symbol in symbols
            }

        def fetch_balance(self):
            return {"free": {"USD": 100.0}}

        def create_order(self, symbol, type, side, amount):
            if symbol == "ETH/USD":
                raise RuntimeError("kraken {\"error\":[\"EOrder:Insufficient funds\"]}")
            return {
                "id": "ok-1",
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "status": "closed",
            }

    fake_ccxt = types.SimpleNamespace(kraken=lambda config: FakeExchange())
    monkeypatch.setitem(sys.modules, "ccxt", fake_ccxt)

    results = _submit_kraken_orders_live(
        orders=[
            PreparedOrder(symbol="ETH/USD", side="buy", notional_usd=25.0),
            PreparedOrder(symbol="XRP/USD", side="buy", notional_usd=25.0),
        ],
        api_key="key",
        api_secret="secret",
        api_passphrase=None,
    )

    assert len(results["failures"]) == 1
    assert results["failures"][0]["symbol"] == "ETH/USD"
    assert len(results["successes"]) == 1
    assert results["successes"][0]["symbol"] == "XRP/USD"


def test_cash_aware_buy_planner_leaves_orders_uncapped_when_no_max_is_configured() -> None:
    buy_orders = [
        PreparedOrder(symbol="ETH/USD", side="buy", notional_usd=80.0),
        PreparedOrder(symbol="XRP/USD", side="buy", notional_usd=20.0),
    ]

    planned, skipped, summary = _plan_cash_aware_buy_orders(
        buy_orders=buy_orders,
        available_cash=150.0,
        min_order_notional=10.0,
        max_order_notional=None,
        cash_buffer_usd=1.0,
    )

    assert skipped == []
    assert [order.notional_usd for order in planned] == [80.0, 20.0]
    assert round(summary["allocated_cash"], 2) == 100.0


def test_submit_kraken_orders_live_caps_sell_by_available_asset_balance(monkeypatch) -> None:
    class FakeExchange:
        def fetch_tickers(self, symbols):
            return {symbol: {"last": 10.0} for symbol in symbols}

        def fetch_balance(self):
            return {"free": {"BTC": 1.5}}

        def create_order(self, symbol, type, side, amount):
            return {
                "id": "sell-1",
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "status": "closed",
            }

    fake_ccxt = types.SimpleNamespace(kraken=lambda config: FakeExchange())
    monkeypatch.setitem(sys.modules, "ccxt", fake_ccxt)

    results = _submit_kraken_orders_live(
        orders=[PreparedOrder(symbol="BTC/USD", side="sell", notional_usd=20.0)],
        api_key="key",
        api_secret="secret",
        api_passphrase=None,
    )

    assert results["failures"] == []
    assert len(results["successes"]) == 1
    assert results["successes"][0]["amount"] == 1.5
    assert results["successes"][0]["notional_usd"] == 15.0