from __future__ import annotations

from live.broker_state import AccountState
from live.preview_five_asset_rebalance import build_hypothetical_five_asset_rebalance


def test_hypothetical_five_asset_preview_uses_full_universe_and_builds_orders(monkeypatch) -> None:
    preview_symbols = ("BTC/USD", "ETH/USD", "XRP/USD", "SOL/USD", "AVAX/USD")
    loader_calls: list[dict[str, object]] = []

    def fake_generate_targets(symbols):
        assert symbols == preview_symbols
        return {
            "strategy_variant": "locked_baseline",
            "timestamp": "2026-04-26 20:00:00+00:00",
            "risk_on": True,
            "selected_symbols": ["BTC/USD", "SOL/USD", "AVAX/USD"],
            "target_weights": {
                "BTC/USD": 0.25,
                "ETH/USD": 0.00,
                "XRP/USD": 0.00,
                "SOL/USD": 0.25,
                "AVAX/USD": 0.25,
            },
            "cash_weight": 0.25,
        }

    def fake_load_account_state(**kwargs):
        loader_calls.append(kwargs)
        return AccountState(
            equity=1_000.0,
            positions={
                "BTC/USD": 400.0,
                "ETH/USD": 100.0,
                "XRP/USD": 100.0,
            },
            available_cash=400.0,
        )

    monkeypatch.setattr("live.preview_five_asset_rebalance.get_data_symbols", lambda: preview_symbols)
    monkeypatch.setattr("live.preview_five_asset_rebalance.generate_targets", fake_generate_targets)
    monkeypatch.setattr("live.preview_five_asset_rebalance.load_account_state", fake_load_account_state)

    preview = build_hypothetical_five_asset_rebalance(
        broker_source="real",
        broker_name="kraken",
        api_key="key",
        api_secret="secret",
        api_passphrase=None,
        min_trade_notional=10.0,
    )

    assert loader_calls == [
        {
            "source": "real",
            "api_key": "key",
            "api_secret": "secret",
            "api_passphrase": None,
            "symbols": preview_symbols,
        }
    ]
    assert preview["symbols"] == preview_symbols
    assert preview["selected_symbols"] == ["BTC/USD", "SOL/USD", "AVAX/USD"]
    assert preview["current_weights"]["BTC/USD"] == 0.4
    assert preview["target_weights"]["SOL/USD"] == 0.25
    assert [order.symbol for order in preview["sells"]] == ["BTC/USD", "ETH/USD", "XRP/USD"]
    assert [order.symbol for order in preview["buys"]] == ["AVAX/USD", "SOL/USD"]
