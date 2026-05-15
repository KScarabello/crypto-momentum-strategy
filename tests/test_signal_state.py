"""Tests for signal state save/load functionality."""

from __future__ import annotations

import json
import logging

from live import signal_state


def test_save_and_load_pending_signal_round_trip(tmp_path, monkeypatch) -> None:
    """Verify that save_pending_signal and load_pending_signal round-trip the same dictionary."""
    signal_dir = tmp_path / ".signals"
    pending_path = signal_dir / "pending_signal.json"

    monkeypatch.setattr(signal_state, "SIGNAL_STATE_DIR", signal_dir)
    monkeypatch.setattr(signal_state, "PENDING_SIGNAL_FILE", pending_path)

    # Create a complete signal dictionary with all the diagnostic fields
    test_signal = {
        "strategy_variant": "momentum_v1",
        "timestamp": "2026-04-26 20:00:00+00:00",
        "selected_symbols": ["BTC", "ETH", "SOL"],
        "target_weights": {"BTC": 0.4, "ETH": 0.3, "SOL": 0.3},
        "risk_on": True,
        "is_rebalance_bar": True,
        "data_fresh": True,
    }

    # Save the signal
    signal_state.save_pending_signal(test_signal)
    assert pending_path.exists()

    # Load it back and verify all fields match
    loaded_signal = signal_state.load_pending_signal()
    assert loaded_signal is not None
    assert loaded_signal == test_signal

    # Verify each field individually
    assert loaded_signal["timestamp"] == "2026-04-26 20:00:00+00:00"
    assert loaded_signal["selected_symbols"] == ["BTC", "ETH", "SOL"]
    assert loaded_signal["target_weights"] == {"BTC": 0.4, "ETH": 0.3, "SOL": 0.3}
    assert loaded_signal["risk_on"] is True
    assert loaded_signal["is_rebalance_bar"] is True
    assert loaded_signal["data_fresh"] is True


def test_load_pending_signal_returns_none_when_missing(tmp_path, monkeypatch) -> None:
    """Verify that load_pending_signal returns None when no signal file exists."""
    signal_dir = tmp_path / ".signals"
    pending_path = signal_dir / "pending_signal.json"

    monkeypatch.setattr(signal_state, "SIGNAL_STATE_DIR", signal_dir)
    monkeypatch.setattr(signal_state, "PENDING_SIGNAL_FILE", pending_path)

    # Don't create the file
    loaded_signal = signal_state.load_pending_signal()
    assert loaded_signal is None


def test_save_pending_signal_logs_all_diagnostic_fields(tmp_path, monkeypatch, caplog) -> None:
    """Verify that save_pending_signal logs all diagnostic fields."""
    signal_dir = tmp_path / ".signals"
    pending_path = signal_dir / "pending_signal.json"

    monkeypatch.setattr(signal_state, "SIGNAL_STATE_DIR", signal_dir)
    monkeypatch.setattr(signal_state, "PENDING_SIGNAL_FILE", pending_path)

    test_signal = {
        "strategy_variant": "momentum_v1",
        "timestamp": "2026-04-26 20:00:00+00:00",
        "selected_symbols": ["BTC", "ETH", "SOL"],
        "target_weights": {"BTC": 0.4, "ETH": 0.3, "SOL": 0.3},
        "risk_on": True,
        "is_rebalance_bar": True,
        "data_fresh": True,
    }

    with caplog.at_level(logging.INFO, logger="live.signal_state"):
        signal_state.save_pending_signal(test_signal)

    # Verify that the log message contains all diagnostic fields
    log_messages = [record.message for record in caplog.records]
    assert len(log_messages) > 0
    log_msg = log_messages[0]
    assert "Saved pending signal" in log_msg
    assert "2026-04-26 20:00:00+00:00" in log_msg
    assert "['BTC', 'ETH', 'SOL']" in log_msg
    assert "target_weights" in log_msg


def test_load_pending_signal_logs_all_diagnostic_fields(tmp_path, monkeypatch, caplog) -> None:
    """Verify that load_pending_signal logs all diagnostic fields."""
    signal_dir = tmp_path / ".signals"
    pending_path = signal_dir / "pending_signal.json"

    monkeypatch.setattr(signal_state, "SIGNAL_STATE_DIR", signal_dir)
    monkeypatch.setattr(signal_state, "PENDING_SIGNAL_FILE", pending_path)

    test_signal = {
        "strategy_variant": "momentum_v1",
        "timestamp": "2026-04-26 20:00:00+00:00",
        "selected_symbols": ["BTC", "ETH"],
        "target_weights": {"BTC": 0.5, "ETH": 0.5},
        "risk_on": False,
        "is_rebalance_bar": False,
        "data_fresh": False,
    }

    # Save the signal directly to bypass logging
    signal_dir.mkdir(parents=True, exist_ok=True)
    with open(pending_path, "w") as f:
        json.dump(test_signal, f)

    # Now load it and verify the log
    with caplog.at_level(logging.INFO, logger="live.signal_state"):
        loaded = signal_state.load_pending_signal()

    # Verify that the log message contains all diagnostic fields
    log_messages = [record.message for record in caplog.records]
    assert len(log_messages) > 0
    log_msg = log_messages[0]
    assert "Loaded pending signal" in log_msg
    assert "2026-04-26 20:00:00+00:00" in log_msg
    assert "['BTC', 'ETH']" in log_msg
    assert "target_weights" in log_msg

    # Verify the signal was loaded correctly
    assert loaded is not None
    assert loaded == test_signal
