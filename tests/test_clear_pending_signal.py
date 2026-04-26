from __future__ import annotations

import json

from live import signal_state
from live.clear_pending_signal import clear_pending_signal_state


def test_clear_pending_signal_state_removes_existing_pending_file(tmp_path, monkeypatch) -> None:
    signal_dir = tmp_path / ".signals"
    pending_path = signal_dir / "pending_signal.json"

    monkeypatch.setattr(signal_state, "SIGNAL_STATE_DIR", signal_dir)
    monkeypatch.setattr(signal_state, "PENDING_SIGNAL_FILE", pending_path)

    signal_dir.mkdir(parents=True, exist_ok=True)
    with open(pending_path, "w", encoding="utf-8") as handle:
        json.dump({"timestamp": "2026-04-26 20:00:00+00:00", "target_weights": {}}, handle)

    cleared = clear_pending_signal_state()

    assert cleared is True
    assert not pending_path.exists()


def test_clear_pending_signal_state_when_missing_returns_false(tmp_path, monkeypatch) -> None:
    signal_dir = tmp_path / ".signals"
    pending_path = signal_dir / "pending_signal.json"

    monkeypatch.setattr(signal_state, "SIGNAL_STATE_DIR", signal_dir)
    monkeypatch.setattr(signal_state, "PENDING_SIGNAL_FILE", pending_path)

    cleared = clear_pending_signal_state()

    assert cleared is False
    assert not pending_path.exists()
