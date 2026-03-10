"""Agent memory guard — I/O bridge for the memory system.

Handles persistence of memory events (append-only JSONL) and the playbook
(mutable JSON). Thread-safe for single-writer (WOLF tick loop).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from modules.memory_engine import MemoryEvent, Playbook
from parent.store import JSONLStore


class MemoryGuard:
    """Persists agent memory events and playbook to disk."""

    def __init__(self, data_dir: str = "data/wolf/memory"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._store = JSONLStore(path=str(self.data_dir / "memory.jsonl"))
        self._playbook_path = self.data_dir / "playbook.json"

    def log_event(self, event: MemoryEvent) -> None:
        """Append a memory event to the JSONL log."""
        self._store.append(event.to_dict())

    def read_events(
        self,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[MemoryEvent]:
        """Read memory events, optionally filtered by type. Most recent first."""
        raw = self._store.read_all()
        events = [MemoryEvent.from_dict(r) for r in raw]
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        events.sort(key=lambda e: e.timestamp_ms, reverse=True)
        return events[:limit]

    def load_playbook(self) -> Playbook:
        """Load the playbook from disk, or return empty if none exists."""
        if not self._playbook_path.exists():
            return Playbook()
        try:
            data = json.loads(self._playbook_path.read_text())
            return Playbook.from_dict(data)
        except (json.JSONDecodeError, KeyError):
            return Playbook()

    def save_playbook(self, playbook: Playbook) -> None:
        """Write the playbook to disk (atomic via temp file)."""
        tmp = self._playbook_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(playbook.to_dict(), indent=2, default=str))
        tmp.replace(self._playbook_path)
