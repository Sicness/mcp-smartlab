"""In-memory TTL cache for smart-lab.ru responses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass
class _Entry:
    data: Any
    expires_at: datetime


class Cache:
    """Simple in-memory cache with per-key TTL."""

    def __init__(self) -> None:
        self._store: dict[str, _Entry] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if datetime.now(timezone.utc) > entry.expires_at:
            del self._store[key]
            return None
        return entry.data

    def set(self, key: str, data: Any, ttl_minutes: int = 60) -> None:
        self._store[key] = _Entry(
            data=data,
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes),
        )

    def invalidate(self, key: str | None = None) -> None:
        if key is None:
            self._store.clear()
        else:
            self._store.pop(key, None)


# TTL presets (minutes)
TTL_SCREENER = 60       # bonds / shares screener
TTL_FUNDAMENTAL = 240   # fundamental data
TTL_DIVIDENDS = 240     # dividend calendar
TTL_HISTORY = 1440      # dividend history (24h)
