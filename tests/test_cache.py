"""Tests for mcp_smartlab.cache."""

from __future__ import annotations

from freezegun import freeze_time

from mcp_smartlab.cache import Cache, TTL_DIVIDENDS, TTL_FUNDAMENTAL, TTL_HISTORY, TTL_SCREENER


def test_get_missing_key():
    c = Cache()
    assert c.get("nonexistent") is None


def test_set_and_get():
    c = Cache()
    c.set("k", {"bonds": [1, 2]}, ttl_minutes=60)
    assert c.get("k") == {"bonds": [1, 2]}


@freeze_time("2026-01-01 12:00:00", tz_offset=0)
def test_expired_entry():
    c = Cache()
    c.set("k", "data", ttl_minutes=10)

    with freeze_time("2026-01-01 12:11:00", tz_offset=0):
        assert c.get("k") is None


@freeze_time("2026-01-01 12:00:00", tz_offset=0)
def test_expired_entry_deleted():
    c = Cache()
    c.set("k", "data", ttl_minutes=10)

    with freeze_time("2026-01-01 12:11:00", tz_offset=0):
        c.get("k")  # triggers deletion
        assert "k" not in c._store


def test_overwrite():
    c = Cache()
    c.set("k", "first", ttl_minutes=60)
    c.set("k", "second", ttl_minutes=60)
    assert c.get("k") == "second"


def test_invalidate_one():
    c = Cache()
    c.set("a", 1, ttl_minutes=60)
    c.set("b", 2, ttl_minutes=60)
    c.invalidate("a")
    assert c.get("a") is None
    assert c.get("b") == 2


def test_invalidate_all():
    c = Cache()
    c.set("a", 1, ttl_minutes=60)
    c.set("b", 2, ttl_minutes=60)
    c.invalidate(None)
    assert c.get("a") is None
    assert c.get("b") is None


def test_invalidate_nonexistent():
    c = Cache()
    c.invalidate("nope")  # should not raise


def test_ttl_constants():
    assert TTL_SCREENER == 60
    assert TTL_FUNDAMENTAL == 240
    assert TTL_DIVIDENDS == 240
    assert TTL_HISTORY == 1440
