"""Shared fixtures for mcp-smartlab tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    """Read an HTML fixture file."""
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _reset_server_globals():
    """Reset server module globals between tests."""
    import mcp_smartlab.server as srv

    srv._cache = srv.Cache()
    srv._client = None
    srv._last_request_time = 0
    yield


@pytest.fixture
def mock_fetch():
    """Patch mcp_smartlab.server._fetch with an AsyncMock."""
    with patch("mcp_smartlab.server._fetch", new_callable=AsyncMock) as m:
        yield m
