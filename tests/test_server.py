"""Tests for mcp_smartlab.server."""

from __future__ import annotations

import json

import httpx
import pytest

from mcp_smartlab.server import (
    BOND_SECTORS,
    RATING_ORDER,
    _build_bonds_url,
    _build_fundamental_url,
    _build_shares_url,
    _rating_meets_minimum,
    get_bond_chart_data,
    get_bond_details,
    get_bond_sectors,
    get_dividend_history,
    get_top_yield_bonds,
    get_upcoming_dividends,
    search_bonds,
    search_shares,
    get_shares_fundamental,
)

from .conftest import load_fixture


def _http_404_error() -> httpx.HTTPStatusError:
    resp = httpx.Response(404, request=httpx.Request("GET", "https://example.com"))
    return httpx.HTTPStatusError("404", request=resp.request, response=resp)


# ── _rating_meets_minimum ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bond_rating, min_rating, expected",
    [
        ("AAA", "B-", True),
        ("BBB+", "BBB+", True),
        ("BB+", "BBB-", False),
        (None, "BBB+", False),
        ("A+", "INVALID", True),
        ("INVALID", "BBB+", False),
        ("D", "CCC-", False),
    ],
)
def test_rating_meets_minimum(bond_rating, min_rating, expected):
    assert _rating_meets_minimum(bond_rating, min_rating) is expected


def test_rating_order_hierarchy():
    """Each adjacent pair: earlier is higher quality (lower rank index)."""
    from mcp_smartlab.server import _RATING_RANK

    for i in range(len(RATING_ORDER) - 1):
        higher = RATING_ORDER[i]
        lower = RATING_ORDER[i + 1]
        assert _RATING_RANK[higher] < _RATING_RANK[lower], f"{higher} should rank above {lower}"


# ── _build_bonds_url ─────────────────────────────────────────────────────────


def test_build_bonds_url_defaults():
    url = _build_bonds_url("corporate", "yield", "desc")
    assert url == "/q/bonds/order_by_yield/desc/"


def test_build_bonds_url_page():
    url = _build_bonds_url("corporate", "yield", "desc", page=3)
    assert "page3/" in url


def test_build_bonds_url_with_filters():
    url = _build_bonds_url(
        "corporate", "yield", "desc",
        rating="A+", duration_from=1, duration_to=5, sector="bank",
    )
    assert "rating=A+" in url
    assert "duration_from=1" in url
    assert "duration_to=5" in url
    assert "sector=bank" in url


def test_build_bonds_url_amortization():
    url_yes = _build_bonds_url("corporate", "yield", "desc", amortization="yes")
    assert "ot=1" in url_yes
    url_no = _build_bonds_url("corporate", "yield", "desc", amortization="no")
    assert "ot=0" in url_no


def test_build_bonds_url_floater():
    url = _build_bonds_url("corporate", "yield", "desc", floater="yes")
    assert "floater=2" in url


def test_build_bonds_url_perpetual():
    url = _build_bonds_url("corporate", "yield", "desc", perpetual="yes")
    assert "perpetual=2" in url


def test_build_bonds_url_coupon_frequency():
    url = _build_bonds_url("corporate", "yield", "desc", coupon_frequency=4)
    assert "cut=4" in url


def test_build_bonds_url_unknown_type():
    url = _build_bonds_url("unknown_type", "yield", "desc")
    assert url.startswith("/q/bonds/")


def test_build_bonds_url_unknown_sort():
    url = _build_bonds_url("corporate", "unknown_sort", "desc")
    assert "order_by_yield" in url


# ── _build_shares_url / _build_fundamental_url ───────────────────────────────


def test_build_shares_url():
    url = _build_shares_url("volume", "desc")
    assert url == "/q/shares/order_by_val_to_day/desc/"


def test_build_shares_url_unknown_sort():
    url = _build_shares_url("unknown", "asc")
    assert "order_by_val_to_day" in url


def test_build_fundamental_url():
    url = _build_fundamental_url("pe", "asc")
    assert url == "/q/shares_fundamental/order_by_pe/asc/"


def test_build_fundamental_url_unknown_sort():
    url = _build_fundamental_url("unknown", "desc")
    assert "order_by_issuevalue" in url


# ── MCP tools (mocked _fetch) ───────────────────────────────────────────────


async def test_search_bonds_basic(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    result = json.loads(await search_bonds())
    assert len(result) == 3
    assert result[0]["secid"] == "RU000A106540"


async def test_search_bonds_rating_filter(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    result = json.loads(await search_bonds(rating="A+"))
    # Only AAA and AA+ should pass (BB+ excluded)
    ratings = [b["rating"] for b in result]
    assert "BB+" not in ratings
    assert len(result) == 2


@pytest.mark.parametrize(
    "kwargs, expected_in_url",
    [
        ({"duration_from": 1, "duration_to": 5}, ["duration_from=1", "duration_to=5"]),
        ({"sector": "bank"}, ["sector=bank"]),
        ({"coupon_frequency": 4}, ["cut=4"]),
        ({"amortization": "yes"}, ["ot=1"]),
        ({"amortization": "no"}, ["ot=0"]),
        ({"floater": "yes"}, ["floater=2"]),
        ({"floater": "no"}, ["floater=0"]),
        ({"perpetual": "yes"}, ["perpetual=2"]),
        ({"perpetual": "no"}, ["perpetual=0"]),
        ({"bond_type": "ofz"}, ["/q/ofz/"]),
        ({"sort_by": "duration", "sort_order": "asc"}, ["order_by_duration/asc/"]),
        ({"rating": "A+"}, ["rating=A+"]),  # server-side param (also applied client-side)
    ],
)
async def test_search_bonds_passes_filter_to_url(mock_fetch, kwargs, expected_in_url):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    await search_bonds(**kwargs)
    url = mock_fetch.call_args[0][0]
    for substr in expected_in_url:
        assert substr in url


async def test_search_bonds_all_filters_combined(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    await search_bonds(
        bond_type="subfederal",
        rating="BBB+",
        duration_from=0.5,
        duration_to=3,
        sector="bank",
        coupon_frequency=2,
        amortization="no",
        floater="yes",
        perpetual="no",
        sort_by="price",
        sort_order="asc",
    )
    url = mock_fetch.call_args[0][0]
    assert url.startswith("/q/subfed/")
    assert "order_by_last/asc/" in url
    assert "rating=BBB+" in url
    assert "duration_from=0.5" in url
    assert "duration_to=3" in url
    assert "sector=bank" in url
    assert "cut=2" in url
    assert "ot=0" in url
    assert "floater=2" in url
    assert "perpetual=0" in url


async def test_search_bonds_min_yield(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    result = json.loads(await search_bonds(min_yield=18.0))
    for b in result:
        assert b["yield_pct"] >= 18.0


async def test_search_bonds_max_yield(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    result = json.loads(await search_bonds(max_yield=19.0))
    for b in result:
        assert b["yield_pct"] <= 19.0


async def test_search_bonds_limit(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    result = json.loads(await search_bonds(limit=2))
    assert len(result) == 2


async def test_search_bonds_empty(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_empty.html")
    result = json.loads(await search_bonds())
    assert result == []


async def test_search_bonds_pagination(mock_fetch):
    """Different HTML per page, fetches multiple pages."""
    page1 = load_fixture("bonds_corporate.html").replace(
        "</table>",
        '</table><a class="page" href="page2/">2</a>',
    )
    page2 = load_fixture("bonds_corporate.html")

    mock_fetch.side_effect = [page1, page2]
    result = json.loads(await search_bonds(limit=10))
    # Should have results from both pages (3 + 3)
    assert len(result) == 6
    assert mock_fetch.call_count == 2


async def test_get_bond_details_found(mock_fetch):
    mock_fetch.return_value = load_fixture("bond_detail.html")
    result = json.loads(await get_bond_details("RU000A106540"))
    assert result["secid"] == "RU000A106540"
    assert result["title"] == "Роснефть БО-05 (RU000A106540)"
    assert len(result["coupons"]) == 3


async def test_get_bond_details_404_fallback(mock_fetch):
    mock_fetch.side_effect = [_http_404_error(), load_fixture("bond_detail.html")]
    result = json.loads(await get_bond_details("SU26238RMFS4"))
    assert result["secid"] == "SU26238RMFS4"


async def test_get_bond_details_all_404(mock_fetch):
    err = _http_404_error()
    mock_fetch.side_effect = [err, err, err, err]
    result = json.loads(await get_bond_details("NOTFOUND"))
    assert "error" in result


async def test_get_bond_chart_data_basic(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_chart_data.html")
    result = json.loads(await get_bond_chart_data())
    assert len(result) == 3
    # Should be sorted by yield descending
    yields = [i["yield_pct"] for i in result]
    assert yields == sorted(yields, reverse=True)


async def test_get_bond_chart_data_passes_filters_to_url(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_chart_data.html")
    await get_bond_chart_data(
        bond_type="ofz",
        duration_from=1,
        duration_to=10,
        sector="bank",
        floater="yes",
    )
    url = mock_fetch.call_args[0][0]
    assert url.startswith("/q/ofz/")
    assert "duration_from=1" in url
    assert "duration_to=10" in url
    assert "sector=bank" in url
    assert "floater=2" in url


async def test_get_bond_chart_data_rating_filter(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_chart_data.html")
    result = json.loads(await get_bond_chart_data(rating="A+"))
    ratings = [i["rating"] for i in result]
    assert "BB+" not in ratings


async def test_get_top_yield_bonds(mock_fetch):
    mock_fetch.return_value = load_fixture("bonds_corporate.html")
    result = json.loads(await get_top_yield_bonds())
    # Default min_rating=BBB+, so BB+ should be filtered
    ratings = [b["rating"] for b in result]
    assert "BB+" not in ratings


async def test_get_bond_sectors():
    result = json.loads(await get_bond_sectors())
    assert len(result) == 13
    assert result["bank"] == "Банки"


async def test_search_shares(mock_fetch):
    mock_fetch.return_value = load_fixture("shares.html")
    result = json.loads(await search_shares())
    assert len(result) == 2
    assert result[0]["ticker"] == "SBER"


async def test_get_shares_fundamental(mock_fetch):
    mock_fetch.return_value = load_fixture("shares_fundamental.html")
    result = json.loads(await get_shares_fundamental())
    assert len(result) == 1
    assert result[0]["ticker"] == "SBER"
    assert result[0]["pe"] == 4.5


async def test_get_upcoming_dividends(mock_fetch):
    mock_fetch.return_value = load_fixture("dividends.html")
    result = json.loads(await get_upcoming_dividends())
    assert len(result) == 2
    assert result[0]["ticker"] == "SBER"
    assert result[0]["dividend_rub"] == 33.3


async def test_get_dividend_history(mock_fetch):
    mock_fetch.return_value = load_fixture("dividends.html")
    result = json.loads(await get_dividend_history("SBER"))
    assert len(result) == 2
