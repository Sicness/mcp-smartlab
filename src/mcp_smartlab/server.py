"""MCP server for scraping financial data from smart-lab.ru."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
from mcp.server.fastmcp import FastMCP

from . import parser
from .cache import Cache, TTL_DIVIDENDS, TTL_FUNDAMENTAL, TTL_HISTORY, TTL_SCREENER

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)

mcp = FastMCP(
    "mcp-smartlab",
    instructions=(
        "MCP server for scraping financial data from smart-lab.ru — "
        "a Russian financial portal. Provides bond screening with filters "
        "(rating, duration, sector, yield, floater/fixed, amortization), "
        "stock screening with market data and fundamentals (P/E, P/B, div yield), "
        "and dividend calendar. All data is scraped from public HTML pages. "
        "Bond yields and prices are in percent. Monetary values are in Russian rubles "
        "unless specified otherwise. Data is cached in-memory (1h for screener, 4h for fundamentals). "
        "Use search_bonds for bond screening, get_top_yield_bonds for quick high-yield "
        "discovery, search_shares / get_shares_fundamental for stocks, "
        "and get_upcoming_dividends for the dividend calendar."
    ),
)

_cache = Cache()
_client: httpx.AsyncClient | None = None
_semaphore = asyncio.Semaphore(3)
_last_request_time: float = 0


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url="https://smart-lab.ru",
            timeout=30,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            },
            follow_redirects=True,
        )
    return _client


async def _fetch(path: str, cache_key: str | None = None, ttl: int = TTL_SCREENER) -> str:
    """Fetch a page with rate limiting and optional caching."""
    if cache_key:
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    global _last_request_time
    async with _semaphore:
        # Polite delay between requests
        now = asyncio.get_event_loop().time()
        elapsed = now - _last_request_time
        if elapsed < 0.5:
            await asyncio.sleep(0.5 - elapsed)

        client = await _get_client()
        resp = await client.get(path)
        resp.raise_for_status()
        _last_request_time = asyncio.get_event_loop().time()

    html = resp.text
    if cache_key:
        _cache.set(cache_key, html, ttl)
    return html


def _fmt(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


# ── URL building ─────────────────────────────────────────────────────────────

BOND_TYPE_MAP = {
    "corporate": "/q/bonds/",
    "ofz": "/q/ofz/",
    "subfederal": "/q/subfed/",
    "currency": "/q/cur_bonds/",
    "other": "/q/other_bonds/",
}

BOND_SORT_MAP = {
    "yield": "order_by_yield",
    "duration": "order_by_duration",
    "price": "order_by_last",
    "maturity": "order_by_mat_date",
    "volume": "order_by_val_to_day",
    "name": "order_by_short_name",
    "coupon_yield": "order_by_year_yield",
    "nkd": "order_by_accruedint",
    "years": "order_by_mat_years",
}

SHARE_SORT_MAP = {
    "volume": "order_by_val_to_day",
    "price": "order_by_last",
    "change_1d": "order_by_last_change_prcnt",
    "change_1w": "order_by_7d",
    "change_1m": "order_by_1m",
    "change_ytd": "order_by_ytd",
    "change_12m": "order_by_12m",
    "market_cap": "order_by_issuevalue",
    "name": "order_by_short_name",
}

FUNDAMENTAL_SORT_MAP = {
    "market_cap": "order_by_issuevalue",
    "ev": "order_by_ev",
    "revenue": "order_by_revenue",
    "net_income": "order_by_net_income",
    "div_yield": "order_by_div_yield",
    "pe": "order_by_pe",
    "ps": "order_by_ps",
    "pb": "order_by_pb",
    "ev_ebitda": "order_by_ev_ebitda",
    "ebitda_margin": "order_by_ebitda_margin",
    "debt_ebitda": "order_by_debt_ebitda",
}

# Rating hierarchy: index 0 = highest quality. Used for client-side filtering
# because smart-lab.ru's server-side rating filter is unreliable.
RATING_ORDER = [
    "AAA", "AA+", "AA", "AA-",
    "A+", "A", "A-",
    "BBB+", "BBB", "BBB-",
    "BB+", "BB", "BB-",
    "B+", "B", "B-",
    "CCC+", "CCC", "CCC-",
    "CC", "C", "D",
]
_RATING_RANK = {r: i for i, r in enumerate(RATING_ORDER)}


def _rating_meets_minimum(bond_rating: str | None, min_rating: str) -> bool:
    """Return True if bond_rating is at or above min_rating in credit quality."""
    if not bond_rating:
        return False
    min_rank = _RATING_RANK.get(min_rating)
    if min_rank is None:
        return True  # unknown min_rating — don't filter
    bond_rank = _RATING_RANK.get(bond_rating)
    if bond_rank is None:
        return False  # unknown bond rating — exclude
    return bond_rank <= min_rank


BOND_SECTORS = {
    "bank": "Банки",
    "oil": "Нефть и газ",
    "finance": "Финансы",
    "leasing": "Лизинг",
    "build": "Строительство",
    "it": "IT",
    "metal": "Металлургия",
    "chemistry": "Химия",
    "food": "Пищевая промышленность",
    "retail": "Ритейл",
    "transport": "Транспорт",
    "telecom": "Телеком",
    "energy": "Энергетика",
}


def _build_bonds_url(
    bond_type: str,
    sort_by: str,
    sort_order: str,
    page: int = 1,
    **filters: Any,
) -> str:
    base = BOND_TYPE_MAP.get(bond_type, "/q/bonds/")
    sort_field = BOND_SORT_MAP.get(sort_by, "order_by_yield")
    path = f"{base}{sort_field}/{sort_order}/"
    if page > 1:
        path += f"page{page}/"

    params = {}
    if filters.get("rating"):
        params["rating"] = filters["rating"]
    if filters.get("duration_from") is not None:
        params["duration_from"] = str(filters["duration_from"])
    if filters.get("duration_to") is not None:
        params["duration_to"] = str(filters["duration_to"])
    if filters.get("sector"):
        params["sector"] = filters["sector"]
    if filters.get("coupon_frequency") is not None:
        params["cut"] = str(filters["coupon_frequency"])

    # amortization: "" (all), "yes" -> ot=1, "no" -> ot=0
    amort = filters.get("amortization", "")
    if amort == "yes":
        params["ot"] = "1"
    elif amort == "no":
        params["ot"] = "0"

    # floater: "" (all), "yes" -> floater=2, "no" -> floater=0
    floater = filters.get("floater", "")
    if floater == "yes":
        params["floater"] = "2"
    elif floater == "no":
        params["floater"] = "0"

    # perpetual: "" (all), "yes" -> perpetual=2, "no" -> perpetual=0
    perp = filters.get("perpetual", "")
    if perp == "yes":
        params["perpetual"] = "2"
    elif perp == "no":
        params["perpetual"] = "0"

    if params:
        path += f"?{urlencode(params)}"

    return path


def _build_shares_url(sort_by: str, sort_order: str) -> str:
    sort_field = SHARE_SORT_MAP.get(sort_by, "order_by_val_to_day")
    return f"/q/shares/{sort_field}/{sort_order}/"


def _build_fundamental_url(sort_by: str, sort_order: str) -> str:
    sort_field = FUNDAMENTAL_SORT_MAP.get(sort_by, "order_by_issuevalue")
    return f"/q/shares_fundamental/{sort_field}/{sort_order}/"


# ── Bond tools ───────────────────────────────────────────────────────────────


@mcp.tool()
async def search_bonds(
    bond_type: str = "corporate",
    rating: str = "",
    duration_from: float | None = None,
    duration_to: float | None = None,
    sector: str = "",
    coupon_frequency: int | None = None,
    amortization: str = "",
    floater: str = "",
    perpetual: str = "",
    sort_by: str = "yield",
    sort_order: str = "desc",
    min_yield: float | None = None,
    max_yield: float | None = None,
    limit: int = 50,
) -> str:
    """Search and filter bonds on smart-lab.ru.

    Returns bonds matching criteria sorted as specified. Fetches multiple pages if needed.

    Args:
        bond_type: Type of bonds — "corporate", "ofz", "subfederal", "currency", "other"
        rating: Minimum credit rating filter (e.g. "A+", "BBB-", "AA")
        duration_from: Minimum duration in years
        duration_to: Maximum duration in years
        sector: Industry sector (use get_bond_sectors for list)
        coupon_frequency: Coupon payments per year (1, 2, 4, or 12)
        amortization: "" for all, "yes" for amortizing, "no" for bullet
        floater: "" for all, "yes" for floating rate, "no" for fixed rate
        perpetual: "" for all, "yes" for perpetual, "no" for dated
        sort_by: Sort field — "yield", "duration", "price", "maturity", "volume", "name", "coupon_yield", "years"
        sort_order: "asc" or "desc"
        min_yield: Post-filter: minimum yield percentage
        max_yield: Post-filter: maximum yield percentage
        limit: Maximum results to return (default 50)
    """
    filters = dict(
        rating=rating,
        duration_from=duration_from,
        duration_to=duration_to,
        sector=sector,
        coupon_frequency=coupon_frequency,
        amortization=amortization,
        floater=floater,
        perpetual=perpetual,
    )

    # When searching for investment-grade bonds (BBB+ and above) sorted by yield
    # descending, Smartlab's server-side rating filter is unreliable.  IG bonds
    # have lower yields than junk and appear on pages 17-22+ when sorted desc,
    # far beyond the safe page limit.  Fix: internally flip to ascending sort so
    # IG bonds appear on early pages (after negative-yield structured products),
    # skip negative yields via min_yield ≥ 0, then re-sort descending.
    _ig_cutoff_rank = _RATING_RANK.get("BBB+", 10)
    _filter_rank = _RATING_RANK.get(rating) if rating else None
    _is_ig_search = (
        _filter_rank is not None
        and _filter_rank <= _ig_cutoff_rank
        and sort_by == "yield"
        and sort_order == "desc"
    )

    fetch_sort_order = "asc" if _is_ig_search else sort_order
    # Exclude negative-yield structured products and zero-yield (no-data) bonds
    # when doing IG ascending sweep.  Real tradeable bonds always have yield > 0.
    fetch_min_yield = max(min_yield if min_yield is not None else 1.0, 1.0) if _is_ig_search else min_yield
    # IG search needs more pages: ~10 pages of zero-yield old bonds before real data starts.
    # Smartlab also hides pagination links when a rating filter is active,
    # so total_pages will appear as 1 even though more pages exist.
    max_pages = 30 if _is_ig_search else 10

    all_bonds: list[dict] = []
    page = 1
    while page <= max_pages:
        url = _build_bonds_url(bond_type, sort_by, fetch_sort_order, page=page, **filters)
        cache_key = f"bonds:{url}"
        html = await _fetch(url, cache_key=cache_key, ttl=TTL_SCREENER)
        bonds = parser.parse_bonds_table(html)
        if not bonds:
            break

        # Apply post-filters
        if rating:
            bonds = [b for b in bonds if _rating_meets_minimum(b.get("rating"), rating)]
        if fetch_min_yield is not None:
            bonds = [b for b in bonds if b.get("yield_pct") is not None and b["yield_pct"] >= fetch_min_yield]
        if max_yield is not None:
            bonds = [b for b in bonds if b.get("yield_pct") is not None and b["yield_pct"] <= max_yield]

        all_bonds.extend(bonds)
        # Smartlab hides pagination links when a rating filter is active,
        # making total_pages appear as 1 even when more pages exist.
        # For IG search, rely solely on max_pages + empty-page detection.
        if not _is_ig_search:
            total_pages = parser.parse_pagination(html)
            if page >= total_pages:
                break
            if len(all_bonds) >= limit:
                break
        page += 1

    if _is_ig_search:
        all_bonds.sort(key=lambda b: b.get("yield_pct") or 0, reverse=True)

    return _fmt(all_bonds[:limit])


@mcp.tool()
async def get_bond_details(secid: str) -> str:
    """Get detailed information about a specific bond including coupon schedule.

    Args:
        secid: Bond security identifier (e.g. "RU000A10EJQ7")
    """
    # Try corporate bonds first, then OFZ
    for bond_type_path in ("/q/bonds/", "/q/ofz/", "/q/subfed/", "/q/cur_bonds/"):
        url = f"{bond_type_path}{secid}/"
        cache_key = f"bond_detail:{secid}"
        try:
            html = await _fetch(url, cache_key=cache_key, ttl=TTL_SCREENER)
            detail = parser.parse_bond_detail(html)
            if detail.get("title") or detail.get("coupons"):
                detail["secid"] = secid
                detail["url"] = f"https://smart-lab.ru{url}"
                return _fmt(detail)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                continue
            raise

    return _fmt({"error": f"Bond {secid} not found"})


@mcp.tool()
async def get_bond_chart_data(
    bond_type: str = "corporate",
    rating: str = "",
    duration_from: float | None = None,
    duration_to: float | None = None,
    sector: str = "",
    floater: str = "",
    limit: int = 100,
) -> str:
    """Get bond yield vs duration data from the aBondsChartData JS variable.

    Returns lightweight data without pagination — contains ALL bonds matching filters.
    Faster than search_bonds when you only need yield/duration/rating overview.
    Returns the "wc" (with coupon) series by default.

    Args:
        bond_type: "corporate", "ofz", "subfederal", "currency", "other"
        rating: Minimum credit rating filter
        duration_from: Min duration years
        duration_to: Max duration years
        sector: Industry sector
        floater: "yes"/"no"/"" for all
        limit: Max results to return (default 100)
    """
    filters = dict(
        rating=rating,
        duration_from=duration_from,
        duration_to=duration_to,
        sector=sector,
        floater=floater,
    )
    url = _build_bonds_url(bond_type, "yield", "desc", **filters)
    cache_key = f"chart:{url}"
    html = await _fetch(url, cache_key=cache_key, ttl=TTL_SCREENER)
    chart_data = parser.parse_bonds_chart_data(html)

    # Return "wc" (with coupon yield, duration) — most useful for investors
    items = chart_data.get("wc", [])

    # Client-side rating filter
    if rating:
        items = [i for i in items if _rating_meets_minimum(i.get("rating"), rating)]

    # Sort by yield descending
    items.sort(key=lambda x: x.get("yield_pct") or 0, reverse=True)

    return _fmt(items[:limit])


@mcp.tool()
async def get_top_yield_bonds(
    bond_type: str = "corporate",
    min_rating: str = "BBB+",
    max_duration: float = 5.0,
    floater: str = "",
    limit: int = 20,
) -> str:
    """Get top-yielding bonds filtered by credit quality and duration.

    Convenience tool for conservative investors: defaults to investment-grade
    (BBB+ and above) bonds with up to 5 years duration, sorted by yield desc.

    Args:
        bond_type: "corporate", "ofz", "subfederal" (default: corporate)
        min_rating: Minimum credit rating (default: BBB+)
        max_duration: Maximum duration in years (default: 5.0)
        floater: "yes" for floaters only, "no" for fixed only, "" for all
        limit: Number of top results (default: 20)
    """
    return await search_bonds(
        bond_type=bond_type,
        rating=min_rating,
        duration_to=max_duration,
        floater=floater,
        sort_by="yield",
        sort_order="desc",
        limit=limit,
    )


@mcp.tool()
async def compare_bonds(secids: str) -> str:
    """Compare multiple bonds side by side.

    Fetches data for each bond from the search results and returns comparison.

    Args:
        secids: Comma-separated list of bond secids (e.g. "RU000A10EJQ7,RU000A106540")
    """
    ids = [s.strip() for s in secids.split(",") if s.strip()]
    results = []

    for secid in ids:
        detail = await get_bond_details(secid)
        results.append(json.loads(detail))

    return _fmt(results)


@mcp.tool()
async def get_bond_sectors() -> str:
    """Get list of available bond sector filters.

    Returns sector codes and their Russian names for use with the 'sector'
    parameter in search_bonds and get_top_yield_bonds.
    """
    return _fmt(BOND_SECTORS)


# ── Share tools ──────────────────────────────────────────────────────────────


@mcp.tool()
async def search_shares(
    sort_by: str = "volume",
    sort_order: str = "desc",
    limit: int = 50,
) -> str:
    """Search shares (stocks) on Moscow Exchange via smart-lab.ru.

    Returns market data: ticker, name, price, daily/weekly/monthly/YTD/12M changes,
    trading volume, and market capitalization.

    Args:
        sort_by: Sort field — "volume", "price", "change_1d", "change_1w", "change_1m", "change_ytd", "change_12m", "market_cap", "name"
        sort_order: "asc" or "desc"
        limit: Maximum results (default 50)
    """
    url = _build_shares_url(sort_by, sort_order)
    cache_key = f"shares:{url}"
    html = await _fetch(url, cache_key=cache_key, ttl=TTL_SCREENER)
    shares = parser.parse_shares_table(html)
    return _fmt(shares[:limit])


@mcp.tool()
async def get_shares_fundamental(
    sort_by: str = "market_cap",
    sort_order: str = "desc",
    limit: int = 50,
) -> str:
    """Get fundamental financial data for Moscow Exchange shares.

    Returns: market cap, EV, revenue, net income, dividend yields (common/preferred),
    payout ratio, P/E, P/S, P/B, EV/EBITDA, EBITDA margin, debt/EBITDA.

    Args:
        sort_by: Sort field — "market_cap", "ev", "revenue", "net_income", "div_yield", "pe", "ps", "pb", "ev_ebitda", "ebitda_margin", "debt_ebitda"
        sort_order: "asc" or "desc"
        limit: Maximum results (default 50)
    """
    url = _build_fundamental_url(sort_by, sort_order)
    cache_key = f"fundamental:{url}"
    html = await _fetch(url, cache_key=cache_key, ttl=TTL_FUNDAMENTAL)
    shares = parser.parse_shares_fundamental_table(html)
    return _fmt(shares[:limit])


# ── Dividend tools ───────────────────────────────────────────────────────────


@mcp.tool()
async def get_upcoming_dividends(
    limit: int = 50,
) -> str:
    """Get upcoming dividend payments calendar from smart-lab.ru.

    Returns: company name, ticker, period, dividend amount (RUB), dividend yield,
    board approval status, last buy date, registry close date, payment deadline, stock price.

    Args:
        limit: Maximum results (default 50)
    """
    url = "/dividends/"
    cache_key = f"dividends:{url}"
    html = await _fetch(url, cache_key=cache_key, ttl=TTL_DIVIDENDS)
    dividends = parser.parse_dividends_table(html)
    return _fmt(dividends[:limit])


@mcp.tool()
async def get_dividend_history(ticker: str) -> str:
    """Get complete dividend payment history for a specific stock.

    Args:
        ticker: Stock ticker (e.g. "SBER", "LKOH", "MTSS")
    """
    url = f"/dividends/index/id/{ticker}/"
    cache_key = f"div_history:{ticker}"
    html = await _fetch(url, cache_key=cache_key, ttl=TTL_HISTORY)
    history = parser.parse_dividend_history(html)
    return _fmt(history)


# ── Entry point ──────────────────────────────────────────────────────────────


def main():
    mcp.run()


if __name__ == "__main__":
    main()
