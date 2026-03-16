"""HTML and JS parsing for smart-lab.ru pages."""

from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup, Tag


def _clean(text: str) -> str:
    """Strip whitespace and normalize."""
    return re.sub(r"\s+", " ", text).strip()


def _parse_number(text: str) -> float | None:
    """Parse a number from text like '401.7%', '12 356', '59.84', '-'."""
    text = _clean(text).replace("\xa0", "").replace(" ", "")
    text = text.rstrip("%")
    text = text.replace(",", ".")
    if not text or text == "-" or text == "\u2014":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(text: str) -> str | None:
    """Parse date from text like '10.04.26' or '21.10.2016', return as-is."""
    text = _clean(text)
    if not text or text == "-" or text == "\u2014" or text == "\xa0":
        return None
    return text


def _extract_link(cell: Tag) -> str | None:
    """Extract href from first <a> in cell."""
    a = cell.find("a")
    if a and a.get("href"):
        return a["href"]
    return None


def _extract_secid_from_link(href: str) -> str | None:
    """Extract secid from URL like /q/bonds/RU000A10EJQ7/."""
    m = re.search(r"/q/[^/]+/([A-Z0-9]+)/?$", href)
    return m.group(1) if m else None


def _get_table(html: str, index: int = 0) -> Tag | None:
    """Get the Nth <table> from HTML."""
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    if index < len(tables):
        return tables[index]
    return None


def _table_rows(table: Tag) -> list[list[Tag]]:
    """Extract all rows as lists of cells (td/th)."""
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if cells:
            rows.append(cells)
    return rows


# ── Bonds ────────────────────────────────────────────────────────────────────

# Header text → output field name mapping.
# Different bond types (corporate, OFZ, subfederal) have different column sets.
_BOND_HEADER_MAP: dict[str, tuple[str, str]] = {
    # header_text_prefix: (field_name, type: "num" | "date" | "text")
    "Имя": ("name", "text"),
    "Лет до": ("years_to_maturity", "num"),
    "Доходн": ("yield_pct", "num"),
    "Год.куп": ("annual_coupon_yield_pct", "num"),
    "Куп.дох": ("last_coupon_yield_pct", "num"),
    "Рейтинг": ("rating", "text"),
    "Объем": ("volume_mln_rub", "num"),
    "Купон": ("coupon_rub", "num"),
    "Частота": ("frequency", "num"),
    "НКД": ("nkd_rub", "num"),
    "Дюр-я": ("duration_years", "num"),
    "Цена": ("price", "num"),
    "Дата купона": ("next_coupon_date", "date"),
    "Размещение": ("placement_date", "date"),
    "Погашение": ("maturity_date", "date"),
    "Оферта": ("offer_date", "date"),
}


def _detect_bond_columns(header_cells: list[Tag]) -> dict[int, tuple[str, str]]:
    """Map column index → (field_name, type) by matching header text."""
    col_map: dict[int, tuple[str, str]] = {}
    for i, cell in enumerate(header_cells):
        text = _clean(cell.get_text())
        if not text or text == "№":
            continue
        for prefix, (field, ftype) in _BOND_HEADER_MAP.items():
            if text.startswith(prefix):
                col_map[i] = (field, ftype)
                break
    return col_map


def parse_bonds_table(html: str) -> list[dict[str, Any]]:
    """Parse the bonds listing table. Adapts to different column layouts
    (corporate, OFZ, subfederal, currency, other)."""
    table = _get_table(html)
    if not table:
        return []

    rows = _table_rows(table)
    if len(rows) < 2:
        return []

    # Detect column mapping from header row
    col_map = _detect_bond_columns(rows[0])
    if not col_map:
        return []

    # Find name column index for secid extraction
    name_idx = next((i for i, (f, _) in col_map.items() if f == "name"), None)

    results = []
    for row in rows[1:]:
        bond: dict[str, Any] = {}
        for i, (field, ftype) in col_map.items():
            if i >= len(row):
                continue
            cell = row[i]
            if ftype == "num":
                bond[field] = _parse_number(cell.get_text())
            elif ftype == "date":
                bond[field] = _parse_date(cell.get_text())
            else:  # text
                bond[field] = _clean(cell.get_text()) or None

        # Extract secid from name link
        if name_idx is not None and name_idx < len(row):
            link = _extract_link(row[name_idx])
            bond["secid"] = _extract_secid_from_link(link) if link else None

        if bond.get("name"):
            results.append(bond)

    return results


def parse_bonds_chart_data(html: str) -> dict[str, list[dict[str, Any]]]:
    """Parse aBondsChartData JS variable.

    Returns dict with keys:
      - wc: with coupon yield, duration in years
      - woc: without coupon yield, duration in years
      - wc_ft: with coupon yield, full term to maturity
      - woc_ft: without coupon yield, full term to maturity

    Each value is a list of {secid, name, rating, duration, yield_pct}.
    """
    m = re.search(r"aBondsChartData\s*=\s*(\{.*?\});", html, re.DOTALL)
    if not m:
        return {}

    try:
        raw = json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}

    result = {}
    for key in ("wc", "woc", "wc_ft", "woc_ft"):
        items = raw.get(key, [])
        result[key] = [
            {
                "secid": item.get("secid"),
                "name": item.get("name"),
                "rating": item.get("rating"),
                "duration": item.get("x"),
                "yield_pct": item.get("y"),
            }
            for item in items
        ]

    return result


def parse_bond_detail(html: str) -> dict[str, Any]:
    """Parse bond detail page — coupon schedule table and page metadata."""
    result: dict[str, Any] = {}

    # Parse coupon schedule table
    # Columns: №, Дата купона, Купон руб, Дох. купона, % от номинала, % от рынка
    table = _get_table(html)
    if table:
        rows = _table_rows(table)
        coupons = []
        for row in rows[1:]:
            if len(row) < 6:
                continue
            coupons.append({
                "date": _parse_date(row[1].get_text()),
                "coupon_rub": _parse_number(row[2].get_text()),
                "coupon_yield_pct": _parse_number(row[3].get_text()),
                "pct_of_nominal": _parse_number(row[4].get_text()),
                "pct_of_market": _parse_number(row[5].get_text()),
            })
        result["coupons"] = coupons

    # Extract key-value pairs from the page (dl/dt/dd or similar structures)
    soup = BeautifulSoup(html, "lxml")

    # Try to find bond info in the page title / header
    h1 = soup.find("h1")
    if h1:
        result["title"] = _clean(h1.get_text())

    return result


# ── Shares ───────────────────────────────────────────────────────────────────


def parse_shares_table(html: str) -> list[dict[str, Any]]:
    """Parse the shares listing table.

    Columns: №, Название, Тикер, (3 empty/icon), Цена посл, Изм%,
    Объем млн руб, (time), 1 нед%, 1 м%, ytd%, 12м%,
    Капит-я млрд руб, Капит-я млрд $, Изм Объема, Изм поз, (+), (+)
    """
    table = _get_table(html)
    if not table:
        return []

    rows = _table_rows(table)
    if len(rows) < 2:
        return []

    results = []
    for row in rows[1:]:  # skip header
        if len(row) < 16:
            continue

        ticker = _clean(row[2].get_text())
        if not ticker or ticker == "IMOEX":
            continue  # skip index row

        results.append({
            "name": _clean(row[1].get_text()),
            "ticker": ticker,
            "price": _parse_number(row[6].get_text()),
            "change_1d_pct": _parse_number(row[7].get_text()),
            "volume_mln_rub": _parse_number(row[8].get_text()),
            "change_1w_pct": _parse_number(row[10].get_text()),
            "change_1m_pct": _parse_number(row[11].get_text()),
            "change_ytd_pct": _parse_number(row[12].get_text()),
            "change_12m_pct": _parse_number(row[13].get_text()),
            "market_cap_bln_rub": _parse_number(row[14].get_text()),
            "market_cap_bln_usd": _parse_number(row[15].get_text()),
        })

    return results


def parse_shares_fundamental_table(html: str) -> list[dict[str, Any]]:
    """Parse the shares_fundamental table.

    Columns: №, Название, Тикер, (2 empty), Капит-я млрд руб, EV млрд руб,
    Выручка, Чистая прибыль, ДД ао%, ДД ап%, ДД/ЧП%, P/E, P/S, P/B,
    EV/EBITDA, Рентаб. EBITDA, долг/EBITDA, отчет
    """
    table = _get_table(html)
    if not table:
        return []

    rows = _table_rows(table)
    if len(rows) < 2:
        return []

    results = []
    for row in rows[1:]:
        if len(row) < 18:
            continue

        ticker = _clean(row[2].get_text())
        if not ticker:
            continue

        results.append({
            "name": _clean(row[1].get_text()),
            "ticker": ticker,
            "market_cap_bln_rub": _parse_number(row[5].get_text()),
            "ev_bln_rub": _parse_number(row[6].get_text()),
            "revenue_bln_rub": _parse_number(row[7].get_text()),
            "net_income_bln_rub": _parse_number(row[8].get_text()),
            "div_yield_common_pct": _parse_number(row[9].get_text()),
            "div_yield_preferred_pct": _parse_number(row[10].get_text()),
            "payout_ratio_pct": _parse_number(row[11].get_text()),
            "pe": _parse_number(row[12].get_text()),
            "ps": _parse_number(row[13].get_text()),
            "pb": _parse_number(row[14].get_text()),
            "ev_ebitda": _parse_number(row[15].get_text()),
            "ebitda_margin_pct": _parse_number(row[16].get_text()),
            "debt_ebitda": _parse_number(row[17].get_text()),
        })

    return results


# ── Dividends ────────────────────────────────────────────────────────────────


def parse_dividends_table(html: str) -> list[dict[str, Any]]:
    """Parse the dividend calendar table (first table on /dividends/).

    Columns: Название, Тикер, Период, Дивиденд руб, Див. Дох., СД,
    Купить До, Дата закрытия реестра, Выплата До, Цена акции, (empty)
    """
    table = _get_table(html)
    if not table:
        return []

    rows = _table_rows(table)
    if len(rows) < 2:
        return []

    results = []
    for row in rows[1:]:
        if len(row) < 10:
            continue

        results.append({
            "name": _clean(row[0].get_text()),
            "ticker": _clean(row[1].get_text()),
            "period": _clean(row[2].get_text()),
            "dividend_rub": _parse_number(row[3].get_text()),
            "yield_pct": _parse_number(row[4].get_text()),
            "board_approved": bool(_clean(row[5].get_text())),
            "last_buy_date": _parse_date(row[6].get_text()),
            "close_date": _parse_date(row[7].get_text()),
            "payment_date": _parse_date(row[8].get_text()),
            "price": _parse_number(row[9].get_text()),
        })

    return results


def parse_dividend_history(html: str) -> list[dict[str, Any]]:
    """Parse dividend history for a specific ticker.

    Uses the same table structure as the dividend calendar.
    """
    # The dividend history page uses the same table format as the calendar
    return parse_dividends_table(html)


def parse_pagination(html: str) -> int:
    """Extract total number of pages from pagination links."""
    soup = BeautifulSoup(html, "lxml")
    # Smart-lab pagination: <a class="page" href="...pageN/">N</a>
    pages = soup.select("a.page")
    max_page = 1
    for link in pages:
        text = _clean(link.get_text())
        try:
            page_num = int(text)
            max_page = max(max_page, page_num)
        except ValueError:
            continue
    return max_page
