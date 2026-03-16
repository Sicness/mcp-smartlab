"""Tests for mcp_smartlab.parser."""

from __future__ import annotations

import pytest

from mcp_smartlab.parser import (
    _detect_bond_columns,
    _extract_secid_from_link,
    _parse_date,
    _parse_number,
    parse_bond_detail,
    parse_bonds_chart_data,
    parse_bonds_table,
    parse_dividends_table,
    parse_pagination,
    parse_shares_fundamental_table,
    parse_shares_table,
)

from .conftest import load_fixture


# ── _parse_number ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text, expected",
    [
        ("59.84", 59.84),
        ("401.7%", 401.7),
        ("12 356", 12356.0),
        ("12\xa0356", 12356.0),
        ("59,84", 59.84),
        ("-3.5", -3.5),
        ("-", None),
        ("\u2014", None),
        ("", None),
        ("abc", None),
    ],
)
def test_parse_number(text, expected):
    assert _parse_number(text) == expected


# ── _parse_date ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text, expected",
    [
        ("10.04.26", "10.04.26"),
        ("21.10.2016", "21.10.2016"),
        ("-", None),
        ("\u2014", None),
        ("", None),
        ("\xa0", None),
    ],
)
def test_parse_date(text, expected):
    assert _parse_date(text) == expected


# ── _extract_secid_from_link ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "href, expected",
    [
        ("/q/bonds/RU000A10EJQ7/", "RU000A10EJQ7"),
        ("/q/ofz/SU26238RMFS4/", "SU26238RMFS4"),
        ("/q/bonds/RU000A10EJQ7", "RU000A10EJQ7"),
        ("/some/other/path/", None),
    ],
)
def test_extract_secid_from_link(href, expected):
    assert _extract_secid_from_link(href) == expected


# ── parse_bonds_table ────────────────────────────────────────────────────────


def test_parse_bonds_basic():
    html = load_fixture("bonds_corporate.html")
    bonds = parse_bonds_table(html)
    assert len(bonds) == 3

    b0 = bonds[0]
    assert b0["name"] == "Роснефть 5"
    assert b0["secid"] == "RU000A106540"
    assert b0["yield_pct"] == 18.34
    assert b0["rating"] == "AAA"
    assert b0["duration_years"] == 1.8
    assert b0["price"] == 98.5
    assert b0["maturity_date"] == "01.07.2028"


def test_parse_bonds_empty():
    html = load_fixture("bonds_empty.html")
    assert parse_bonds_table(html) == []


def test_parse_bonds_header_only():
    html = "<html><body><table><tr><th>Имя</th><th>Доходн.</th></tr></table></body></html>"
    assert parse_bonds_table(html) == []


def test_parse_bonds_row_shorter_than_col_map():
    html = """<html><body><table>
    <tr><th>Имя</th><th>Доходн.</th><th>Рейтинг</th><th>Дюр-я</th></tr>
    <tr><td><a href="/q/bonds/XX/">Short</a></td><td>10.5</td></tr>
    </table></body></html>"""
    bonds = parse_bonds_table(html)
    assert len(bonds) == 1
    assert bonds[0]["name"] == "Short"
    assert bonds[0]["yield_pct"] == 10.5
    assert bonds[0].get("rating") is None  # missing column


def test_parse_bonds_no_link_in_name():
    html = """<html><body><table>
    <tr><th>Имя</th><th>Доходн.</th></tr>
    <tr><td>NoLink Bond</td><td>10.0</td></tr>
    </table></body></html>"""
    bonds = parse_bonds_table(html)
    assert len(bonds) == 1
    assert bonds[0]["secid"] is None


def test_parse_bonds_ofz():
    html = load_fixture("bonds_ofz.html")
    bonds = parse_bonds_table(html)
    assert len(bonds) == 1
    assert bonds[0]["name"] == "ОФЗ 26238"
    assert bonds[0]["secid"] == "SU26238RMFS4"
    assert "rating" not in bonds[0]  # OFZ has no rating column


def test_parse_bonds_empty_name_skipped():
    html = """<html><body><table>
    <tr><th>Имя</th><th>Доходн.</th></tr>
    <tr><td></td><td>10.0</td></tr>
    <tr><td>Valid</td><td>11.0</td></tr>
    </table></body></html>"""
    bonds = parse_bonds_table(html)
    assert len(bonds) == 1
    assert bonds[0]["name"] == "Valid"


# ── _detect_bond_columns ────────────────────────────────────────────────────


def test_detect_bond_columns_corporate():
    from bs4 import BeautifulSoup

    html = "<tr><th>№</th><th>Имя</th><th>Доходн. к погаш.</th><th>Рейтинг</th></tr>"
    soup = BeautifulSoup(html, "lxml")
    cells = soup.find("tr").find_all("th")
    col_map = _detect_bond_columns(cells)
    # "№" should be skipped
    assert 0 not in col_map
    assert col_map[1] == ("name", "text")
    assert col_map[2] == ("yield_pct", "num")
    assert col_map[3] == ("rating", "text")


def test_detect_bond_columns_prefix_matching():
    from bs4 import BeautifulSoup

    html = "<tr><th>Доходность к погашению</th></tr>"
    soup = BeautifulSoup(html, "lxml")
    cells = soup.find("tr").find_all("th")
    col_map = _detect_bond_columns(cells)
    assert col_map[0] == ("yield_pct", "num")


# ── parse_bonds_chart_data ───────────────────────────────────────────────────


def test_parse_bonds_chart_data_valid():
    html = load_fixture("bonds_chart_data.html")
    data = parse_bonds_chart_data(html)
    assert "wc" in data
    assert "woc" in data
    assert "wc_ft" in data
    assert "woc_ft" in data
    assert len(data["wc"]) == 3
    assert data["wc"][0]["secid"] == "RU000A106540"
    assert data["wc"][0]["yield_pct"] == 18.34
    assert data["wc"][0]["duration"] == 1.8


def test_parse_bonds_chart_data_missing():
    html = "<html><body>no chart data here</body></html>"
    assert parse_bonds_chart_data(html) == {}


def test_parse_bonds_chart_data_invalid_json():
    html = "<html><body><script>aBondsChartData = {invalid json};</script></body></html>"
    assert parse_bonds_chart_data(html) == {}


# ── parse_bond_detail ────────────────────────────────────────────────────────


def test_parse_bond_detail_with_coupons():
    html = load_fixture("bond_detail.html")
    detail = parse_bond_detail(html)
    assert detail["title"] == "Роснефть БО-05 (RU000A106540)"
    assert len(detail["coupons"]) == 3
    c = detail["coupons"][0]
    assert c["date"] == "10.04.2026"
    assert c["coupon_rub"] == 42.38
    assert c["coupon_yield_pct"] == 8.48


def test_parse_bond_detail_no_table():
    html = "<html><body><h1>Some Bond</h1><p>No table here</p></body></html>"
    detail = parse_bond_detail(html)
    assert detail["title"] == "Some Bond"
    assert "coupons" not in detail


def test_parse_bond_detail_short_rows_skipped():
    html = """<html><body><table>
    <tr><th>№</th><th>Дата</th><th>Купон</th><th>Дох</th><th>%ном</th><th>%рын</th></tr>
    <tr><td>1</td><td>10.04.26</td><td>42</td></tr>
    <tr><td>2</td><td>10.10.26</td><td>42</td><td>8.0</td><td>4.0</td><td>4.1</td></tr>
    </table></body></html>"""
    detail = parse_bond_detail(html)
    assert len(detail["coupons"]) == 1


# ── parse_shares_table ───────────────────────────────────────────────────────


def test_parse_shares_basic():
    html = load_fixture("shares.html")
    shares = parse_shares_table(html)
    assert len(shares) == 2  # IMOEX skipped
    assert shares[0]["ticker"] == "SBER"
    assert shares[0]["price"] == 295.5
    assert shares[0]["change_1d_pct"] == 1.25
    assert shares[0]["volume_mln_rub"] == 12356.0
    assert shares[1]["ticker"] == "LKOH"


def test_parse_shares_imoex_skipped():
    html = load_fixture("shares.html")
    shares = parse_shares_table(html)
    tickers = [s["ticker"] for s in shares]
    assert "IMOEX" not in tickers


def test_parse_shares_short_row_skipped():
    html = """<html><body><table>
    <tr><th>№</th><th>Название</th><th>Тикер</th></tr>
    <tr><td>1</td><td>Short</td><td>SH</td></tr>
    </table></body></html>"""
    assert parse_shares_table(html) == []


def test_parse_shares_empty():
    assert parse_shares_table("<html><body></body></html>") == []


# ── parse_shares_fundamental_table ───────────────────────────────────────────


def test_parse_shares_fundamental_basic():
    html = load_fixture("shares_fundamental.html")
    shares = parse_shares_fundamental_table(html)
    assert len(shares) == 1
    s = shares[0]
    assert s["ticker"] == "SBER"
    assert s["market_cap_bln_rub"] == 6789.0
    assert s["pe"] == 4.5
    assert s["ps"] == 1.9
    assert s["pb"] == 1.1
    assert s["div_yield_common_pct"] == 12.5
    assert s["ev_bln_rub"] is None  # "-" in fixture


def test_parse_shares_fundamental_short_row():
    html = """<html><body><table>
    <tr><th>№</th><th>Название</th><th>Тикер</th></tr>
    <tr><td>1</td><td>Short</td><td>SH</td></tr>
    </table></body></html>"""
    assert parse_shares_fundamental_table(html) == []


def test_parse_shares_fundamental_empty_ticker():
    html = """<html><body><table>
    <tr><th>№</th><th>Название</th><th>Тикер</th><th></th><th></th>
    <th>К</th><th>EV</th><th>В</th><th>ЧП</th><th>ДД</th><th>ДДп</th><th>ДД/ЧП</th>
    <th>P/E</th><th>P/S</th><th>P/B</th><th>EV/E</th><th>Р</th><th>Д</th></tr>
    <tr><td>1</td><td>Name</td><td></td><td></td><td></td>
    <td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td><td>7</td>
    <td>8</td><td>9</td><td>10</td><td>11</td><td>12</td><td>13</td></tr>
    </table></body></html>"""
    assert parse_shares_fundamental_table(html) == []


# ── parse_dividends_table ────────────────────────────────────────────────────


def test_parse_dividends_basic():
    html = load_fixture("dividends.html")
    divs = parse_dividends_table(html)
    assert len(divs) == 2

    d0 = divs[0]
    assert d0["name"] == "Сбербанк"
    assert d0["ticker"] == "SBER"
    assert d0["dividend_rub"] == 33.3
    assert d0["yield_pct"] == 11.27
    assert d0["board_approved"] is True
    assert d0["close_date"] == "10.07.2026"
    assert d0["price"] == 295.5


def test_parse_dividends_board_approved_false():
    html = load_fixture("dividends.html")
    divs = parse_dividends_table(html)
    assert divs[1]["board_approved"] is False  # Лукойл has empty СД


def test_parse_dividends_short_row_skipped():
    html = """<html><body><table>
    <tr><th>Название</th><th>Тикер</th></tr>
    <tr><td>X</td><td>Y</td></tr>
    </table></body></html>"""
    assert parse_dividends_table(html) == []


# ── parse_pagination ─────────────────────────────────────────────────────────


def test_parse_pagination_multiple_pages():
    html = load_fixture("pagination.html")
    assert parse_pagination(html) == 5


def test_parse_pagination_no_links():
    html = "<html><body><p>no pages</p></body></html>"
    assert parse_pagination(html) == 1


def test_parse_pagination_non_numeric():
    html = '<html><body><a class="page">next</a><a class="page">3</a></body></html>'
    assert parse_pagination(html) == 3
