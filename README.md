# mcp-smartlab

[![Tests](https://github.com/Sicness/mcp-smartlab/actions/workflows/tests.yml/badge.svg)](https://github.com/Sicness/mcp-smartlab/actions/workflows/tests.yml)

MCP server for scraping financial data from [smart-lab.ru](https://smart-lab.ru) — a Russian financial portal.

Provides bond screening with filters (rating, duration, sector, floater/fixed, amortization), stock screening with market data and fundamentals, and dividend calendar.

## Installation

```bash
git clone <repo-url>
cd mcp-smartlab
python -m venv .venv
.venv/bin/pip install -e .
```

## Usage with Claude Code

Add to your project's `.mcp.json`:

```json
{
  "mcpServers": {
    "smartlab": {
      "command": "/path/to/mcp-smartlab/.venv/bin/mcp-smartlab"
    }
  }
}
```

No environment variables or authentication required — smart-lab.ru is a public website.

## Tools

### Bonds (6 tools)

- `search_bonds` — Main bond screener with filters (type, rating, duration, sector, floater, amortization, perpetual)
- `get_top_yield_bonds` — Quick top-yield search with conservative defaults (BBB+, 5yr duration)
- `get_bond_chart_data` — Lightweight yield/duration/rating data for all matching bonds
- `get_bond_details` — Detailed info for a specific bond (coupon schedule)
- `compare_bonds` — Compare multiple bonds side by side
- `get_bond_sectors` — List available sector filters

### Shares (2 tools)

- `search_shares` — Stock screener (price, volume, changes, market cap)
- `get_shares_fundamental` — Fundamental data (P/E, P/S, P/B, EV/EBITDA, div yield, margins)

### Dividends (2 tools)

- `get_upcoming_dividends` — Upcoming dividend payments calendar
- `get_dividend_history` — Historical dividends for a specific ticker

## Data Source

All data is scraped from public HTML pages on smart-lab.ru. No API key required.
In-memory caching: 1h for screener data, 4h for fundamentals, 24h for dividend history.
