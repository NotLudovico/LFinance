# LFinance — ETF Holdings Scraper

LFinance is a small set of scrapers that collect **ETF facts and full holdings** from multiple issuers (Amundi, iShares, SPDR) and store them in a **clean, normalized SQLite database**.

---

## Folder structure

```
.
├── bonds.py
├── etfs.py
├── pyproject.toml
├── uv.lock
├── README.md
├── data/
│   ├── amundi.py
│   ├── ishares.py
│   ├── README.md
│   └── spdr.py
└── utilities/
    ├── common.py
    ├── country.py
    ├── database.py
    └── translate.py
```

---

## Quick start
Using [uv](https://github.com/astral-sh/uv) package manager:
**Install**
```bash
# With uv (uses pyproject/uv.lock)
uv sync
```

**Run a scraper**

> The SQLite file is named `database.db` and is written to your **current working directory**.

```bash
# Recommended: run from the data/ folder so the DB lives there
cd data

# Amundi
uv run amundi.py

# iShares
uv run ishares.py

# SPDR
uv run spdr.py
```

Re-running a scraper refreshes the ETFs it handles (old holdings for those ISINs are cleared and reinserted).

---

## Using the notebooks (marimo)

`etfs.py` and `bonds.py` are **[marimo](https://marimo.io) notebooks** (Python scripts that open as interactive notebooks). Use either approach:

### Option A — with `uvx` (no install)
From the project root:
```bash
# Open interactive notebook UI
uvx marimo edit etfs.py
uvx marimo edit bonds.py
```
Run headless (execute cells without a UI):
```bash
uvx marimo run etfs.py
uvx marimo run bonds.py
```
Export to HTML (optional):
```bash
uvx marimo export etfs.py --to html -o etfs.html
```



## Data model (SQLite)

Created automatically on first run.

- **etfs** — one row per ETF (issuer, name, ticker, TER, AUM, currency, asset/sub-asset class, region, distribution policy, replication, domicile, inception date, URL).
- **securities** — unique holdings (by ISIN; for no-ISIN items like CASH, de-duped by `name+currency+country`).
- **etf_holdings** — latest weight for `(etf_isin, security_id)`.
- **v_holdings** — view joining holdings with security attributes.

### Example: top 10 holdings of an ETF
```sql
SELECT holding_name, holding_isin, weight, sector, country, currency
FROM v_holdings
WHERE etf_isin = 'IE00B4L5Y983'
ORDER BY weight DESC
LIMIT 10;
```

---

## Notes

- Shared models normalize dates, currencies, sectors, countries, and clamp weights to 0–100.
- Country names are mapped to ISO-3 codes; common Italian labels (e.g., sectors, “Acc/Dist”) are translated to normalized English.
- The DB helpers handle idempotent upserts and de-duplicate no-ISIN securities.

---

## License
MIT

