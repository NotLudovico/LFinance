import sqlite3
from typing import Optional, Tuple


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def open_db(path: str = "database.db") -> sqlite3.Connection:
    """
    Open a SQLite connection with safe defaults for bulk loads & integrity.
    Call this in your scrapers instead of sqlite3.connect().
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON;")  # ALWAYS ON
    return conn


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------
DDL = r"""
PRAGMA foreign_keys = ON;

-- One row per ETF (static facts)
CREATE TABLE IF NOT EXISTS etfs (
    isin            TEXT PRIMARY KEY
                        CHECK (length(isin) = 12 AND isin = UPPER(isin)),
    issuer          TEXT NOT NULL,
    name            TEXT,
    ticker          TEXT,
    ter             REAL,
    nav             REAL,
    size            REAL,
    currency        TEXT CHECK (currency IS NULL OR
                                (length(currency) = 3 AND currency = UPPER(currency))),
    asset_class     TEXT,
    sub_asset_class TEXT,
    region          TEXT,
    use_of_profits  TEXT CHECK (use_of_profits IN ('acc','dist') OR use_of_profits IS NULL),
    replication     TEXT,
    domicile        TEXT CHECK (domicile IS NULL OR
                                (length(domicile) = 3 AND domicile = UPPER(domicile))),
    inception_date  TEXT,  -- normalized 'YYYY-MM-DD' by scrapers if possible
    url             TEXT
);

-- One row per unique security/holding
CREATE TABLE IF NOT EXISTS securities (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    isin     TEXT UNIQUE,  -- may be NULL for cash or baskets
    name     TEXT NOT NULL,
    sector   TEXT,
    country  TEXT CHECK (country IS NULL OR
                         (length(country) = 3 AND country = UPPER(country))),
    currency TEXT CHECK (currency IS NULL OR
                         (length(currency) = 3 AND currency = UPPER(currency))),
    CHECK (isin IS NULL OR (length(isin) = 12 AND isin = UPPER(isin)))
);

-- Deduplicate NULL-ISIN securities by (lower(name), currency, country)
CREATE UNIQUE INDEX IF NOT EXISTS ux_securities_null_isin_name_cc
ON securities(lower(name), ifnull(currency,''), ifnull(country,''))
WHERE isin IS NULL;

-- Latest-only ETF -> Security mapping (weight only)
CREATE TABLE IF NOT EXISTS etf_holdings (
    etf_isin    TEXT    NOT NULL REFERENCES etfs(isin) ON DELETE CASCADE,
    security_id INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
    weight      REAL    NOT NULL CHECK (weight >= 0.0 AND weight <= 100.0),
    PRIMARY KEY (etf_isin, security_id)
);

-- App-friendly read layer
CREATE VIEW IF NOT EXISTS v_holdings AS
SELECT
    eh.etf_isin,
    s.isin     AS holding_isin,
    s.name     AS holding_name,
    s.sector,
    s.country,
    s.currency,
    eh.weight
FROM etf_holdings AS eh
JOIN securities   AS s  ON s.id = eh.security_id;

-- Indexing strategy
CREATE INDEX IF NOT EXISTS  idx_securities_sector   ON securities(sector);
CREATE INDEX IF NOT EXISTS  idx_securities_country  ON securities(country);
CREATE INDEX IF NOT EXISTS  idx_holdings_etf        ON etf_holdings(etf_isin);     -- fast "holdings of ETF X"
CREATE INDEX IF NOT EXISTS  idx_holdings_security   ON etf_holdings(security_id);  -- fast "ETFs holding Y"
"""


def setup_database(conn: sqlite3.Connection, *, drop_and_recreate: bool = True) -> None:
    """
    Create the normalized schema
    """
    with conn:
        conn.executescript(DDL)


# ---------------------------------------------------------------------------
# Upsert helpers for scrapers (clean, simple, transaction-friendly)
# ---------------------------------------------------------------------------
def upsert_etf(conn: sqlite3.Connection, etf_tuple: Tuple) -> None:
    """
    Insert or replace an ETF. etf_tuple must match the order produced by utilities.common.ETF.to_db_tuple().
    """
    sql = """
    INSERT OR REPLACE INTO etfs
    (isin, issuer, name, ticker, ter, nav, size, currency, asset_class,
     sub_asset_class, region, use_of_profits, replication, domicile,
     inception_date, url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    conn.execute(sql, etf_tuple)


def _select_security_id(
    conn: sqlite3.Connection,
    *,
    isin: Optional[str],
    name: str,
    currency: Optional[str],
    country: Optional[str],
) -> Optional[int]:
    cur = conn.cursor()
    if isin:
        cur.execute("SELECT id FROM securities WHERE isin = ?", (isin,))
    else:
        cur.execute(
            """
            SELECT id FROM securities
            WHERE isin IS NULL
              AND lower(name) = lower(?)
              AND ifnull(currency,'') = ifnull(?,'')
              AND ifnull(country,'')  = ifnull(?,'')
            """,
            (name, currency, country),
        )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_security(
    conn: sqlite3.Connection,
    *,
    isin: Optional[str],
    name: str,
    sector: Optional[str],
    country: Optional[str],
    currency: Optional[str],
) -> int:
    """
    Upsert a security and return its id.
    - If ISIN is present: de-duplicate by ISIN (global).
    - If ISIN is NULL: de-duplicate by (lower(name), currency, country).
    - Non-null incoming attributes overwrite NULLs, but never the other way round.
    """
    assert name, "security.name is required"

    existing_id = _select_security_id(
        conn, isin=isin, name=name, currency=currency, country=country
    )
    if existing_id is None:
        cur = conn.execute(
            "INSERT INTO securities(isin, name, sector, country, currency) VALUES (?, ?, ?, ?, ?)",
            (isin, name, sector, country, currency),
        )
        return cur.lastrowid

    # Update only with non-null fields
    conn.execute(
        """
        UPDATE securities
           SET name     = COALESCE(?, name),
               sector   = COALESCE(?, sector),
               country  = COALESCE(?, country),
               currency = COALESCE(?, currency)
         WHERE id = ?;
        """,
        (name, sector, country, currency, existing_id),
    )
    return existing_id


def upsert_holding(
    conn: sqlite3.Connection, *, etf_isin: str, security_id: int, weight: float
) -> None:
    """
    Insert the ETF -> security weight. Re-scrapes overwrite the pair (latest-only).
    """
    conn.execute(
        """
        INSERT INTO etf_holdings(etf_isin, security_id, weight)
        VALUES (?, ?, ?)
        ON CONFLICT(etf_isin, security_id) DO UPDATE SET
            weight = excluded.weight;
        """,
        (etf_isin, security_id, float(weight)),
    )
