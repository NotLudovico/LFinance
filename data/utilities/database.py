import sqlite3


def setup_database(conn: sqlite3.Connection):
    """Create database tables if they don't already exist."""
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etfs (
                isin TEXT PRIMARY KEY,
                issuer TEXT,
                name TEXT,
                ticker TEXT,
                ter REAL,
                nav REAL,
                size REAL,
                currency TEXT,
                asset_class TEXT,
                sub_asset_class TEXT,
                region TEXT,
                use_of_profits TEXT,
                replication TEXT,
                domicile TEXT,
                inception_date TEXT,
                url TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etf_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                etf_isin TEXT,
                holding_isin TEXT,
                holding_name TEXT,
                weight REAL,
                sector TEXT,
                country TEXT,
                currency TEXT,
                FOREIGN KEY (etf_isin) REFERENCES etfs (isin)
            )
        """)
    print("Database tables ensured to exist.")
