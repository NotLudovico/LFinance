import asyncio
import json
import re
import sqlite3
from typing import Any, Coroutine, Dict, List

import aiohttp
from tqdm import tqdm  # Optional: for a nice progress bar

# --- Configuration ---
DB_NAME = "database_gemini.db"
CONCURRENT_REQUESTS = 10  # Limit the number of concurrent requests

# --- Constants & Helpers ---
PROFITS_CONV = {
    "Ad Accumulazione": "ACC",
    "Distribuzione": "DIST",
    "Nessun rendimento": None,
}


def clean_product(prod: Dict[str, Any]) -> Dict[str, Any]:
    """Cleans the raw product data from the initial JSON list."""
    # This function remains unchanged
    return {
        "product_type": prod["productView"][
            1
        ],  # First element seems to be always "all"
        "issuer": "ishares",
        "pid": prod["portfolioId"],
        "name": prod["fundName"],
        "isin": prod["isin"],
        "ticker": prod["localExchangeTicker"],
        "nav": prod["navAmount"]["r"],
        "asset_class": prod["aladdinAssetClass"],
        "sub_asset_class": prod["aladdinSubAssetClass"],
        "market_type": prod["aladdinMarketType"],
        "region": prod["aladdinRegion"],
        "url": prod["productPageUrl"],
        "domicile": prod["domicile"],
        "inception_date": prod["inceptionDate"]["d"],
        "use_of_profits": PROFITS_CONV.get(prod["useOfProfits"]),
        "size": prod["totalNetAssets"]["r"],
        "currency": prod["seriesBaseCurrencyCode"],
        "ter": prod["ter_ocf"]["r"] if prod["ter_ocf"] != "-" else None,
    }


def parse_ishares_holding(holding_data: list) -> Dict[str, Any]:
    """
    Parses a holding list for core information.
    This function remains unchanged.
    """
    parsed = {}
    strings = [item for item in holding_data if isinstance(item, str)]
    dicts = [item for item in holding_data if isinstance(item, dict)]

    percent_dicts = [d for d in dicts if "%" in d.get("display", "")]
    if percent_dicts:
        percent_dicts.sort(
            key=lambda d: len(str(d.get("raw", "")).split(".")[-1]), reverse=True
        )
        parsed["weight"] = percent_dicts[0].get("raw")

    isin_regex = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")
    for s in strings:
        if isin_regex.match(s):
            parsed["isin"] = s
            strings.remove(s)
            break

    if strings and len(strings[-1]) == 3 and strings[-1].isupper():
        parsed["currency"] = strings.pop(-1)

    if len(strings) > 1:
        # The first element is often the exchange, which can be '-'
        exchange = strings.pop(-1)
        if exchange != "-":
            parsed["exchange"] = exchange
        parsed["country"] = strings.pop(-1)

    if len(strings) > 0:
        parsed["ticker"] = strings[0]
    if len(strings) > 1:
        parsed["name"] = strings[1]

    return parsed


def setup_database(conn: sqlite3.Connection):
    """Sets up the database tables."""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS etfs")
    cursor.execute("DROP TABLE IF EXISTS etf_holdings")
    cursor.execute(
        """
        CREATE TABLE etfs (
            isin TEXT PRIMARY KEY, product_type TEXT, issuer TEXT, name TEXT,
            ticker TEXT, nav REAL, asset_class TEXT, sub_asset_class TEXT,
            market_type TEXT, region TEXT, url TEXT, domicile TEXT,
            inception_date TEXT, use_of_profits TEXT, size REAL,
            currency TEXT, ter REAL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE etf_holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, etf_isin TEXT, holding_isin TEXT,
            holding_name TEXT, weight REAL, country TEXT, currency TEXT
        )
        """
    )
    conn.commit()


async def get_products_list(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fetches the main list of all products asynchronously."""
    url = "https://www.ishares.com/it/investitore-privato/it/product-screener/product-screener-v3.1.jsn"
    params = {
        "dcrPath": "/templatedata/config/product-screener-v3/data/it/it/product-screener/ishares-product-screener-backend-config",
        "siteEntryPassthrough": "true",
    }
    headers = {
        "accept": "application/json, text/plain, */*",
    }
    print("Fetching product list...")
    async with session.get(url, params=params, headers=headers) as response:
        response.raise_for_status()
        data = await response.json()
        return [clean_product(v) for _, v in data.items()]


async def fetch_holding(
    session: aiohttp.ClientSession, prod: Dict[str, Any], semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Fetches and processes holdings for a single product."""
    url = f"https://www.ishares.com/it/investitore-privato/it/prodotti/{prod['pid']}/fund/1506575546154.ajax"
    params = {"tab": "all", "fileType": "json"}

    async with semaphore:
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                # The utf-8-sig encoding handles the leading BOM
                data = await response.json(encoding="utf-8-sig")

                holdings_data = [
                    parse_ishares_holding(h) for h in data.get("aaData", [])
                ]
                return {"product": prod, "holdings": holdings_data}

        except (aiohttp.ClientError, json.JSONDecodeError, asyncio.TimeoutError) as e:
            print(f"Failed to fetch holdings for {prod['name']} ({prod['isin']}): {e}")
            return {"product": prod, "holdings": []}  # Return empty list on failure


async def main():
    """Main asynchronous routine."""
    conn = sqlite3.connect(DB_NAME)
    setup_database(conn)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        products = await get_products_list(session)

        # Prepare data for batch insertion
        etfs_to_insert = []
        all_holdings_to_insert = []

        # Create a semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        # Create a list of tasks to run concurrently
        tasks: List[Coroutine] = [
            fetch_holding(session, p, semaphore) for p in products
        ]

        print(
            f"Fetching holdings for {len(products)} ETFs with {CONCURRENT_REQUESTS} concurrent workers..."
        )

        # Use tqdm for a progress bar
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            result = await future
            if not result:
                continue

            prod = result["product"]
            holdings = result["holdings"]

            # Prepare ETF data for insertion
            etf_data = prod.copy()
            del etf_data["pid"]  # Remove the temporary portfolio ID
            etfs_to_insert.append(etf_data)

            # Prepare holdings data for insertion
            for h in holdings:
                all_holdings_to_insert.append(
                    (
                        prod["isin"],
                        h.get("isin"),
                        h.get("name"),
                        h.get("weight"),
                        h.get("country"),
                        h.get("currency"),
                    )
                )

    # --- Batch insert all collected data into the database ---
    print("\nInserting data into the database...")
    cursor = conn.cursor()

    # Batch insert ETFs
    etf_cols = etfs_to_insert[0].keys()
    etf_placeholders = ", ".join("?" for _ in etf_cols)
    etf_values = [tuple(p[col] for col in etf_cols) for p in etfs_to_insert]

    cursor.executemany(
        f"INSERT INTO etfs ({', '.join(etf_cols)}) VALUES ({etf_placeholders})",
        etf_values,
    )

    # Batch insert holdings
    cursor.executemany(
        "INSERT INTO etf_holdings (etf_isin, holding_isin, holding_name, weight, country, currency) VALUES (?, ?, ?, ?, ?, ?)",
        all_holdings_to_insert,
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
