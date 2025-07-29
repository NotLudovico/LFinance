import asyncio
import json
import re
import sqlite3
from typing import Any, Coroutine, Dict, List

from utilities.country import country_to_iso3
from utilities.translate import translate
from utilities.database import setup_database

from datetime import datetime
import locale
import aiohttp
from tqdm import tqdm  # Optional: for a nice progress bar

# --- Configuration ---
DB_NAME = "database.db"
CONCURRENT_REQUESTS = 10  # Limit the number of concurrent requests

# --- Constants & Helpers ---
PROFITS_CONV = {
    "Ad Accumulazione": "acc",
    "Distribuzione": "dist",
    "Nessun rendimento": None,
}


def clean_product(prod: Dict[str, Any]) -> Dict[str, Any]:
    """Cleans the raw product data from the initial JSON list."""
    locale.setlocale(locale.LC_TIME, "it_IT.UTF-8")
    date_str = prod["inceptionDate"]["d"]
    parsed_date = datetime.strptime(date_str, "%d %b %Y")
    formatted_date = parsed_date.strftime("%d/%m/%Y")

    return {
        "issuer": "ishares",
        "pid": prod["portfolioId"],
        "name": prod["fundName"],
        "isin": prod["isin"],
        "ticker": prod["localExchangeTicker"],
        "nav": prod["navAmount"]["r"],
        "asset_class": translate(prod["aladdinAssetClass"]),
        "sub_asset_class": translate(prod["aladdinSubAssetClass"]),
        # "market_type": translate(prod["aladdinMarketType"]),
        "region": translate(prod["aladdinRegion"]),
        "url": prod["productPageUrl"],
        "domicile": country_to_iso3(prod["domicile"]),
        "inception_date": formatted_date,
        "use_of_profits": PROFITS_CONV.get(prod["useOfProfits"]),
        "replication": None,
        "size": prod["totalNetAssets"]["r"],
        "currency": prod["seriesBaseCurrencyCode"],
        "ter": prod["ter_ocf"]["r"] if prod["ter_ocf"] != "-" else None,
    }


def parse_ishares_holding(data: list) -> dict:
    """
    Parses an inconsistent list from iShares to extract key financial data.

    This function identifies data points based on their intrinsic patterns (e.g., ISIN format)
    and their position relative to other known elements (e.g., a sector is found just
    before the asset class).

    Args:
        data: A list containing the raw holding information.

    Returns:
        A dictionary containing the parsed data for the fields of interest.
    """
    result = {
        "country": None,
        "sector": None,
        "asset_class": None,
        "name": None,
        "weight": None,
        "isin": None,
        "currency": None,
    }

    # --- Extracts Name ---
    if len(data) > 1 and isinstance(data[1], str):
        result["name"] = data[1]
        if "CASH " in result["name"] or " CASH" in result["name"]:
            result["sector"] = "cash"

    # --- Use markers to identify key indices first ---
    isin_index = -1
    asset_class_index = -1

    for i, item in enumerate(data):
        if isinstance(item, str):
            # Find ISIN using its unique 12-character format
            if re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", item):
                isin_index = i
            # Find the asset class by its specific name
            elif item in ["Azionario", "Obbligazionario"]:
                asset_class_index = i

    # --- Extract data based on patterns and markers ---

    # ISIN: Identified by its index
    if isin_index != -1:
        result["isin"] = data[isin_index]

    # Asset Class and Sector: Sector is the string just before the asset class
    if asset_class_index != -1:
        result["asset_class"] = data[asset_class_index]
        if asset_class_index > 0 and isinstance(data[asset_class_index - 1], str):
            result["sector"] = data[asset_class_index - 1]

    # Country: The first string that appears after the ISIN
    if isin_index != -1:
        for item in data[isin_index + 1 :]:
            if isinstance(item, str) and item != "-":
                result["country"] = item
                break

    # Weight and Currency: Find by iterating through the list
    found_weight = False
    for item in data:
        # Weight is the 'raw' value of the first dictionary containing '%'
        if (
            not found_weight
            and isinstance(item, dict)
            and "display" in item
            and "%" in str(item.get("display"))
        ):
            result["weight"] = item.get("raw")
            found_weight = True

        # Currency is a 3-letter uppercase string (will capture the last one found)
        elif isinstance(item, str) and re.match(r"^[A-Z]{3}$", item):
            result["currency"] = item

    return result


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
            with open("./fetch_error.txt", "+a") as error_file:
                error_file.write(
                    f"Failed to fetch holdings for {prod['name']} ({prod['isin']}): {e}\n"
                )
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
                        translate(h.get("sector")),
                        country_to_iso3(h.get("country")),
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
        "INSERT INTO etf_holdings (etf_isin, holding_isin, holding_name, weight, sector, country, currency) VALUES (?, ?, ?, ?, ?, ?, ?)",
        all_holdings_to_insert,
    )

    # Fix Germany category for brazil etf
    cursor.execute(
        "UPDATE etf_holdings SET country = 'BRA' WHERE holding_isin = 'DE000A0Q4R85'"
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
