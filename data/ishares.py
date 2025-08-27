import asyncio
import json
import re
from typing import Any, Coroutine, Dict, List, Tuple
from datetime import datetime
import locale

import aiohttp
from tqdm import tqdm  # Optional: progress bar

# Normalization helpers
from utilities.country import country_to_iso3
from utilities.translate import translate
from utilities.common import ETF, Holding

# New DB helpers (normalized schema)
from utilities.database import (
    open_db,
    setup_database,
    upsert_etf,
    upsert_security,
    upsert_holding,
)

# --- Configuration ---
DB_NAME = "database.db"
CONCURRENT_REQUESTS = 10  # Limit concurrent requests


# --- Constants & Helpers ---
PROFITS_CONV = {
    "Ad Accumulazione": "acc",
    "Distribuzione": "dist",
    "Nessun rendimento": None,
}


def clean_product(prod: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize the product summary coming from the product screener."""
    # Parse inception date like "16 gen 2018"
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
    """Robust parser for iShares holdings rows (list with mixed types)."""
    result = {
        "country": None,
        "sector": None,
        "asset_class": None,
        "name": None,
        "weight": None,
        "isin": None,
        "currency": None,
    }

    # Name
    if len(data) > 1 and isinstance(data[1], str):
        result["name"] = data[1]
        if "CASH " in result["name"] or " CASH" in result["name"]:
            result["sector"] = "cash"

    # Locate ISIN and the asset class position
    isin_index = -1
    asset_class_index = -1
    for i, item in enumerate(data):
        if isinstance(item, str):
            if re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", item):
                isin_index = i
            elif item in ["Azionario", "Obbligazionario"]:
                asset_class_index = i

    # ISIN
    if isin_index != -1:
        result["isin"] = data[isin_index]

    # Asset class + sector (sector usually precedes asset class)
    if asset_class_index != -1:
        result["asset_class"] = data[asset_class_index]
        if asset_class_index > 0 and isinstance(data[asset_class_index - 1], str):
            result["sector"] = data[asset_class_index - 1]

    # Country (first string after ISIN)
    if isin_index != -1:
        for item in data[isin_index + 1 :]:
            if isinstance(item, str) and item != "-":
                result["country"] = item
                break

    # Weight (from dict with '%') and Currency (last 3-letter code seen)
    found_weight = False
    for item in data:
        if (
            not found_weight
            and isinstance(item, dict)
            and "display" in item
            and "%" in str(item.get("display"))
        ):
            result["weight"] = item.get("raw")
            found_weight = True
        elif isinstance(item, str) and re.match(r"^[A-Z]{3}$", item):
            result["currency"] = item

    return result


async def get_products_list(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fetch the main list of iShares products (IT site)."""
    url = "https://www.ishares.com/it/investitore-privato/it/product-screener/product-screener-v3.1.jsn"
    params = {
        "dcrPath": "/templatedata/config/product-screener-v3/data/it/it/product-screener/ishares-product-screener-backend-config",
        "siteEntryPassthrough": "true",
    }
    headers = {"accept": "application/json, text/plain, */*"}
    print("Fetching product list...")
    async with session.get(url, params=params, headers=headers) as response:
        response.raise_for_status()
        data = await response.json()
        return [clean_product(v) for _, v in data.items()]


async def fetch_holding(
    session: aiohttp.ClientSession, prod: Dict[str, Any], semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Fetch holdings JSON for a single product and parse it."""
    url = f"https://www.ishares.com/it/investitore-privato/it/prodotti/{prod['pid']}/fund/1506575546154.ajax"
    params = {"tab": "all", "fileType": "json"}

    async with semaphore:
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json(encoding="utf-8-sig")  # handles BOM
                holdings_data = [
                    parse_ishares_holding(h) for h in data.get("aaData", [])
                ]
                return {"product": prod, "holdings": holdings_data}
        except (aiohttp.ClientError, json.JSONDecodeError, asyncio.TimeoutError) as e:
            return {"product": prod, "holdings": []}


def handle_nested_etfs(
    holdings: List[tuple], nested_etf_isins: List[str]
) -> List[tuple]:
    """
    Unroll specified nested ETFs by redistributing their underlying weights into the parents.
    Input/Output holding tuple format:
      (etf_isin, holding_isin, holding_name, weight, sector, country, currency)
    """
    if not nested_etf_isins:
        return holdings

    nested_set = set(nested_etf_isins)
    parent_relationships = []
    by_nested: Dict[str, List[tuple]] = {isin: [] for isin in nested_set}

    for h in holdings:
        etf_isin, holding_isin = h[0], h[1]
        if holding_isin in nested_set:
            parent_relationships.append(
                {"parent_isin": etf_isin, "nested_isin": holding_isin, "weight": h[3]}
            )
        if etf_isin in nested_set:
            by_nested[etf_isin].append(h)

    if not parent_relationships:
        print("\nNo specified nested ETFs found among holdings.")
        return holdings

    unrolled: List[tuple] = []
    for rel in parent_relationships:
        parent_isin = rel["parent_isin"]
        nested_isin = rel["nested_isin"]
        w_parent = rel["weight"]

        for h in by_nested.get(nested_isin, []):
            w_new = h[3] * w_parent / 100 if h[3] and w_parent else None
            unrolled.append((parent_isin, h[1], h[2], w_new, h[4], h[5], h[6]))

    filtered = [
        h for h in holdings if h[0] not in nested_set and h[1] not in nested_set
    ]
    filtered.extend(unrolled)

    print(
        f"\nUnrolled {len(parent_relationships)} nested ETF instances. "
        f"Added {len(unrolled)} rescaled holdings."
    )
    return filtered


async def main():
    """End-to-end: fetch products & holdings, normalize, and upsert into the DB."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    # Fetch products and all holdings concurrently
    async with aiohttp.ClientSession(headers=headers) as session:
        products = await get_products_list(session)

        etf_tuples: List[Tuple] = []
        holdings_tuples: List[Tuple] = []
        isins_to_update: List[str] = []

        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks: List[Coroutine] = [
            fetch_holding(session, p, semaphore) for p in products
        ]

        print(
            f"Fetching holdings for {len(products)} ETFs "
            f"with {CONCURRENT_REQUESTS} concurrent workers..."
        )

        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            result = await future
            if not result:
                continue

            prod = result["product"]
            holdings = result["holdings"]

            # Normalized ETF via shared model
            etf = ETF(
                isin=prod["isin"],
                issuer=prod["issuer"],
                name=prod.get("name"),
                ticker=prod.get("ticker"),
                ter=prod.get("ter"),
                nav=prod.get("nav"),
                size=prod.get("size"),
                currency=prod.get("currency"),
                asset_class=prod.get("asset_class"),
                sub_asset_class=prod.get("sub_asset_class"),
                region=prod.get("region"),
                use_of_profits=prod.get("use_of_profits"),
                replication=prod.get("replication"),
                domicile=prod.get("domicile"),
                inception_date=prod.get("inception_date"),
                url=prod.get("url"),
            )
            etf_tuples.append(etf.to_db_tuple())
            isins_to_update.append(prod["isin"])

            # Normalized holdings via shared model -> DB tuple
            for h in holdings:
                h_obj = Holding(
                    etf_isin=prod["isin"],
                    holding_isin=h.get("isin"),
                    holding_name=h.get("name"),
                    weight=h.get("weight"),
                    sector=h.get("sector"),
                    country=h.get("country"),
                    currency=h.get("currency"),
                )
                holdings_tuples.append(h_obj.to_db_tuple())

    # Optionally unroll selected nested ETFs
    print("\nUnrolling...")
    ETFS_TO_UNROLL = [
        "DE000A0Q4R85",
        "FR0011720911",
        "IE0006GNB732",
        "IE000JJPY166",
        "IE000MELAE65",
        "IE000OKVTDF7",
        "IE000QVYFUT7",
        "IE00B14X4S71",
        "IE00B1FZS798",
        "IE00B1FZSB30",
        "IE00B1FZSC47",
        "IE00B3VWN393",
        "IE00B5M4WH52",
        "IE00B66F4759",
        "IE00BD4DX952",
        "IE00BF553838",
        "IE00BFMNPS42",
        "IE00BFNM3G45",
        "IE00BG36TC12",
        "IE00BG370F43",
        "IE00BGHQ0G80",
        "IE00BGQYRS42",
        "IE00BGSF1X88",
        "IE00BHZPJ239",
        "IE00BHZPJ452",
        "IE00BHZPJ676",
        "IE00BHZPJ890",
        "IE00BJ0KDR00",
        "IE00BJ5JNY98",
        "IE00BJ5JP097",
        "IE00BJ5JP212",
        "IE00BJ5JP329",
        "IE00BJ5JP436",
        "IE00BJ5JP659",
        "IE00BJ5JP766",
        "IE00BJK55B31",
        "IE00BJK55C48",
        "IE00BJZ2DD79",
        "IE00BKKKWJ26",
        "IE00BL25JM42",
        "IE00BL25JN58",
        "IE00BLDGH553",
        "IE00BQT3WG13",
        "IE00BTJRMP35",
        "IE00BYPHT736",
        "IE00BYVJRR92",
        "IE00BYYR0489",
        "IE00BYZTVT56",
        "IE00BZCQB185",
        "LU0290355717",
        "LU0290356871",
        "LU0290358224",
        "LU0290358497",
        "LU0292109344",
        "LU0292109856",
        "LU0322253732",
        "LU0322253906",
        "LU0328475792",
        "LU0524480265",
        "LU1109943388",
        "LU2178481649",
    ]
    holdings_tuples = handle_nested_etfs(holdings_tuples, ETFS_TO_UNROLL)

    # Persist into normalized DB
    print("\nWriting to database...")
    with open_db(DB_NAME) as conn:
        # Drop & recreate normalized schema (destructive, consistent with other scrapers)
        setup_database(conn)

        print("Upserting ETFs...")
        for tup in etf_tuples:
            upsert_etf(conn, tup)

        # Clear previous holdings for these ETFs to avoid stale rows
        if isins_to_update:
            print("Clearing old holdings...")
            placeholders = ", ".join("?" for _ in isins_to_update)
            conn.execute(
                f"DELETE FROM etf_holdings WHERE etf_isin IN ({placeholders})",
                isins_to_update,
            )

        print("Upserting securities and holdings...")
        for (
            etf_isin,
            holding_isin,
            holding_name,
            weight,
            sector,
            country,
            currency,
        ) in holdings_tuples:
            # Basic hygiene
            if holding_isin and len(holding_isin) != 12:
                continue
            if weight is None or weight < 0:
                weight = 0.0

            # Ensure a usable security name
            name = (
                holding_name
                or (sector == "cash" and "CASH")
                or holding_isin
                or "UNKNOWN"
            )

            sec_id = upsert_security(
                conn,
                isin=holding_isin,
                name=str(name),
                sector=sector,
                country=country,
                currency=currency,
            )
            upsert_holding(conn, etf_isin=etf_isin, security_id=sec_id, weight=weight)

        print("Rebuilding search index...")

    print("✅ iShares scraping complete. Database is up to date.")


if __name__ == "__main__":
    asyncio.run(main())
