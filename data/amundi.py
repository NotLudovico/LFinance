import sqlite3
import requests
import sys
from utilities.database import (
    open_db,
    setup_database,
    upsert_etf,
    upsert_security,
    upsert_holding,
)
from utilities.common import ETF, Holding  # <-- use shared models

# --- Configuration ---
API_URL = "https://www.amundietf.it/mapi/ProductAPI/getProductsData"
DATABASE_NAME = "database.db"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
}
# Common context for API requests
API_CONTEXT = {
    "countryName": "Italy",
    "countryCode": "ITA",
    "languageName": "Italian",
    "languageCode": "it",
    "userProfileName": "RETAIL",
    "bcp47Code": "it-IT",
}


def fetch_data(session: requests.Session, payload: dict) -> list:
    """Generic function to fetch data from the API."""
    try:
        response = session.post(API_URL, json=payload, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json().get("products", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError:
        print("Error: Could not decode JSON from response.", file=sys.stderr)
        sys.exit(1)


def process_etfs_data(products: list) -> tuple[list[tuple], list[str]]:
    """
    Build ETF objects from raw products and return:
      - list of tuples ready for DB insertion
      - list of ISINs to fetch holdings for
    """
    etfs_for_db: list[tuple] = []
    isins: list[str] = []

    for prod in products:
        if prod.get("productType") == "DELISTED_PRODUCT":
            continue

        chars = prod.get("characteristics", {}) or {}
        isin = chars.get("ISIN")
        if not isin:
            continue
        # Create normalized ETF via shared model
        etf = ETF(
            isin=isin,
            issuer="amundi",
            name=chars.get("SHARE_MARKETING_NAME"),
            ticker=chars.get("MNEMO"),
            ter=chars.get("TER"),
            nav=chars.get("NAV"),
            size=chars.get("AUM_IN_EURO"),
            currency=chars.get("CURRENCY"),
            asset_class=chars.get("ASSET_CLASS"),
            sub_asset_class=chars.get("SUBASSET_CLASS"),
            region=chars.get("INVESTMENT_ZONE"),
            use_of_profits=chars.get("SHARE_TYPE"),
            replication=chars.get("FUND_REPLICATION_METHODOLOGY"),
            domicile=chars.get("FUND_DOMICILIATION_COUNTRY"),
            inception_date=chars.get("INCEPTION_DATE"),
            url=prod.get("url"),
        )
        etfs_for_db.append(etf.to_db_tuple())
        isins.append(isin)

    return etfs_for_db, isins


def process_holdings_data(holdings_products: list) -> list[tuple]:
    """
    Build Holding objects from raw composition payload and return
    a list of tuples ready for DB insertion.
    """
    holdings_for_db: list[tuple] = []

    for etf in holdings_products:
        if not isinstance(etf, dict) or not etf.get("composition"):
            continue

        composition_data = etf.get("composition", {}).get("compositionData", []) or []
        etf_isin = etf.get("productId")

        for holding in composition_data:
            chars = holding.get("compositionCharacteristics", {}) or {}

            # Keep your special-case 'cash' mapping based on name
            name = chars.get("name")
            sector = chars.get("sector")
            if isinstance(name, str) and ("CASH " in name or " CASH" in name):
                sector = "cash"

            weight = holding.get("weight")
            if weight:
                weight *= 100

            h = Holding(
                etf_isin=etf_isin,
                holding_isin=chars.get("isin"),
                holding_name=name,
                weight=weight,
                sector=sector,
                country=chars.get("countryOfRisk"),
                currency=chars.get("currency"),
            )
            holdings_for_db.append(h.to_db_tuple())

    return holdings_for_db


def main():
    """Main function to orchestrate the data scraping and storage process."""
    with requests.Session() as session:
        # 1) Fetch the list of all ETFs
        etf_list_payload = {
            "characteristics": [
                "ISIN",
                "MNEMO",
                "TER",
                "SHARE_MARKETING_NAME",
                "CURRENCY",
                "INDEX_TICKER",
                "EXCHANGE_PLACE",
                "INCEPTION_DATE",
                "AUM_IN_EURO",
                "NAV",
                "FUND_TYPE",
                "FUND_REPLICATION_METHODOLOGY",
                "STRATEGY",
                "SUBASSET_CLASS",
                "ASSET_CLASS",
                "INVESTMENT_ZONE",
                "CATEGORY",
                "DISTRIBUTION_POLICY",
                "CURRENCY_HEDGE",
                "FUND_DOMICILIATION_COUNTRY",
                "MARKET",
                "SHARE_TYPE",
            ],
            "context": API_CONTEXT,
            "productType": "ALL",
            "url": True,
            "filters": [],
        }
        print("Fetching ETF list...")
        all_products = fetch_data(session, etf_list_payload)

        # 2) Process ETF data via shared model
        etfs_to_insert, isins_to_fetch = process_etfs_data(all_products)
        print(f"Found {len(etfs_to_insert)} active ETFs to process.")

        # 3) Fetch holdings data for all found ETFs in a single request
        holdings_payload = {
            "context": API_CONTEXT,
            "productIds": isins_to_fetch,
            "productType": "PRODUCT",
            "composition": {
                "compositionFields": [
                    "isin",
                    "name",
                    "weight",
                    "sector",
                    "currency",
                    "countryOfRisk",
                ]
            },
        }
        print(f"Fetching holdings for {len(isins_to_fetch)} ETFs...")
        holdings_products = fetch_data(session, holdings_payload)

        # 4) Process holdings via shared model
        holdings_to_insert = process_holdings_data(holdings_products)
        print(f"Found {len(holdings_to_insert)} total holdings to save.")

    # 5) Insert all data into the normalized DB
    with open_db(DATABASE_NAME) as conn:
        # Recreate schema (destructive, same behavior as before)
        setup_database(conn)

        print("Upserting ETFs...")
        for etf_tuple in etfs_to_insert:
            upsert_etf(conn, etf_tuple)

        # Clear previous holdings for these ETFs (so removals don’t linger)
        if isins_to_fetch:
            print("Clearing old holdings data...")
            placeholders = ", ".join("?" for _ in isins_to_fetch)
            conn.execute(
                f"DELETE FROM etf_holdings WHERE etf_isin IN ({placeholders})",
                isins_to_fetch,
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
        ) in holdings_to_insert:
            if weight is None or weight < 0:
                weight = 0.0

            # Ensure a non-empty security.name for upsert
            name = (
                holding_name
                or (sector == "cash" and "CASH")
                or holding_isin
                or "UNKNOWN"
            )

            if holding_isin and len(holding_isin) != 12:
                continue

            security_id = upsert_security(
                conn,
                isin=holding_isin,
                name=str(name),
                sector=sector,
                country=country,
                currency=currency,
            )
            upsert_holding(
                conn, etf_isin=etf_isin, security_id=security_id, weight=weight
            )

        print("Rebuilding search index...")

    print("✅ Process complete. Database is up to date.")


if __name__ == "__main__":
    main()
