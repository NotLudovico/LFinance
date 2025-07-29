import sqlite3
import requests
import sys
from utilities.database import setup_database
from utilities.country import country_to_iso3
from utilities.translate import translate

# --- Configuration ---
# Use constants for values that don't change. This makes the code cleaner and easier to modify.
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
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json().get("products", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data: {e}", file=sys.stderr)
        sys.exit(1)  # Exit if we can't get the data
    except ValueError:  # Catches JSON decoding errors
        print("Error: Could not decode JSON from response.", file=sys.stderr)
        sys.exit(1)


def process_etfs_data(products: list) -> tuple[list[tuple], list[str]]:
    """Processes raw ETF data and prepares it for database insertion."""
    etfs_for_db = []
    isins = []

    for prod in products:
        if prod.get("productType") == "DELISTED_PRODUCT":
            continue

        chars = prod.get("characteristics", {})
        isin = chars.get("ISIN")
        if not isin:
            continue

        isins.append(isin)

        etfs_for_db.append(
            (
                isin,
                "amundi",  # Issuer
                chars.get("SHARE_MARKETING_NAME"),
                chars.get("MNEMO"),
                chars.get("TER"),
                chars.get("NAV"),
                chars.get("AUM_IN_EURO"),
                chars.get("CURRENCY"),
                translate(chars.get("ASSET_CLASS")),
                translate(chars.get("SUBASSET_CLASS")),
                translate(chars.get("INVESTMENT_ZONE")),
                translate(chars.get("SHARE_TYPE")),
                translate(chars.get("FUND_REPLICATION_METHODOLOGY")),
                country_to_iso3(chars.get("FUND_DOMICILIATION_COUNTRY")),
                chars.get("INCEPTION_DATE"),
                prod.get("url"),
            )
        )
    return etfs_for_db, isins


def process_holdings_data(holdings_products: list) -> list[tuple]:
    """Processes raw holdings data and prepares it for database insertion."""
    holdings_for_db = []
    for etf in holdings_products:
        if not isinstance(etf, dict) or not etf.get("composition"):
            continue

        composition_data = etf.get("composition", {}).get("compositionData", [])
        etf_isin = etf.get("productId")

        for holding in composition_data:
            chars = holding.get("compositionCharacteristics", {})

            if "name" in chars:
                if "CASH " in chars["name"] or " CASH" in chars["name"]:
                    chars["sector"] = "cash"

            holdings_for_db.append(
                (
                    etf_isin,
                    chars.get("isin"),
                    chars.get("name"),
                    holding.get("weight", 0) * 100,
                    translate(chars.get("sector")),
                    country_to_iso3(chars.get("countryOfRisk")),
                    chars.get("currency"),
                )
            )
    return holdings_for_db


def main():
    """Main function to orchestrate the data scraping and storage process."""
    # Use a requests.Session() object for connection pooling
    with requests.Session() as session:
        # 1. Fetch the list of all ETFs
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

        # 2. Process ETF data
        etfs_to_insert, isins_to_fetch = process_etfs_data(all_products)
        print(f"Found {len(etfs_to_insert)} active ETFs to process.")

        # 3. Fetch holdings data for all found ETFs in a single request
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

        # 4. Process holdings data
        holdings_to_insert = process_holdings_data(holdings_products)
        print(f"Found {len(holdings_to_insert)} total holdings to save.")

    # 5. Connect to DB and insert all data
    # Use a `with` statement to ensure the connection is properly managed
    with sqlite3.connect(DATABASE_NAME) as conn:
        setup_database(conn)
        cursor = conn.cursor()

        # Insert ETFs using executemany for huge performance gains
        print("Inserting ETF data into database...")
        etf_sql = """
            INSERT OR REPLACE INTO etfs 
            (isin, issuer, name, ticker, ter, nav, size, currency, asset_class, 
             sub_asset_class, region, use_of_profits, replication, domicile, 
             inception_date, url) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(etf_sql, etfs_to_insert)

        # Clear existing holdings for the ETFs we are updating
        print("Clearing old holdings data...")
        # Create a tuple of isins for the SQL query
        isin_placeholders = ", ".join("?" for _ in isins_to_fetch)
        cursor.execute(
            f"DELETE FROM etf_holdings WHERE etf_isin IN ({isin_placeholders})",
            isins_to_fetch,
        )

        # Insert holdings using executemany
        print("Inserting new holdings data...")
        holdings_sql = """
            INSERT INTO etf_holdings 
            (etf_isin, holding_isin, holding_name, weight, sector, country, currency) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(holdings_sql, holdings_to_insert)

    print("✅ Process complete. Database is up to date.")


if __name__ == "__main__":
    main()
