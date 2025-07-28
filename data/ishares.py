import re
import requests
import json
import sqlite3
from bs4 import BeautifulSoup

PROFITS_CONV = {
    "Ad Accumulazione": "ACC",
    "Distribuzione": "DIST",
    "Nessun rendimento": None,
}


def clean_product(prod):
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
        "use_of_profits": PROFITS_CONV[prod["useOfProfits"]],
        "size": prod["totalNetAssets"]["r"],
        "currency": prod["seriesBaseCurrencyCode"],
        "ter": prod["ter_ocf"]["r"] if prod["ter_ocf"] != "-" else None,
    }


def get_products_list():
    url = "https://www.ishares.com/it/investitore-privato/it/product-screener/product-screener-v3.1.jsn"

    querystring = {
        "dcrPath": "/templatedata/config/product-screener-v3/data/it/it/product-screener/ishares-product-screener-backend-config",
        "siteEntryPassthrough": "true",
    }

    payload = ""
    headers = {
        "cookie": "ts-us-ishares-locale=en_US; StatisticalAnalyticsEnabled=false; ts-ishares-it-locale=it_IT",
        "accept": "application/json, text/plain, */*",
    }

    response = requests.request(
        "GET", url, data=payload, headers=headers, params=querystring
    )

    data = json.loads(response.text)
    clean = []

    for _, v in data.items():
        clean.append(clean_product(v))

    return clean


def create_holdings():
    pass


def parse_ishares_holding(holding_data: list) -> dict:
    """
    Parses a holding list for core information (name, ISIN, weight, etc.)
    and ignores extra bond- or equity-specific details.
    """
    parsed = {}

    # --- Separate by Type ---
    strings = [item for item in holding_data if isinstance(item, str)]
    dicts = [item for item in holding_data if isinstance(item, dict)]

    # --- Find the Weight ---
    # Heuristic: The weight's raw value is a precise fraction.
    percent_dicts = [d for d in dicts if "%" in d.get("display", "")]
    if percent_dicts:
        percent_dicts.sort(
            key=lambda d: len(str(d.get("raw", "")).split(".")[-1]), reverse=True
        )
        parsed["weight"] = percent_dicts[0].get("raw")

    # --- Parse Strings for Core Info ---
    isin_regex = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")
    for s in strings:
        if isin_regex.match(s):
            parsed["isin"] = s
            strings.remove(s)
            break

    if strings and len(strings[-1]) == 3 and strings[-1].isupper():
        parsed["currency"] = strings.pop(-1)

    # Handle exchange and country, ignoring the exchange if it's '-'
    if len(strings) > 1:
        strings.pop(-1)  # ignoring exchange
        parsed["country"] = strings.pop(-1)

    # Use remaining strings as identifiers
    if len(strings) > 0:
        parsed["ticker"] = strings[0]
    if len(strings) > 1:
        parsed["name"] = strings[1]

    return parsed


if __name__ == "__main__":
    prods = get_products_list()

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # Create table
    cursor.execute("DROP TABLE IF EXISTS etfs")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS etfs (isin TEXT PRIMARY KEY, product_type TEXT, issuer TEXT, name TEXT, ticker TEXT, nav DOUBLE, asset_class TEXT, sub_asset_class TEXT, market_type TEXT, region TEXT, url TEXT, domicile TEXT, inception_date TEXT, use_of_profits TEXT, size DOUBLE, currency TEXT, ter DOUBLE)"
    )

    # Create holdins-etf correspondence table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS etf_holdings (id INTEGER PRIMARY KEY AUTOINCREMENT, etf_isin TEXT, holding_isin TEXT, holding_name TEXT, weight DOUBLE, country TEXT, currency TEXT)"
    )

    # Insert data
    session = requests.Session()
    for prod in prods:
        print(prod["isin"])
        pid = prod["pid"]
        del prod["pid"]
        columns = ", ".join(k for k in prod.keys() if k != "pid")
        placeholders = ", ".join("?" for _ in prod)
        values = list(prod.values())
        cursor.execute(f"INSERT INTO etfs ({columns}) VALUES ({placeholders})", values)

        try:
            response = session.get(
                f"https://www.ishares.com/it/investitore-privato/it/prodotti/{pid}/fund/1506575546154.ajax",
                params={"tab": "all", "fileType": "json"},
            )
            # Response JSON is not in standart utf-8 but has a leading BOM
            response.encoding = "utf-8-sig"

            data = json.loads(response.text)["aaData"]

            for holding in data:
                h = parse_ishares_holding(holding)
                # Use ? placeholders for safety and correctness
                sql = "INSERT INTO etf_holdings (etf_isin, holding_isin, holding_name, weight, country, currency) VALUES (?, ?, ?, ?, ?, ?)"
                values = (
                    prod["isin"],
                    h.get("isin"),
                    h.get("name"),
                    h.get("weight"),
                    h.get("country"),
                    h.get("currency"),
                )
                cursor.execute(sql, values)

        except Exception as e:
            print(f"Errored on {prod['name']}: {e}")
            continue

    conn.commit()
    conn.close()
