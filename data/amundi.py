import sqlite3
import json
import requests

url = "https://www.amundietf.it/mapi/ProductAPI/getProductsData"

payload = {
    "sortCriterias": [],
    "characteristics": [
        "ISIN",
        "TICKER",
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
        "IMPACT",
        "CATEGORY",
        "DISTRIBUTION_POLICY",
        "CURRENCY_HEDGE",
        "FUND_DOMICILIATION_COUNTRY",
        "MARKET",
        "SHARE_TYPE",
    ],
    "context": {
        "countryName": "Italy",
        "countryCode": "ITA",
        "languageName": "Italian",
        "languageCode": "it",
        "userProfileName": "RETAIL",
        "bcp47Code": "it-IT",
    },
    "productType": "ALL",
    "historics": [],
    "url": True,
    "filters": [],
}
headers = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
}

response = requests.request("POST", url, json=payload, headers=headers)
data = json.loads(response.text)["products"]

cleaned = []

conn = sqlite3.connect("database.db")
cursor = conn.cursor()
cursor.execute(
    "CREATE TABLE IF NOT EXISTS etfs (isin TEXT PRIMARY KEY, product_type TEXT, issuer TEXT, name TEXT, ticker TEXT, nav DOUBLE, asset_class TEXT, sub_asset_class TEXT, market_type TEXT, region TEXT, url TEXT, domicile TEXT, inception_date TEXT, use_of_profits TEXT, size DOUBLE, currency TEXT, ter DOUBLE)"
)

cursor.execute(
    "CREATE TABLE IF NOT EXISTS etf_holdings (id INTEGER PRIMARY KEY AUTOINCREMENT, etf_isin TEXT, holding_isin TEXT, holding_name TEXT, weight DOUBLE, country TEXT, currency TEXT)"
)
placeholders = ", ".join("?" for _ in range(17))
isins = []

for prod in data:
    if prod["productType"] == "DELISTED_PRODUCT":
        continue

    chars = prod.get("characteristics", {})

    isins.append(chars.get("ISIN"))

    # Use of profits
    uop = None
    if "SHARE_TYPE" in chars:
        if "Acc" in chars["SHARE_TYPE"]:
            uop = "ACC"
        else:
            uop = "DIST"

    # Saving into DB
    cursor.execute(
        f"INSERT INTO etfs (product_type, issuer, name, isin, ticker, nav, asset_class, sub_asset_class, market_type, region, url, domicile, inception_date, use_of_profits, size, currency, ter) VALUES ({placeholders})",
        [
            "etf",
            "amundi",
            chars.get("SHARE_MARKETING_NAME"),
            chars.get("ISIN"),
            chars.get("TICKER") if "TICKER" in chars else chars.get("MNEMO"),
            chars.get("NAV"),
            chars.get("ASSET_CLASS"),
            chars.get("SUBASSET_CLASS"),
            chars.get("MARKET"),
            None,
            None,
            chars.get("FUND_DOMICILIATION_COUNTRY"),
            None,
            uop,
            chars.get("AUM_IN_EURO"),
            chars.get("CURRENCY"),
            chars.get("TER"),
        ],
    )

print(isins)
response = requests.request(
    "POST",
    "https://www.amundietf.it/mapi/ProductAPI/getProductsData",
    json={
        "context": {
            "countryCode": "ITA",
            "countryName": "Italy",
            "googleCountryCode": "IT",
            "domainName": "www.amundietf.it",
            "bcp47Code": "it-IT",
            "languageName": "Italian",
            "gtmCode": "GTM-MCR6CN7",
            "languageCode": "it",
            "userProfileName": "RETAIL",
            "userProfileSlug": "retail",
            "portalProfileName": None,
            "portalProfileSlug": None,
        },
        "productIds": isins,
        "productType": "PRODUCT",
        "composition": {
            "compositionFields": [
                "isin",
                "name",
                "weight",
                "quantity",
                "currency",
                "sector",
                "country",
                "countryOfRisk",
            ]
        },
    },
    headers={
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    },
)

holdings_data = json.loads(response.text)["products"]
sql = "INSERT INTO etf_holdings (etf_isin, holding_isin, holding_name, weight, country, currency) VALUES (?, ?, ?, ?, ?, ?)"

for etf in holdings_data:
    if isinstance(etf, dict) and etf and etf.get("composition"):
        composition_data = etf.get("composition", {}).get("compositionData", [])
        if composition_data:
            for holding in composition_data:
                chars = holding.get("compositionCharacteristics", {})

                values = (
                    etf["productId"],
                    chars.get("isin"),
                    chars.get("name"),
                    holding.get("weight") * 100,
                    chars.get("countryOfRisk"),
                    chars.get("currency"),
                )
                cursor.execute(sql, values)
conn.commit()
conn.close()
