import requests
import json

symbols_list = [
    1069936,  # Natural Gas
    1072386,  # Heating Oil
    3334534,  # RBOB Gasoline
    1070572,  # WTI Crude Oil
    1054972,  # Brent Crude Oil
    1045598,  # Silver 5000oz
    18366493,  # Platinum
    1046328,  # Copper
    608346748,  # Iron Ore 62% Fe, CFR China (TSI) Swa
    18483223,  # Palladium
    1045492,  # COMEX Gold
    1066877,  # Cotton
    1037017,  # Soybeans
    1061323,  # White Sugar
    1046731,  # Sugar #11
    1037373,  # Wheat
    1046650,  # Coffee Arabica
    1039187,  # Corn
    1044934,  # Cattle
    1045268,  # Feeder Cattle
    1044843,  # Lean Hogs
    1038557,  # Soybean Meal
    10003387,  # Coffee Robusta
    1055540,  # Cocoa
    1067048,  # Orange Juice
]

url = "https://markets.ft.com/data/chartapi/series"


for symbol in symbols_list:
    payload = {
        "days": 1825,
        "dataNormalized": False,
        "dataPeriod": "Week",
        "dataInterval": 1,
        "realtime": False,
        "yFormat": "0.###",
        "timeServiceFormat": "JSON",
        "rulerIntradayStart": 26,
        "rulerIntradayStop": 3,
        "rulerInterdayStart": 10957,
        "rulerInterdayStop": 365,
        "returnDateType": "ISO8601",
        "elements": [
            {
                "Label": "4a605efb",
                "Type": "price",
                "Symbol": symbol,
                "OverlayIndicators": [],
                "Params": {},
            }
        ],
    }

    headers = {"cookie": "GZIP=1", "Accept": "*/*", "content-type": "application/json"}
    response = requests.request("POST", url, json=payload, headers=headers)

    try:
        result = {}
        response_json = response.json()

        dates = response_json.get("Dates", [])

        result["start_date"] = dates[0]
        result["end_date"] = dates[-1]
        result["company_name"] = response_json.get("Elements")[0].get("CompanyName")
        result["issue_type"] = response_json.get("Elements")[0].get("IssueType")
        result["symbol"] = response_json.get("Elements")[0].get("Symbol")
        result["currency"] = response_json.get("Elements")[0].get("Currency")
        result["exchange_id"] = response_json.get("Elements")[0].get("ExchangeId")

        # Remember there are 252 days in a year
        result["prices"] = (
            response_json.get("Elements")[0].get("ComponentSeries")[0].get("Values")
        )

        filename = f"{result["company_name"]}.json"
        with open(filename, "w") as file:
            json.dump(result, file, indent=4)

        print(f"Dates for symbol {symbol} saved to {filename}\n")

    except json.JSONDecodeError:
        print(f"Failed to decode JSON for symbol {symbol}. Response: {response.text}")
