import json
import requests

response = requests.request(
    "GET",
    "https://www.ssga.com/bin/v1/ssmp/fund/fundfinder",
    data="",
    headers={
        "accept": "application/json, text/javascript, */*; q=0.01",
    },
    params={
        "country": "it",
        "language": "it",
        "role": "intermediary",
        "product": "",
        "ui": "fund-finder",
    },
)

data = json.loads(response.text)["data"]["funds"]["etfs"]["datas"]

cleaned = []
for etf in data:
    ter = None
    if "perfIndex" in etf:
        if etf["perfIndex"][0]["ter"] != "-":
            ter = etf["perfIndex"][0]["ter"]

    cleaned.append(
        {
            "name": etf["fundName"],
            "ticker": etf["fundTicker"].split(" ")[0],
            "url": etf["fundUri"],
            "inception_date": etf["inceptionDate"][1].replace("-", "/"),
            "use_of_profits": "dist" if "Dist" in etf["fundName"] else "acc",
            "ter": ter,
        }
    )
    print(cleaned[-1])
