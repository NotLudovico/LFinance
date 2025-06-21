import asyncio
from datetime import datetime
import json
import aiohttp
import random


class iShares:
    def __init__(self):
        self.data = []
        self.holdings = {}
        self.error_urls = []

        self.bond_keys = [
            "parValue",
            "isin",
            "price",
            "country",
            "exchange",
            "modified",
            "maturityDate",
            "coupon",
            "currency",
            "issueDate",
        ]
        self.equity_keys = ["isin", "price", "country", "exchange", "currency"]
        self.date_fields = [
            "maturityDate",
            "inceptionDate",
            "calendarPerfAsOfdate",
            "discretePerfAsOfdate",
            "totalFundSizeInMillionsAsOf",
            "navPerfAsOf",
            "navAnnualisedAsOf",
        ]

    async def _funds_list(self, session):
        url = "https://www.ishares.com/it/investitore-privato/it/product-screener/product-screener-v3.1.jsn"
        params = {
            "dcrPath": "/templatedata/config/product-screener-v3/data/it/it/product-screener/ishares-product-screener-backend-config",
            "siteEntryPassthrough": "true",
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.ishares.com/it/investitore-privato/it/prodotti/etf-investments",
        }
        try:
            async with session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                self.data = random.sample(
                    [self._clean_fund(f) for f in data.values()], 10
                )
        except aiohttp.ClientError as e:
            print(f"Error fetching funds list: {e}")
            self.error_urls.append(url)

    def _clean_fund(self, fund):
        conversion_table = {
            "Ad Accumulazione": "ACC",
            "Distribuzione": "DIST",
            "Nessun rendimento": "NO",
        }
        return {
            "name": fund["fundName"],
            "isin": fund["isin"],
            "provider": {"name": "iShares", "code": fund["portfolioId"]},
            "esg": fund["esgRating"],
            "currency": "BOH",
            "url": fund["productPageUrl"],
            "size": fund["totalFundSizeInMillions"]["r"],
            "hedged": True if fund["investorClassName"] == "Hedged" else False,
            "porfitsUsage": conversion_table[fund["useOfProfits"]],
            "assetClass": {
                "main": fund["aladdinAssetClass"],
                "sub": fund["aladdinSubAssetClass"],
            },
        }

    async def _fetch_for(self, session, fund, semaphore):
        pid = fund["provider"]["code"]
        if not pid:
            return None

        url = f"https://www.ishares.com/it/investitore-privato/it/prodotti/{pid}/fund/1506575546154.ajax"
        params = {"tab": "all", "fileType": "json"}
        headers = {"User-Agent": "zorro"}

        async with semaphore:
            print(f"Acquired semaphore, fetching {url}")
            await asyncio.sleep(random.uniform(0.5, 1.5))

            try:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status != 200:
                        print(f"Error fetching {url}: Status {response.status}")
                        return None

                    text_content = await response.text(encoding="utf-8-sig")
                    rows = json.loads(text_content)["aaData"]
                    clean = lambda v: v.get("raw") if isinstance(v, dict) else v

                    fund_holdings = []
                    for row in rows:
                        vals = [clean(v) for v in row]
                        base = {
                            "symbol": vals[0],
                            "name": vals[1],
                            "sector": vals[2],
                            "weight": vals[5],
                        }
                        asset_class = base.get("assetClass")
                        extra_keys = (
                            self.bond_keys
                            if asset_class == "Obbligazionario"
                            else self.equity_keys
                        )

                        extra = dict(zip(extra_keys, vals[8:]))
                        tot = {**base, **extra}
                        self.holdings[extra["isin"]] = tot
                        fund_holdings.append(
                            {
                                "isin": tot["isin"],
                                "ticker": tot["symbol"],
                                "weight": tot["weight"],
                                "sector": tot["sector"],
                            }
                        )

                    return fund_holdings

            except (aiohttp.ClientError, json.JSONDecodeError) as e:
                print(f"Error fetching {url}: {e}")
                return None

    async def _get_holdings(self, session):
        CONCURRENCY_LIMIT = 10
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [self._fetch_for(session, fund, semaphore) for fund in self.data]
        results = await asyncio.gather(*tasks)
        self.holdings = [res for res in results if res is not None]

    def save(self, general, holdings):
        with open(general, "w+") as file:
            json.dump({"data": self.data}, file, indent=2, sort_keys=True)
        with open(holdings, "w+") as file:
            json.dump({"data": self.holdings}, file, indent=2, sort_keys=True)
        if self.error_urls:
            with open("errors.json", "w+") as file:
                json.dump({"errors": self.error_urls}, file, indent=2, sort_keys=True)

    async def run(self, general, holdings):
        print("Starting fetch...")
        async with aiohttp.ClientSession() as session:
            await self._funds_list(session)

            if self.data:
                print("Getting holdings...")
                await self._get_holdings(session)

        print("Saving to file...")
        self.save(general, holdings)


if __name__ == "__main__":
    scraper = iShares()
    asyncio.run(scraper.run("isharesSmallNew.json", "isharesHoldingsNew.json"))
