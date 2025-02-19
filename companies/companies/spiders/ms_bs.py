import re
import json
import requests
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from companies.itemloaders import CompanyLoader
from companies.items import Company
from scrapy import Request


class MsBsSpider(CrawlSpider):
    name = "msbs"
    allowed_domains = ["borsaitaliana.it", "morningstar.it"]
    sec_ids = []
    start_urls = []
    custom_settings = {
        "ITEM_PIPELINES": {
            "companies.pipelines.MergeBSMS": 300,
        }
    }

    rules = (
        Rule(
            LinkExtractor(
                allow=(r"/scheda/",), deny=(r"/obbligazioni", r"/warrant", r"/eurotlx/")
            ),
            callback="parse_detail",
        ),
    )

    def start_requests(self):
        url_list = []
        # Get Security ID's from MS
        response = requests.request(
            "GET",
            "https://tools.morningstar.co.uk/api/rest.svc/klr5zyak8x/security/screener",
            data="",
            headers={},
            params={
                "page": "1",
                "pageSize": "1000",
                "sortOrder": "Name asc",
                "outputType": "json",
                "universeIds": "E0EXG$XMIL",
                "securityDataPoints": "SecId|Ticker",
            },
        )
        self.sec_ids = json.loads(response.text)["rows"]

        for i in range(1, 1):
            url_list.append(
                Request(
                    f"https://www.borsaitaliana.it/borsa/azioni/listino-a-z.html?lang=en&page={i}"
                )
            )

        for company in self.sec_ids:
            url_list.append(
                Request(
                    f"https://tools.morningstar.it/it/stockreport/default.aspx?Site=it&id={company["SecId"]}&LanguageId=it-IT&SecurityToken={company["SecId"]}"
                    + "]3]0]E0EXG%24XMIL",
                    callback=self.parse_ms,
                )
            )
        return url_list

    def parse_detail(self, response):
        company = CompanyLoader(item=Company(), selector=response)
        company.add_xpath("name", "//title")
        els = response.xpath(
            "(//table[contains(@class, '-clear-mtop') and not(contains(@class, '-indice'))])[2]/tr//span[contains(@class, '-right')]/text()"
        ).getall()

        company.add_value("P1M", els[0])
        company.add_value("P6M", els[1])
        company.add_value("P1Y", els[2])

        company.add_value("isin", els[3])
        assert len(company.get_collected_values("isin")[0]) == 12

        company.add_value("ticker", els[4])
        assert len(company.get_collected_values("ticker")[0]) < 7

        # company.add_value(
        #     "weekly_prices",
        #     json.loads(
        #         self.get_weekly_prices(company.get_collected_values("ticker")[0])
        #     )["d"],
        # )

        yield company.load_item()

    def parse_ms(self, response):
        company = CompanyLoader(item=Company(), selector=response)
        market_cap = response.css("td#Col0MCap::text").get()

        if market_cap != "-":
            company.add_value("market_cap", market_cap)
        p_e = response.css("td#Col0PE::text").get()
        if p_e != "-":
            company.add_value("p_e", p_e)
        div_yield = response.css("td#Col0Yield::text").get()
        if div_yield != "-":
            company.add_value("div_yield", div_yield)

        company.add_value("isin", response.css("td#Col0Isin::text").get())
        company.add_value(
            "sector",
            response.css("div#CompanyProfile > .item")[1].get().split(">")[3][:-5],
        )
        company.add_value(
            "sub_sector",
            response.css("div#CompanyProfile > .item")[2].get().split(">")[3][:-5],
        )

        yield company.load_item()

    def parse_eurotlx(self, response):
        company = CompanyLoader(item=Company(), selector=response)
        company.add_xpath("name", "//title")

        els = response.xpath(
            "(//table[contains(@class, '-clear-mtop') and not(contains(@class, '-indice'))])[2]/tr//span[contains(@class, '-right')]/text()"
        ).getall()

        company.add_value("isin", els[2])
        assert len(company.get_collected_values("isin")[0]) == 12

        company.add_value("ticker", els[4])
        assert len(company.get_collected_values("ticker")[0]) < 7

        # company.add_value(
        #     "weekly_prices",
        #     json.loads(
        #         self.get_weekly_prices(company.get_collected_values("ticker")[0])
        #     )["d"],
        # )

        yield company.load_item()

    def get_weekly_prices(self, ticker):
        url = "https://charts.borsaitaliana.it/charts/services/ChartWService.asmx/GetPrices"

        payload = {
            "request": {
                "SampleTime": "1d",
                "TimeFrame": "5y",
                "RequestedDataSetType": "ohlc",
                "ChartPriceType": "price",
                "Key": ticker + ".MTA",
            }
        }
        headers = {"content-type": "application/json; charset=UTF-8"}

        response = requests.request("POST", url, json=payload, headers=headers)
        return response.text
