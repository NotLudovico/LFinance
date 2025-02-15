import time
import re
import json
from scrapy import Request
import requests
import scrapy
from companies.items import ETFItem
from companies.itemloaders import ETFLoader


def camel_to_snake(name):
    # Insert underscores before uppercase letters and convert them to lowercase
    s = re.sub(r"([A-Z])", r"_\1", name).lower()
    # Handle the case where the string starts with an underscore
    if s.startswith("_"):
        s = s[1:]
    return s


class JustetfSpider(scrapy.Spider):
    name = "justetf"
    custom_settings = {"DOWNLOAD_DELAY": 1, "CONCURRENT_REQUESTS_PER_DOMAIN": 3}

    def start_requests(self):
        items = get_etf_list()
        urls = []
        for item in items:
            urls.append(
                Request(
                    f"https://www.justetf.com/en/etf-profile.html?isin={item["isin"]}",
                    callback=self.parse_item,
                    cb_kwargs={"item": item},
                )
            )
        return urls

    def parse_item(self, response, item):
        etf = ETFLoader(item=ETFItem(), selector=response)

        for k, v in item.items():
            try:
                etf.add_value(camel_to_snake(k), v)
                if camel_to_snake(k) == "isin":
                    etf.add_value(
                        "url", f"https://www.justetf.com/en/etf-profile.html?isin={v}"
                    )
            except:
                pass

        # Get top countries
        try:
            countries = list(
                filter(
                    lambda x: x != " ",
                    response.xpath(
                        "//h3[contains(text(), 'Countries')]/following-sibling::table/tbody//td/text()"
                    ).getall(),
                )
            )
            weights = list(
                filter(
                    lambda x: x != " ",
                    response.xpath(
                        "//h3[contains(text(), 'Countries')]/following-sibling::table/tbody//span/text()"
                    ).getall(),
                )
            )
            for i in range(0, 3):
                etf.add_value(
                    "top_countries", {"country": countries[i], "weight": weights[i]}
                )
        except:
            pass

        # Get top sectors
        try:
            sectors = list(
                filter(
                    lambda x: x != " ",
                    response.xpath(
                        "//h3[contains(text(), 'Sectors')]/following-sibling::table/tbody//td/text()"
                    ).getall(),
                )
            )
            weights = list(
                filter(
                    lambda x: x != " ",
                    response.xpath(
                        "//h3[contains(text(), 'Sectors')]/following-sibling::table/tbody//span/text()"
                    ).getall(),
                )
            )
            for i in range(0, 3):
                etf.add_value(
                    "top_sectors", {"sector": sectors[i], "weight": weights[i]}
                )
        except:
            pass

        return etf.load_item()


def get_etf_list():
    url = "https://www.justetf.com/en/search.html"

    querystring = {
        "5-1.0-container-tabsContentContainer-tabsContentRepeater-1-container-content-etfsTablePanel": "",
        "search": "ETFS",
        "_wicket": "1",
        "": "",
    }

    payload = "draw=1&columns%5B0%5D%5Bdata%5D=&columns%5B0%5D%5Bname%5D=selectCheckbox&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=name&columns%5B1%5D%5Bname%5D=name&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=&columns%5B2%5D%5Bname%5D=sparkline&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=fundCurrency&columns%5B3%5D%5Bname%5D=fundCurrency&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=fundSize&columns%5B4%5D%5Bname%5D=fundSize&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=ter&columns%5B5%5D%5Bname%5D=ter&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=&columns%5B6%5D%5Bname%5D=bullet&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=weekReturnCUR&columns%5B7%5D%5Bname%5D=weekReturn&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=monthReturnCUR&columns%5B8%5D%5Bname%5D=monthReturn&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=threeMonthReturnCUR&columns%5B9%5D%5Bname%5D=threeMonthReturn&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=sixMonthReturnCUR&columns%5B10%5D%5Bname%5D=sixMonthReturn&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=true&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Bdata%5D=yearReturnCUR&columns%5B11%5D%5Bname%5D=yearReturn&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=true&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Bdata%5D=threeYearReturnCUR&columns%5B12%5D%5Bname%5D=threeYearReturn&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=true&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Bdata%5D=fiveYearReturnCUR&columns%5B13%5D%5Bname%5D=fiveYearReturn&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=true&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B14%5D%5Bdata%5D=ytdReturnCUR&columns%5B14%5D%5Bname%5D=ytdReturn&columns%5B14%5D%5Bsearchable%5D=true&columns%5B14%5D%5Borderable%5D=true&columns%5B14%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B14%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B15%5D%5Bdata%5D=yearReturn1CUR&columns%5B15%5D%5Bname%5D=yearReturn1&columns%5B15%5D%5Bsearchable%5D=true&columns%5B15%5D%5Borderable%5D=true&columns%5B15%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B15%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B16%5D%5Bdata%5D=yearReturn2CUR&columns%5B16%5D%5Bname%5D=yearReturn2&columns%5B16%5D%5Bsearchable%5D=true&columns%5B16%5D%5Borderable%5D=true&columns%5B16%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B16%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B17%5D%5Bdata%5D=yearReturn3CUR&columns%5B17%5D%5Bname%5D=yearReturn3&columns%5B17%5D%5Bsearchable%5D=true&columns%5B17%5D%5Borderable%5D=true&columns%5B17%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B17%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B18%5D%5Bdata%5D=yearReturn4CUR&columns%5B18%5D%5Bname%5D=yearReturn4&columns%5B18%5D%5Bsearchable%5D=true&columns%5B18%5D%5Borderable%5D=true&columns%5B18%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B18%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B19%5D%5Bdata%5D=yearVolatilityCUR&columns%5B19%5D%5Bname%5D=yearVolatility&columns%5B19%5D%5Bsearchable%5D=true&columns%5B19%5D%5Borderable%5D=true&columns%5B19%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B19%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B20%5D%5Bdata%5D=threeYearVolatilityCUR&columns%5B20%5D%5Bname%5D=threeYearVolatility&columns%5B20%5D%5Bsearchable%5D=true&columns%5B20%5D%5Borderable%5D=true&columns%5B20%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B20%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B21%5D%5Bdata%5D=fiveYearVolatilityCUR&columns%5B21%5D%5Bname%5D=fiveYearVolatility&columns%5B21%5D%5Bsearchable%5D=true&columns%5B21%5D%5Borderable%5D=true&columns%5B21%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B21%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B22%5D%5Bdata%5D=yearReturnPerRiskCUR&columns%5B22%5D%5Bname%5D=yearReturnPerRisk&columns%5B22%5D%5Bsearchable%5D=true&columns%5B22%5D%5Borderable%5D=true&columns%5B22%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B22%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B23%5D%5Bdata%5D=threeYearReturnPerRiskCUR&columns%5B23%5D%5Bname%5D=threeYearReturnPerRisk&columns%5B23%5D%5Bsearchable%5D=true&columns%5B23%5D%5Borderable%5D=true&columns%5B23%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B23%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B24%5D%5Bdata%5D=fiveYearReturnPerRiskCUR&columns%5B24%5D%5Bname%5D=fiveYearReturnPerRisk&columns%5B24%5D%5Bsearchable%5D=true&columns%5B24%5D%5Borderable%5D=true&columns%5B24%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B24%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B25%5D%5Bdata%5D=yearMaxDrawdownCUR&columns%5B25%5D%5Bname%5D=yearMaxDrawdown&columns%5B25%5D%5Bsearchable%5D=true&columns%5B25%5D%5Borderable%5D=true&columns%5B25%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B25%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B26%5D%5Bdata%5D=threeYearMaxDrawdownCUR&columns%5B26%5D%5Bname%5D=threeYearMaxDrawdown&columns%5B26%5D%5Bsearchable%5D=true&columns%5B26%5D%5Borderable%5D=true&columns%5B26%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B26%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B27%5D%5Bdata%5D=fiveYearMaxDrawdownCUR&columns%5B27%5D%5Bname%5D=fiveYearMaxDrawdown&columns%5B27%5D%5Bsearchable%5D=true&columns%5B27%5D%5Borderable%5D=true&columns%5B27%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B27%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B28%5D%5Bdata%5D=maxDrawdownCUR&columns%5B28%5D%5Bname%5D=maxDrawdown&columns%5B28%5D%5Bsearchable%5D=true&columns%5B28%5D%5Borderable%5D=true&columns%5B28%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B28%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B29%5D%5Bdata%5D=inceptionDate&columns%5B29%5D%5Bname%5D=inceptionDate&columns%5B29%5D%5Bsearchable%5D=true&columns%5B29%5D%5Borderable%5D=true&columns%5B29%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B29%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B30%5D%5Bdata%5D=distributionPolicy&columns%5B30%5D%5Bname%5D=distributionPolicy&columns%5B30%5D%5Bsearchable%5D=true&columns%5B30%5D%5Borderable%5D=false&columns%5B30%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B30%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B31%5D%5Bdata%5D=sustainable&columns%5B31%5D%5Bname%5D=sustainable&columns%5B31%5D%5Bsearchable%5D=true&columns%5B31%5D%5Borderable%5D=true&columns%5B31%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B31%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B32%5D%5Bdata%5D=numberOfHoldings&columns%5B32%5D%5Bname%5D=numberOfHoldings&columns%5B32%5D%5Bsearchable%5D=true&columns%5B32%5D%5Borderable%5D=true&columns%5B32%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B32%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B33%5D%5Bdata%5D=currentDividendYield&columns%5B33%5D%5Bname%5D=currentDividendYield&columns%5B33%5D%5Bsearchable%5D=true&columns%5B33%5D%5Borderable%5D=true&columns%5B33%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B33%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B34%5D%5Bdata%5D=yearDividendYield&columns%5B34%5D%5Bname%5D=yearDividendYield&columns%5B34%5D%5Bsearchable%5D=true&columns%5B34%5D%5Borderable%5D=true&columns%5B34%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B34%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B35%5D%5Bdata%5D=domicileCountry&columns%5B35%5D%5Bname%5D=domicileCountry&columns%5B35%5D%5Bsearchable%5D=true&columns%5B35%5D%5Borderable%5D=false&columns%5B35%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B35%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B36%5D%5Bdata%5D=replicationMethod&columns%5B36%5D%5Bname%5D=replicationMethod&columns%5B36%5D%5Bsearchable%5D=true&columns%5B36%5D%5Borderable%5D=false&columns%5B36%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B36%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B37%5D%5Bdata%5D=savingsPlanReady&columns%5B37%5D%5Bname%5D=savingsPlanReady&columns%5B37%5D%5Bsearchable%5D=true&columns%5B37%5D%5Borderable%5D=false&columns%5B37%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B37%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B38%5D%5Bdata%5D=hasSecuritiesLending&columns%5B38%5D%5Bname%5D=hasSecuritiesLending&columns%5B38%5D%5Bsearchable%5D=true&columns%5B38%5D%5Borderable%5D=false&columns%5B38%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B38%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B39%5D%5Bdata%5D=isin&columns%5B39%5D%5Bname%5D=isin&columns%5B39%5D%5Bsearchable%5D=true&columns%5B39%5D%5Borderable%5D=false&columns%5B39%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B39%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B40%5D%5Bdata%5D=ticker&columns%5B40%5D%5Bname%5D=ticker&columns%5B40%5D%5Bsearchable%5D=true&columns%5B40%5D%5Borderable%5D=false&columns%5B40%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B40%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B41%5D%5Bdata%5D=wkn&columns%5B41%5D%5Bname%5D=wkn&columns%5B41%5D%5Bsearchable%5D=true&columns%5B41%5D%5Borderable%5D=false&columns%5B41%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B41%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B42%5D%5Bdata%5D=valorNumber&columns%5B42%5D%5Bname%5D=valorNumber&columns%5B42%5D%5Bsearchable%5D=true&columns%5B42%5D%5Borderable%5D=false&columns%5B42%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B42%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B43%5D%5Bdata%5D=&columns%5B43%5D%5Bname%5D=addButton&columns%5B43%5D%5Bsearchable%5D=true&columns%5B43%5D%5Borderable%5D=false&columns%5B43%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B43%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=4&order%5B0%5D%5Bdir%5D=desc&start=0&length=3000&search%5Bvalue%5D=&search%5Bregex%5D=false&ajaxsortOrder=desc&ajaxsortField=fundSize&lang=en&country=DE&defaultCurrency=EUR&universeType=private&etfsParams=search%3DETFS%26query%3D"
    headers = {
        "cookie": "priceAlertdontShowAgain=false; locale_=en; CookieConsent={stamp:%275zVtWwEN49W0RU6I/VIIQodzHMolnmPjCeZx7PzHCacdSem/a6/52w==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:false%2Cmarketing:false%2Cmethod:%27explicit%27%2Cver:5%2Cutc:1726496759706%2Cregion:%27it%27}; _vwo_uuid_v2=D68B2C6E136DD0405231EA389861BF69D|67f88bd4243030dc02c2321570836622; _vwo_uuid=D68B2C6E136DD0405231EA389861BF69D; _vwo_ds=3%241726496762%3A26.96257159%3A%3A; etfs-search-order=fundSize%2Cdesc; etfprofile-feedback-v2=0; fullscreen-search-layout=1; XSRF-TOKEN=1e9faaf1-58cc-4844-b1f2-90cf5a99fcde; JSESSIONID=4EF84D1B58183F1EB74CF8CEE1A3A62A; _vis_opt_s=7%7C; _vis_opt_test_cookie=1; _vwo_sn=663408%3A6%3A%3A%3A1; AWSALB=G04QFKFVOxbzGpchSa/C/q069PgVZqiGxRO+Ng1TGbv/k8xUfNszA6hGG7zRb7iZ7rAsWCsg3WnxGXu6/k5PLxuFe7kKMGl0YKt/oWpD74ShQgm4gCciuOzjOtKY; AWSALBCORS=G04QFKFVOxbzGpchSa/C/q069PgVZqiGxRO+Ng1TGbv/k8xUfNszA6hGG7zRb7iZ7rAsWCsg3WnxGXu6/k5PLxuFe7kKMGl0YKt/oWpD74ShQgm4gCciuOzjOtKY",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "dnt": "1",
        "origin": "https://www.justetf.com",
        "priority": "u=1, i",
        "referer": "https://www.justetf.com/en/search.html?search=ETFS",
        "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    i = 0
    while True:
        try:
            return json.loads(
                requests.request(
                    "POST", url, data=payload, headers=headers, params=querystring
                ).text
            )["data"]
        except:
            if i == 10:
                break
            i += 1
            pass

    with open("justetf_req.json") as reader:
        return json.loads(reader)
