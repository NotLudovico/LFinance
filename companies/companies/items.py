# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class Company(Item):
    name = Field()
    P1M = Field()
    P6M = Field()
    P1Y = Field()
    isin = Field()
    ticker = Field()
    weekly_prices = Field()
    market_cap = Field()
    p_e = Field()
    div_yield = Field()
    isin = Field()
    sector = Field()
    sub_sector = Field()


class ETFItem(Item):
    isin = Field()
    name = Field()
    ticker = Field()
    wkn = Field()
    fund_currency = Field()
    ter = Field()
    distribution_policy = Field()
    replication = Field()
    fund_size = Field()
    numer_of_holdings = Field()
    top_countries = Field()
    top_sectors = Field()
    replication_method = Field()
    week_return_cur = Field()
    url = Field()
