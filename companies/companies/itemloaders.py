import re
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst, Compose, Identity


def process_percentage(percentage) -> float:
    return float(re.match(r"[-+]?\d+\.\d+", percentage).group())


def extract_title(title) -> str:
    return re.search(r"<title>(.*?) Stock Real Time Quotes |", title).group(1)


def build_weekly(data):
    weekly_prices = []

    i = 0
    for price in data:
        if i % 7 == 0:
            weekly_prices.append(round(sum(price[1:]) / len(price[1:]), 2))
        i += 1
    return weekly_prices


def convert_number_with_unit(number_str):
    multiplier = 1
    number_part = ""
    if re.search(r"Mil", number_str):
        multiplier = 1_000_000
        number_part = number_str.split("M")[0]
    elif re.search(r"Bil", number_str):
        multiplier = 1_000_000_000
        number_part = number_str.split("B")[0]

    # Convert the numerical part to an integer and multiply by the multiplier
    return int(to_num(number_part) * multiplier)


def to_num(string):
    # Replace periods with commas and vice versa
    string = string.replace(".", ",").replace(",", ".")

    # Remove extra decimal points if present, keeping only the last one
    string = string.replace(".", "", string.count(".") - 1)

    # Convert the string to a float
    try:
        return float(string)
    except ValueError:
        raise ValueError(f"Invalid string format: {string}")


def clean_str(string):
    return re.sub(
        "<.*?>",
        "",
        string.strip().replace('"', "").replace("à", "a").replace("ò", "o"),
    )


class CompanyLoader(ItemLoader):
    name_in = Compose(lambda x: x[0], extract_title)
    P1M_in = MapCompose(str.strip, process_percentage)
    P6M_in = MapCompose(str.strip, process_percentage)
    P1Y_in = MapCompose(str.strip, process_percentage)
    isin_in = MapCompose(str.strip)
    ticker_in = MapCompose(str.strip)
    weekly_prices_in = build_weekly
    market_cap_in = Compose(lambda x: x[0], convert_number_with_unit)
    p_e_in = Compose(lambda x: x[0], to_num)
    div_yield_in = Compose(lambda x: x[0], to_num)
    sector_in = Compose(lambda x: x[0], clean_str)
    sub_sector_in = Compose(lambda x: x[0], clean_str)

    name_out = TakeFirst()
    P1M_out = TakeFirst()
    P6M_out = TakeFirst()
    P1Y_out = TakeFirst()
    isin_out = TakeFirst()
    ticker_out = TakeFirst()
    weekly_prices_out = Identity()
    market_cap_out = TakeFirst()
    p_e_out = TakeFirst()
    div_yield_out = TakeFirst()
    sector_out = TakeFirst()
    sub_sector_out = TakeFirst()


class ETFLoader(ItemLoader):
    name_in = Identity()
    isin_in = Identity()
    ticker_in = Identity()
    wkn_in = Identity()
    fund_currency_in = clean_str
    ter_in = MapCompose(str.strip, process_percentage)
    distribution_policy_in = clean_str
    replication_method_in = clean_str
    fund_size_in = to_num
    number_of_holdings_in = to_num
    top_countries_in = MapCompose(
        lambda el: {
            "country": clean_str(el["country"]),
            "weight": process_percentage(el["weight"]),
        }
    )
    top_sectors_in = MapCompose(
        lambda el: {
            "sector": clean_str(el["sector"]),
            "weight": process_percentage(el["weight"]),
        }
    )
    url_in = Identity()

    name_out = TakeFirst()
    isin_out = TakeFirst()
    ticker_out = TakeFirst()
    wkn_out = TakeFirst()
    fund_currency_out = TakeFirst()
    ter_out = TakeFirst()
    distribution_policy_out = TakeFirst()
    replication_method_out = TakeFirst()
    fund_size_out = TakeFirst()
    number_of_holdings_out = TakeFirst()
    top_countries_out = Identity()
    top_sectors_out = Identity()
    url_out = TakeFirst()
