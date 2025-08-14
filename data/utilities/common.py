from typing import Optional, Tuple
import re
from datetime import datetime
from .translate import translate
from .country import country_to_iso3


def standardize_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%d %B %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s if re.match(r"\d{4}-\d{2}-\d{2}", s) else None


# Class containing ETF Structure, the constructor automatically
# enforces a first normalization
class ETF:
    def __init__(
        self,
        isin: str,
        issuer: str,
        name: Optional[str] = None,
        ticker: Optional[str] = None,
        ter: Optional[float] = None,
        nav: Optional[float] = None,
        size: Optional[float] = None,
        currency: Optional[str] = None,
        asset_class: Optional[str] = None,
        sub_asset_class: Optional[str] = None,
        region: Optional[str] = None,
        use_of_profits: Optional[str] = None,
        replication: Optional[str] = None,
        domicile: Optional[str] = None,
        inception_date: Optional[str] = None,
        url: Optional[str] = None,
    ):
        self.isin = isin
        self.issuer = issuer
        self.name = name
        self.ticker = ticker
        self.ter = None if ter in (None, "-", "—") else float(ter)
        self.nav = None if nav in (None, "-", "—") else float(nav)
        self.size = None if size in (None, "-", "—") else float(size)
        self.currency = currency or None
        self.asset_class = translate(asset_class)
        self.sub_asset_class = translate(sub_asset_class)
        self.region = translate(region)
        self.use_of_profits = translate(use_of_profits)
        self.replication = translate(replication)
        self.domicile = country_to_iso3(domicile)
        self.inception_date = standardize_date(inception_date)
        self.url = url

    def to_db_tuple(self) -> Tuple:
        return (
            self.isin,
            self.issuer,
            self.name,
            self.ticker,
            self.ter,
            self.nav,
            self.size,
            self.currency,
            self.asset_class,
            self.sub_asset_class,
            self.region,
            self.use_of_profits,
            self.replication,
            self.domicile,
            self.inception_date,
            self.url,
        )


# Class modelling holding contained in an ETF
# contructor preforms normalization
class Holding:
    __slots__ = (
        "etf_isin",
        "holding_isin",
        "holding_name",
        "weight",
        "sector",
        "country",
        "currency",
    )

    def __init__(
        self,
        etf_isin: str,
        holding_isin: Optional[str],
        holding_name: Optional[str],
        weight: Optional[float],
        sector: Optional[str],
        country: Optional[str],
        currency: Optional[str],
    ):
        self.etf_isin = etf_isin
        self.holding_isin = holding_isin or None
        self.holding_name = holding_name or None
        self.weight = None if weight in (None, "-", "—") else float(weight)
        self.sector = translate(sector)
        self.country = country_to_iso3(country)
        self.currency = currency or None

        if self.weight and self.weight < 0:
            self.weight = 0
        if self.weight and self.weight > 100:
            self.weight = 100

    def to_db_tuple(self) -> Tuple:
        return (
            self.etf_isin,
            self.holding_isin,
            self.holding_name,
            self.weight,
            self.sector,
            self.country,
            self.currency,
        )
