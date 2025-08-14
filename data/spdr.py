import asyncio
import io
import json
import re
from typing import Any, Dict, List

import aiohttp
import polars as pl
import requests
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio

# DB utilities (normalized schema + helpers)
from utilities.database import (
    open_db,
    setup_database,
    upsert_etf,
    upsert_security,
    upsert_holding,
)

# Shared normalization models
from utilities.common import ETF, Holding


# --- Configuration ---
DB_NAME = "database.db"
CONCURRENT_REQUESTS = 10  # Limit the number of concurrent HTTP requests


def parse_amount(s: str) -> float:
    """
    Convert strings like '€803,44 M' into a float number of units (e.g. 803_440_000.0).
    Supports suffixes: K (thousand), M (million), B (billion).
    """
    s = s.strip()
    s = re.sub(r"[€$£]", "", s)

    match = re.match(r"([\d.,]+)\s*([KMB])?", s, re.IGNORECASE)
    if not match:
        raise ValueError(f"Unrecognized format: {s}")
    number_str, suffix = match.groups()

    number_str = number_str.replace(".", "").replace(",", ".")
    value = float(number_str)

    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9}
    if suffix:
        value *= multipliers[suffix.upper()]

    return value


async def fetch_and_process_etf(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    etf_details: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fetch and parse a single SPDR ETF:
      - read main page for ISIN / TER / AUM / currency / domicile / replication
      - fetch the XLSX of holdings and parse it into a normalized list (dicts)
    """
    async with semaphore:
        try:
            etf_url = "https://www.ssga.com" + etf_details["url"]
            async with session.get(etf_url) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), "html.parser")

            # ISIN
            isin_label = soup.find("td", string=re.compile(r"\s*ISIN\s*"))
            if isin_label:
                etf_details["isin"] = isin_label.find_next_sibling("td").get_text(
                    strip=True
                )
            else:
                print(f"Warning: ISIN not found for {etf_details.get('url')}")
                return etf_details  # Exit early if no ISIN

            # TER
            ter_label = soup.find("td", string=re.compile(r"\s*TER\s*"))
            if ter_label and (ter_value_tag := ter_label.find_next_sibling("td")):
                ter_text = ter_value_tag.get_text(strip=True)
                if ter_text and ter_text != "-":
                    etf_details["ter"] = float(
                        ter_text.replace("%", "").replace(",", ".")
                    )

            # AUM (size)
            aum_label = soup.find(
                "div", string=re.compile(r"\s*Asset Totali del  Fondo EUR\s*")
            )
            if aum_label and (aum_value_tag := aum_label.find_next_sibling("div")):
                aum_text = aum_value_tag.get_text(strip=True)
                if aum_text:
                    etf_details["size"] = float(parse_amount(aum_text))

            # Currency
            curr_label = soup.find(
                "div", string=re.compile(r"\s*Valuta della classe di azioni\s*")
            )
            if curr_label and (curr_value_tag := curr_label.find_next_sibling("div")):
                curr_text = curr_value_tag.get_text(strip=True)
                if curr_text:
                    etf_details["currency"] = curr_text

            # Domicile
            domicile_label = soup.find("td", string=re.compile(r"\s*Domicilio\s*"))
            if domicile_label and (
                domicile_value_tag := domicile_label.find_next_sibling("td")
            ):
                domicile_text = domicile_value_tag.get_text(strip=True)
                if domicile_text:
                    etf_details["domicile"] = domicile_text  # normalized later by ETF

            # Replication
            replication_label = soup.find(
                "td", string=re.compile(r"\s*Metodologia di Replica\s*")
            )
            if replication_label and (
                replication_value_tag := replication_label.find_next_sibling("td")
            ):
                replication_text = replication_value_tag.get_text(strip=True)
                if replication_text:
                    etf_details["replication"] = (
                        replication_text  # normalized later by ETF
                    )

            # Holdings XLSX
            link_tag = soup.find("a", string="Scarica le posizioni giornaliere")
            if link_tag and (link := link_tag.get("href")):
                holdings_url = "https://www.ssga.com" + link
                async with session.get(holdings_url) as holdings_response:
                    holdings_response.raise_for_status()
                    excel_content = await holdings_response.read()

                # Parse 'holdings' sheet; SPDR files tend to have headers starting on row 6 (0-based 5)
                df = pl.read_excel(
                    io.BytesIO(excel_content),
                    engine="calamine",
                    sheet_name="holdings",
                    read_options={"header_row": 5},
                ).rename(
                    {
                        "ISIN": "isin",
                        "Security Name": "holding_name",
                        "Percent of Fund": "weight",
                    }
                )

                # Column variations
                if "Currency Local" in df.columns:
                    df = df.rename({"Currency Local": "currency"})
                else:
                    df = df.rename(
                        {"Currency": "currency", "Trade Country Name": "country"}
                    )

                # Bond vs equity layout differences
                if "Maturity Date" in df.columns:
                    df = (
                        df.rename({"Country of Issue": "country"})
                        .drop(
                            "Maturity Date",
                            "Interest Rate",
                            "Base Market Value",
                            "Local Price",
                            "PAR Value Local",
                            "SEDOL",
                        )
                        .with_columns(pl.lit("bond").alias("sector"))
                    )
                else:
                    df = df.rename({"Sector Classification": "sector"})

                # Build list of clean dicts (minimal pre-normalization)
                final_holdings: List[Dict[str, Any]] = []
                for row in df.to_dicts():
                    isin = row.get("isin")
                    if not isin or isin == "-" or len(str(isin)) > 12:
                        continue

                    weight = row.get("weight")
                    if weight not in (None, "-"):
                        try:
                            row["weight"] = float(weight)
                        except Exception:
                            row["weight"] = None
                    else:
                        row["weight"] = None

                    final_holdings.append(row)

                etf_details["holdings"] = final_holdings

            return etf_details

        except (aiohttp.ClientError, Exception) as e:
            print(f"Error processing {etf_details.get('ticker')}: {e}")
            return etf_details


async def main():
    """
    Orchestrates:
      1) Fetch list of SPDR ETFs (IT locale)
      2) Concurrently scrape each ETF page + holdings file
      3) Normalize into ETF/Holding models
      4) Upsert into normalized DB (etfs, securities, etf_holdings)
    """
    # 1) Initial list
    response = requests.get(
        "https://www.ssga.com/bin/v1/ssmp/fund/fundfinder",
        params={
            "country": "it",
            "language": "it",
            "role": "intermediary",
            "product": "",
            "ui": "fund-finder",
        },
        headers={"accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    etf_list = response.json()["data"]["funds"]["etfs"]["datas"]

    # Clean minimal fields needed before per-ETF fetch
    cleaned: List[Dict[str, Any]] = []
    for etf in etf_list:
        ter = None
        if "perfIndex" in etf and etf["perfIndex"][0]["ter"] != "-":
            ter = etf["perfIndex"][0]["ter"]

        cleaned.append(
            {
                "issuer": "spdr",
                "name": etf["fundName"],
                "ticker": etf["fundTicker"].split(" ")[0],
                "url": etf["fundUri"],
                "inception_date": "/".join((etf["inceptionDate"][1]).split("-")[::-1]),
                "use_of_profits": "dist" if "Dist" in etf["fundName"] else "acc",
                "ter": ter,
                "holdings": [],
            }
        )

    # 2) Concurrent scraping
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_process_etf(session, semaphore, etf) for etf in cleaned]
        results = await tqdm_asyncio.gather(*tasks, desc="Processing ETFs")

    # 3) Normalize via shared models
    etf_tuples: List[tuple] = []
    holdings_tuples: List[tuple] = []
    isins_to_update: List[str] = []

    for etf_data in results:
        isin = etf_data.get("isin")
        if not isin:
            print(f"Skipping {etf_data.get('name')} due to missing ISIN.")
            continue

        etf_obj = ETF(
            isin=isin,
            issuer=etf_data["issuer"],
            name=etf_data.get("name"),
            ticker=etf_data.get("ticker"),
            ter=etf_data.get("ter"),
            nav=None,
            size=etf_data.get("size"),
            currency=etf_data.get("currency"),
            asset_class=None,
            sub_asset_class=None,
            region=None,
            use_of_profits=etf_data.get("use_of_profits"),
            replication=etf_data.get("replication"),
            domicile=etf_data.get("domicile"),
            inception_date=etf_data.get("inception_date"),
            url=etf_data.get("url") or "",
        )
        etf_tuples.append(etf_obj.to_db_tuple())
        isins_to_update.append(isin)

        for h in etf_data.get("holdings", []):
            h_obj = Holding(
                etf_isin=isin,
                holding_isin=h.get("isin"),
                holding_name=h.get("holding_name"),
                weight=h.get("weight"),
                sector=h.get("sector"),
                country=h.get("country"),
                currency=h.get("currency"),
            )
            holdings_tuples.append(h_obj.to_db_tuple())

    if not etf_tuples:
        print("\nNo ETF data to insert. Exiting.")
        return

    # 4) Persist into normalized DB
    print("\nWriting to database...")
    with open_db(DB_NAME) as conn:
        # Recreate schema (destructive, consistent with other scrapers)
        setup_database(conn)

        # Upsert ETFs
        print("Upserting ETFs...")
        for tup in etf_tuples:
            upsert_etf(conn, tup)

        # Clear previous holdings for these ETFs to avoid stale rows
        if isins_to_update:
            print("Clearing old holdings...")
            placeholders = ", ".join("?" for _ in isins_to_update)
            conn.execute(
                f"DELETE FROM etf_holdings WHERE etf_isin IN ({placeholders})",
                isins_to_update,
            )

        print("Upserting securities and holdings...")
        for (
            etf_isin,
            holding_isin,
            holding_name,
            weight,
            sector,
            country,
            currency,
        ) in holdings_tuples:
            # Skip obviously bad ISINs; ensure a usable name
            if holding_isin and len(holding_isin) != 12:
                continue

            if weight is None or weight < 0:
                weight = 0.0

            name = (
                holding_name
                or (sector == "cash" and "CASH")
                or holding_isin
                or "UNKNOWN"
            )

            sec_id = upsert_security(
                conn,
                isin=holding_isin,
                name=str(name),
                sector=sector,
                country=country,
                currency=currency,
            )
            upsert_holding(conn, etf_isin=etf_isin, security_id=sec_id, weight=weight)

        print("Rebuilding search index...")

    print("✅ SPDR scraping complete. Database is up to date.")


if __name__ == "__main__":
    asyncio.run(main())
