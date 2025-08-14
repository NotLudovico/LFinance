import json
import asyncio
import aiohttp
import polars as pl
import sqlite3
import io
import re
import requests
from typing import Any, Dict, List, Tuple

from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio

# Assuming these utilities are in a 'utilities' directory
from utilities.database import setup_database
from utilities.country import country_to_iso3
from utilities.translate import translate

# --- Configuration ---
DB_NAME = "database.db"
CONCURRENT_REQUESTS = 10  # Limit the number of concurrent requests


def parse_amount(s: str) -> float:
    """
    Convert strings like '€803,44 M' into a number (e.g. 803440000.0).
    Supports suffixes: K (thousand), M (million), B (billion).
    """
    # 1. Strip whitespace and currency symbols
    s = s.strip()
    s = re.sub(r"[€$£]", "", s)

    # 2. Extract the multiplier suffix (if any)
    match = re.match(r"([\d.,]+)\s*([KMB])?", s, re.IGNORECASE)
    if not match:
        raise ValueError(f"Unrecognized format: {s}")
    number_str, suffix = match.groups()

    # 3. Normalize decimal comma to point, remove thousands separators
    number_str = number_str.replace(".", "").replace(",", ".")
    value = float(number_str)

    # 4. Apply suffix multiplier
    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9}
    if suffix:
        value *= multipliers[suffix.upper()]

    return value


# --- Helper Function to Process a Single ETF ---
async def fetch_and_process_etf(session, semaphore, etf_details):
    """
    Asynchronously fetches and processes data for a single ETF.
    It acquires a semaphore to limit concurrency.
    """
    async with semaphore:
        try:
            # 1. Get ETF's main page to find ISIN, TER and holdings link
            etf_url = "https://www.ssga.com" + etf_details["url"]
            async with session.get(etf_url) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), "html.parser")

            isin_label = soup.find("td", string=re.compile(r"\s*ISIN\s*"))
            if isin_label:
                etf_details["isin"] = isin_label.find_next_sibling("td").get_text(
                    strip=True
                )
            else:
                # Handle cases where ISIN is not found
                print(f"Warning: ISIN not found for {etf_details.get('url')}")
                return etf_details  # Exit early if no ISIN

            # 3. Extract TER from the page ## FIXED
            ter_label = soup.find("td", string=re.compile(r"\s*TER\s*"))
            if ter_label and (ter_value_tag := ter_label.find_next_sibling("td")):
                ter_text = ter_value_tag.get_text(strip=True)
                if ter_text and ter_text != "-":
                    etf_details["ter"] = float(
                        ter_text.replace("%", "").replace(",", ".")
                    )

            aum_label = soup.find(
                "div", string=re.compile(r"\s*Asset Totali del  Fondo EUR\s*")
            )
            if aum_label and (aum_value_tag := aum_label.find_next_sibling("div")):
                aum_text = aum_value_tag.get_text(strip=True)
                if aum_text:
                    etf_details["size"] = float(parse_amount(aum_text))

            curr_label = soup.find(
                "div", string=re.compile(r"\s*Valuta della classe di azioni\s*")
            )
            if curr_label and (curr_value_tag := curr_label.find_next_sibling("div")):
                curr_text = curr_value_tag.get_text(strip=True)
                if curr_text:
                    etf_details["currency"] = curr_text

            domicile_label = soup.find("td", string=re.compile(r"\s*Domicilio\s*"))
            if domicile_label and (
                domicile_value_tag := domicile_label.find_next_sibling("td")
            ):
                domicile_text = domicile_value_tag.get_text(strip=True)
                if domicile_text:
                    etf_details["domicile"] = country_to_iso3(domicile_text)

            replication_label = soup.find(
                "td", string=re.compile(r"\s*Metodologia di Replica\s*")
            )
            if replication_label and (
                replication_value_tag := replication_label.find_next_sibling("td")
            ):
                replication_text = replication_value_tag.get_text(strip=True)
                if replication_text:
                    etf_details["replication"] = translate(replication_text)

            # 4. Find and download holdings XLSX file
            link_tag = soup.find("a", string="Scarica le posizioni giornaliere")
            if link_tag and (link := link_tag.get("href")):
                holdings_url = "https://www.ssga.com" + link
                async with session.get(holdings_url) as holdings_response:
                    holdings_response.raise_for_status()
                    excel_content = await holdings_response.read()

                # 5. Process holdings data with Polars
                holdings = pl.read_excel(
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

                if "Currency Local" in holdings.columns:
                    holdings = holdings.rename({"Currency Local": "currency"})
                else:
                    holdings = holdings.rename(
                        {"Currency": "currency", "Trade Country Name": "country"}
                    )

                # Handle different column names for bond vs equity funds
                if "Maturity Date" in holdings.columns:
                    holdings = (
                        holdings.rename({"Country of Issue": "country"})
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
                    holdings = holdings.rename({"Sector Classification": "sector"})

                # 6. Clean and format holdings data
                final_holdings = []
                for holding in holdings.to_dicts():
                    # Skip rows with invalid or placeholder ISINs
                    if (
                        not holding.get("isin")
                        or holding["isin"] == "-"
                        or len(holding.get("isin", "")) > 12
                    ):
                        continue

                    if (
                        "weight" in holding
                        and holding["weight"] != "-"
                        and holding["weight"] is not None
                    ):
                        holding["weight"] = float(holding["weight"])
                    else:
                        holding["weight"] = 0.0

                    if "country" in holding and holding["country"]:
                        holding["country"] = country_to_iso3(holding["country"])

                    if "sector" in holding and holding["sector"]:
                        holding["sector"] = translate(holding["sector"])

                    final_holdings.append(holding)

                etf_details["holdings"] = final_holdings

            return etf_details

        except (aiohttp.ClientError, Exception) as e:
            print(f"Error processing {etf_details.get('ticker')}: {e}")
            # Return original details without holdings on error
            return etf_details


# --- Main Asynchronous Execution Logic ---
async def main():
    """
    Main function to orchestrate the fetching, processing, and saving of all ETFs.
    """
    # Initial synchronous request to get the list of ETFs
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
    )
    response.raise_for_status()
    etf_list = response.json()["data"]["funds"]["etfs"]["datas"]

    # Initial cleaning of the ETF list
    cleaned = []
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

    # --- Asynchronous Processing ---
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_process_etf(session, semaphore, etf) for etf in cleaned]
        results = await tqdm_asyncio.gather(*tasks, desc="Processing ETFs")

    # --- Prepare Data for Database Insertion ---
    etfs_to_insert = []
    all_holdings_to_insert = []

    for etf_data in results:
        # Skip ETFs where we failed to get an ISIN
        if "isin" not in etf_data:
            print(f"Skipping {etf_data.get('name')} due to missing ISIN.")
            continue

        etf_record = etf_data.copy()
        holdings = etf_record.pop("holdings", [])

        # Ensure url is not None for database insertion
        etf_record["url"] = etf_record.get("url", "")

        etfs_to_insert.append(etf_record)

        for h in holdings:
            all_holdings_to_insert.append(
                (
                    etf_data["isin"],  # etf_isin
                    h.get("isin"),  # holding_isin
                    h.get("holding_name"),  # holding_name
                    h.get("weight"),  # weight
                    h.get("sector"),  # sector
                    h.get("country"),  # country
                    h.get("currency"),  # currency
                )
            )

    # --- Batch insert all collected data into the database ---
    if not etfs_to_insert:
        print("\nNo ETF data to insert. Exiting.")
        return

    print("\nConnecting to database and inserting data...")
    conn = sqlite3.connect(DB_NAME)
    setup_database(conn)
    cursor = conn.cursor()

    try:
        # Batch insert ETFs
        etf_cols = list(etfs_to_insert[0].keys())
        etf_placeholders = ", ".join("?" for _ in etf_cols)
        etf_values = [tuple(p.get(col) for col in etf_cols) for p in etfs_to_insert]

        cursor.executemany(
            f"INSERT OR REPLACE INTO etfs ({', '.join(etf_cols)}) VALUES ({etf_placeholders})",
            etf_values,
        )
        print(f"Inserted or replaced {cursor.rowcount} ETFs.")

        # Batch insert holdings
        if all_holdings_to_insert:
            # First, clear existing holdings for the ETFs we are updating
            updated_etf_isins = {f"'{p['isin']}'" for p in etfs_to_insert}
            cursor.execute(
                f"DELETE FROM etf_holdings WHERE etf_isin IN ({','.join(updated_etf_isins)})"
            )

            cursor.executemany(
                "INSERT INTO etf_holdings (etf_isin, holding_isin, holding_name, weight, sector, country, currency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                all_holdings_to_insert,
            )
            print(f"Inserted {cursor.rowcount} holdings.")

        conn.commit()
        print("\nProcessing complete. Data saved to database.")
    except sqlite3.Error as e:
        print(f"\nDatabase error: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
