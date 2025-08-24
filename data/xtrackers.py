import asyncio
import io
from typing import Any, Dict, List, Tuple

import aiohttp
import polars as pl
import pandas as pd
from tqdm import tqdm

# Shared models
from utilities.common import ETF, Holding

# DB helpers (normalized schema)
from utilities.database import (
    open_db,
    setup_database,
    upsert_etf,
    upsert_security,
    upsert_holding,
)

# --- Configuration ---
DB_NAME = "database.db"
CONCURRENT_REQUESTS = 1
ALL_PRODUCTS_XLSX = "AllProductData.xlsx"
REQUEST_DELAY = 0.1  # seconds between requests


# ---------- ETF list (local XLSX) ----------
def get_etf_list() -> List[ETF]:
    """
    Read the local AllProductData.xlsx exported from Xtrackers (IT),
    normalize into ETF objects via the shared model.
    """
    df = pd.read_excel(ALL_PRODUCTS_XLSX, skiprows=6).dropna(subset=["ISIN"])

    etfs = [
        ETF(
            isin=row["ISIN"],
            issuer="xtrackers",
            name=row.get("Nome"),
            ticker=None,
            ter=row.get("TER annuale (%)"),
            nav=None,
            size=row.get("Assets totali (EUR)"),
            currency=row.get("Valuta"),
            asset_class=row.get("Classe di investimento"),
            sub_asset_class=None,
            region=None,
            use_of_profits=row.get("Utilizzo dividendi"),
            replication=None,
            domicile=None,
            inception_date=row.get("Lancio del comparto del fondo"),
            url=None,
        )
        for _, row in df.iterrows()
    ]
    return etfs


# ---------- Holdings (per-ISIN, concurrent over HTTP) ----------
async def fetch_holdings_for_isin(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, isin: str
) -> Dict[str, Any]:
    """
    Fetch and parse the per-ISIN holdings XLSX export from Xtrackers (DWS).
    Returns a dict: {"isin": <etf_isin>, "holdings": [ {isin, holding_name, weight, sector, country, currency}, ... ]}
    """
    url = (
        f"https://etf.dws.com/etfdata/export/ITA/ITA/excel/product/constituent/{isin}/"
    )

    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.read()

            # File has headers after 3 skipped rows -> header_row=3 (0-based)
            df = pl.read_excel(
                io.BytesIO(content),
                engine="calamine",
                read_options={"header_row": 3},
            ).rename(
                {
                    "Weighting": "weight",
                    "Industry Classification": "sector",
                    "ISIN": "isin",
                    "Country": "country",
                    "Name": "holding_name",
                    "Currency": "currency",
                }
            )

            # Some files might have slightly different casing; ensure columns exist
            for col, alias in [
                ("Weighting", "weight"),
                ("Industry Classification", "sector"),
                ("ISIN", "isin"),
                ("Country", "country"),
                ("Name", "holding_name"),
                ("Currency", "currency"),
            ]:
                if col in df.columns and alias not in df.columns:
                    df = df.rename({col: alias})

            # Coerce weight to float and scale to 0..100 if needed
            if "weight" in df.columns:
                df = df.with_columns(pl.col("weight").cast(pl.Float64, strict=False))
                # If the max is <= 1, values are in 0..1 range -> multiply by 100
                try:
                    max_w = df.select(pl.col("weight").max()).item()
                    if max_w is not None and max_w <= 1.0:
                        df = df.with_columns((pl.col("weight") * 100.0).alias("weight"))
                except Exception:
                    pass

            # Minimal cleansing + shape
            records: List[Dict[str, Any]] = []
            for row in df.to_dicts():
                h_isin = row.get("isin")
                if h_isin and len(str(h_isin)) != 12:
                    # Skip clearly invalid ISINs
                    continue

                w = row.get("weight")
                try:
                    w = float(w) if w not in (None, "-", "—") else None
                except Exception:
                    w = None

                records.append(
                    {
                        "isin": h_isin,
                        "holding_name": row.get("holding_name"),
                        "weight": w,
                        "sector": row.get("sector"),
                        "country": row.get("country"),
                        "currency": row.get("currency"),
                    }
                )

            return {"isin": isin, "holdings": records}

        except Exception as e:
            # Return empty set on errors but keep pipeline moving
            print(f"Warning: failed to fetch holdings for {isin}: {e}")
            return {"isin": isin, "holdings": []}


# ---------- Orchestrator ----------
async def main():
    """
    1) Read ETF list from local XLSX
    2) Concurrently fetch holdings per ISIN (limit = CONCURRENT_REQUESTS) with tqdm bar
    3) Normalize to models and upsert into DB (same as other scrapers)
    """
    print("Reading Xtrackers ETF list...")
    etfs = get_etf_list()
    if not etfs:
        print("No ETFs found in AllProductData.xlsx. Exiting.")
        return

    etf_tuples: List[Tuple] = [e.to_db_tuple() for e in etfs]
    isins_to_update: List[str] = [e.isin for e in etfs]

    print(
        f"Fetching holdings for {len(isins_to_update)} ETFs "
        f"with {CONCURRENT_REQUESTS} concurrent workers..."
    )

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    holdings_tuples: List[Tuple] = []

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_holdings_for_isin(session, semaphore, isin)
            for isin in isins_to_update
        ]

        # Progress over completion of individual tasks
        for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Holdings"):
            result = await fut
            etf_isin = result["isin"]
            for h in result["holdings"]:
                # Normalize through shared Holding model
                h_obj = Holding(
                    etf_isin=etf_isin,
                    holding_isin=h.get("isin")
                    if not str(h.get("isin")).startswith("_CURRENCY")
                    else None,
                    holding_name=h.get("holding_name"),
                    weight=h.get("weight"),
                    sector=h.get("sector"),
                    country=h.get("country"),
                    currency=h.get("currency"),
                )
                holdings_tuples.append(h_obj.to_db_tuple())

    if not holdings_tuples:
        print("\nNo holdings parsed. Still writing ETF master data.")

    # ---------- Persist into DB ----------
    print("\nWriting to database...")
    with open_db(DB_NAME) as conn:
        # Recreate schema (destructive; consistent with other scrapers)
        setup_database(conn)

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
            # Basic hygiene (mirror other scrapers)
            if holding_isin and len(holding_isin) != 12:
                continue
            if weight is None or weight < 0:
                weight = 0.0

            # Ensure a usable security name
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

    print("✅ Xtrackers scraping complete. Database is up to date.")


if __name__ == "__main__":
    asyncio.run(main())
