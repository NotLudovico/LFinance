import marimo

__generated_with = "0.14.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import datetime 
    return datetime, mo, pl


@app.cell
def _(pl):
    bonds = (
        pl.read_csv("https://simpletoolsforinvestors.eu/data/export/EF274963E6B63A80FCC715673B769541.csv", separator=";")
        .drop(pl.last()) # Seems just empty column
        .with_columns(
            pl.col("referencedate").str.to_date(format="%d/%m/%Y"),
            pl.col("redemptiondate").str.to_date(format="%d/%m/%Y"),
            pl.col("minimumlot").cast(pl.Int32),
            pl.col("settlementprice").str.replace(",",".").cast(pl.Float32),
            pl.col("volume").cast(pl.Int32),
            pl.col("volumevalue").cast(pl.Int32),
            pl.col("grossyieldtomaturity").str.replace(",", ".").cast(pl.Float32),
            pl.col("grossduration").str.replace(",", ".").cast(pl.Float32),
            pl.col("netyieldtomaturity").str.replace(",", ".").cast(pl.Float32),
            pl.col("nocgduration").str.replace(",", ".").cast(pl.Float32),
            pl.col("nocgyieldtomaturity").str.replace(",", ".").cast(pl.Float32),
            pl.col("ispread").str.replace(",", ".").cast(pl.Float32),
            pl.col("zspread").str.replace(",", ".").cast(pl.Float32),
            pl.col("currentcouponrate").str.replace(",", ".").cast(pl.Float32),
            pl.col("couponperiodicity").cast(pl.Int32),
            pl.col("couponmonths").str.replace(",", ".").cast(pl.Float32, strict=False),
            pl.col("instantyield").str.replace(",", ".").cast(pl.Float32),
        )
    )
    bonds
    return (bonds,)


@app.cell
def _(mo):
    mo.md(r"""# Filtering""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# """)
    return


@app.cell
def _(mo):
    currencies_codes = [
        "EUR",
        "USD",
        "GBP",
        "CHF",
        "AUD",
        "NOK",
        "SEK",
        "TRY",
        "MXN",
        "ZAR",
        "DEM",
        "ITL",
    ]

    currencies = mo.ui.dictionary(
        dict(
            zip(
                currencies_codes,
                [
                    mo.ui.checkbox(value=False, label=f"{currencies_codes[i]}")
                    for i in range(0, len(currencies_codes))
                ],
            )
        )
    )
    return (currencies,)


@app.cell
def _(bonds, currencies, datetime, mo):
    date_range = mo.ui.date_range(
        start=datetime.date.today(),
        stop=bonds.select("redemptiondate").max().item(),
    )
    min_lot = mo.ui.number(value=1000)

    mo.vstack(
        [
            mo.md("##Currencies"),
            mo.hstack([v for _, v in currencies.items()]),
            mo.md("## Maturity Date"),
            date_range,
            mo.md("## Minimum Lot"),
            min_lot
        ]
    )
    return date_range, min_lot


@app.cell
def _(bonds, currencies, date_range, min_lot, pl):
    bonds.filter(
        pl.col("currencycode").is_in([k for k, v in currencies.value.items() if v]),
        pl.col("redemptiondate") >= date_range.value[0],
        pl.col("redemptiondate") <= date_range.value[1],
        pl.col("minimumlot") <= min_lot.value
    )
    return


if __name__ == "__main__":
    app.run()
