import marimo

__generated_with = "0.14.11"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import json
    import sqlite3
    import polars as pl
    import matplotlib.pyplot as plt
    import plotly.express as px
    from matplotlib.patches import Polygon
    from matplotlib.collections import PatchCollection
    from matplotlib.cm import get_cmap
    from matplotlib.colors import Normalize
    from dataclasses import dataclass


    plt.style.use("default")
    return mo, pl, plt, px, sqlite3


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Portfolio""")
    return


@app.cell
def _():
    portfolio = {
        "IE00B52MJY50": 5072,
        "IE00BMG6Z448": 15278,
        "IE00B4L5Y983": 25430,
        "LU0908500753": 29484,
    }
    return (portfolio,)


@app.cell(hide_code=True)
def _(mo, portfolio):
    total = sum([v for k, v in portfolio.items()])
    portfolio_pct = dict(
        zip(portfolio.keys(), map(lambda x: x / total, portfolio.values()))
    )

    mo.vstack([mo.md(f"{k}: **{v*100:.2f}%**") for k,v in portfolio_pct.items()])
    return (portfolio_pct,)


@app.cell(hide_code=True)
def _(sqlite3):
    # Attaching DB
    conn = sqlite3.connect("data/database.db")
    return (conn,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# All Holdings""")
    return


@app.cell(hide_code=True)
def _(conn, pl, portfolio_pct):
    dfs = []
    for k, v in portfolio_pct.items():
        df = (
            pl.read_database(
                query=f"""
                    SELECT *
                    FROM etf_holdings
                    WHERE etf_isin="{k}";
                   """,
                connection=conn,
            )
            .with_columns(
                pl.col("weight") * v,
            )
            .drop(pl.col("id"))
            .filter(pl.col("weight") > 0)
        )
        dfs.append(df)


    complete_portfolio = (
        pl.concat(dfs)
        .drop("etf_isin")
        .group_by("holding_isin")
        .agg(
            pl.first("holding_name").alias("holding_name"),
            pl.sum("weight").alias("weight"),
            pl.first("sector").alias("sector"),
            pl.first("country").alias("country"),
            pl.first("currency").alias("currency"),
        )  
        .sort(by="weight", descending=True)
    )
    complete_portfolio
    return (complete_portfolio,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Geographical Exposure""")
    return


@app.cell
def _(complete_portfolio, pl):
    geo_allocation = (
        complete_portfolio.select(["country", "weight"])
        .group_by("country")
        .agg([pl.col("weight").sum()])
        .filter(pl.col("weight") >= 0)
        .sort(pl.col("weight"), descending=True)
    )
    geo_allocation
    return (geo_allocation,)


@app.cell(hide_code=True)
def _(geo_allocation, mo, px):
    mo.ui.plotly(
        px.choropleth(
            geo_allocation,
            locations="country",
            color="weight",
            color_continuous_scale=[
                [0.0, "rgb(70, 130, 180)"],
                [1.0, "rgb(220, 20, 60)"],
            ],
            projection="natural earth",
            title="Distribuzione investimenti per Paese",
        )
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Sector Exposure""")
    return


@app.cell(hide_code=True)
def _(complete_portfolio, pl):
    sector_allocation = (
        complete_portfolio.select(["sector", "weight"])
        .group_by("sector")
        .agg([pl.col("weight").sum()])
        .filter(pl.col("weight") >= 0)
        .sort(pl.col("weight"), descending=True)
        .filter(pl.col("sector").is_not_null())
    )

    sector_allocation
    return (sector_allocation,)


@app.cell(hide_code=True)
def _(mo, plt, sector_allocation):
    plt.figure(figsize=(6, 4))
    plt.barh(
        sector_allocation["sector"].to_list(),
        sector_allocation["weight"].to_list(),
    )
    plt.title("Sector Exposure")
    plt.grid(alpha=0.4)
    plt.xlabel("Weight (%)")

    mo.center(plt.gca())
    return


if __name__ == "__main__":
    app.run()
