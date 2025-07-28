import marimo

__generated_with = "0.14.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import json
    import sqlite3
    import polars as pl
    import matplotlib.pyplot as plt
    import plotly.express as px
    from utilities.country import COUNTRY_TO_ISO3 
    from matplotlib.patches import Polygon
    from matplotlib.collections import PatchCollection
    from matplotlib.cm import get_cmap
    from matplotlib.colors import Normalize


    plt.style.use("default")
    return COUNTRY_TO_ISO3, pl, px, sqlite3


@app.cell
def _():
    portfolio = {
        "IE00B52MJY50": 5072,
        "IE00BMG6Z448": 15278,
        "IE00B4L5Y983": 18430,
        "LU0908500753": 19484,
    }
    return (portfolio,)


@app.cell
def _(portfolio):
    total = sum([v for k, v in portfolio.items()])
    portfolio_pct = dict(
        zip(portfolio.keys(), map(lambda x: x / total, portfolio.values()))
    )
    portfolio_pct
    return (portfolio_pct,)


@app.cell
def _(sqlite3):
    # Attaching DB
    conn = sqlite3.connect("data/database.db")
    return (conn,)


@app.cell
def _(COUNTRY_TO_ISO3, conn, pl, portfolio_pct):
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
                pl.col("country").str.to_lowercase(),
            )
            .with_columns(
                pl.col("country").replace(COUNTRY_TO_ISO3),
            )
            .drop(pl.col("id"))
            .filter(pl.col("weight") > 0)
        )
        dfs.append(df)


    complete_portfolio = pl.concat(dfs)
    complete_portfolio
    return (complete_portfolio,)


@app.cell
def _(complete_portfolio, pl):
    allocation = (
        complete_portfolio.select(["country", "weight"])
        .group_by("country")
        .agg([pl.col("weight").sum()])
        .filter(pl.col("weight") >= 0)
        .sort(pl.col("weight"))
    )
    allocation
    return (allocation,)


@app.cell
def _(allocation, px):
    labels = allocation["country"].to_list()
    weights = allocation["weight"].to_list()

    fig = px.choropleth(
        allocation,
        locations="country",            
        color="weight",
        color_continuous_scale="Blues",
        projection="natural earth",
        title="Distribuzione investimenti per Paese"
    )
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))
    fig.show()
    return


if __name__ == "__main__":
    app.run()
