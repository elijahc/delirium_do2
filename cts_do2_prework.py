import marimo

__generated_with = "0.17.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    return duckdb, mo, pd


@app.cell
def _(duckdb, mo):
    dcon = duckdb.connect(mo.notebook_location() / "public" / "delirium_do2.ddb")
    return (dcon,)


@app.cell
def _(dcon, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE delirium AS
        	SELECT * FROM read_csv("/Users/elijahc/data/clendenen_projs/prin/2025-02-25/delirium7d.csv")
        """,
        engine=dcon
    )
    return


@app.cell
def _(pd):
    do2_prep = pd.read_csv("/Users/elijahc/data/clendenen_projs/prin/2023-10-08/do2.csv") \
        .drop(columns=['delirium 72hr']) \
        .dropna()
    return (do2_prep,)


@app.cell
def _(dcon, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE do2 AS
        	SELECT * FROM read_parquet("./public/do2.parquet")
        """,
        engine=dcon
    )
    return


@app.cell
def _(do2_prep):
    do2_prep.to_parquet('./public/do2.parquet')
    return


@app.cell
def _(dcon, delirium, mo):
    pod_pid = mo.sql(
        f"""
        SELECT person_id FROM "delirium"
        """,
        engine=dcon
    )
    return (pod_pid,)


@app.cell
def _(pd):
    t1 = pd.read_csv("/Users/elijahc/data/clendenen_projs/prin/2023-10-08/encounters.csv") \
        .drop(columns=['delirium 72hr']); t1
    return (t1,)


@app.cell
def _(pod_pid):
    pod_pid['person_id']
    return


@app.cell
def _(pod_pid, t1):
    t1[t1.person_id.isin(pod_pid['person_id'])].reset_index(drop=True) \
        .to_parquet('./public/t1.parquet')
    return


@app.cell
def _(dcon, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE encounter AS
        	SELECT * FROM read_parquet("./public/t1.parquet")
        """,
        engine=dcon
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
