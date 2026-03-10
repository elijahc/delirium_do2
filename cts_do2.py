import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium", app_title="Delirium and Oxygen Delivery")


@app.cell
def _():
    import marimo as mo
    import matplotlib.pyplot as plt
    import pyarrow as pa
    import scienceplots
    import plot_styler
    import duckdb
    import pandas as pd
    import seaborn as sns
    import numpy as np
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    import pathlib
    # from styles import params_basic, params_serif, params_thin
    return duckdb, mo, np, pa, pd, plt, sns


@app.cell
def _():
    from utils import rebin_time, tidy_flow, ddb_to_pandas
    from tables import Table

    return ddb_to_pandas, rebin_time


@app.cell
def _(pd):
    def align_metric(df,by='person_id',events=None):
            df = df.merge(events[[by,'offset']], on=by, how='left')
            df.time = df.time - pd.to_timedelta(df.offset,unit='day')
            return df

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Delirium and Oxygen delivery

    ## Data
    - Connects to remote duckdb instance

    ```
    ATTACH 'https://data.pubs.edclabs.net/doi/10.1177/02676591261431999/delirium_do2.ddb' AS pub;
    ```

    - Load DO2 data, merge with demographic data

    ```
    SELECT
        do2.person_id, encounter.gender, do2.time, do2.value, do2.offset
    FROM
        pub.do2
    JOIN pub.encounter ON do2.person_id = encounter.person_id;
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH 'https://data.pubs.edclabs.net/doi/10.1177/02676591261431999/delirium_do2.ddb' AS pub;
        """
    )
    return


@app.cell
def _():
    # If connecting locally
    # apath_to_ddb = mo.notebook_location() / "public" / "delirium_do2.ddb"
    # dcon = duckdb.connect(str(apath_to_ddb))
    return


@app.cell
def _(mo):
    pod_labels = mo.sql(
        f"""
        SELECT * FROM pub.delirium
        """,
        output=False
    )
    return (pod_labels,)


@app.cell
def _(mo):
    do2 = mo.sql(
        f"""
        SELECT
            do2.person_id, encounter.gender, do2.time, do2.value, do2.offset
        FROM
            pub.do2
            -- JOIN delirium ON do2.person_id = delirium.person_id
        	JOIN pub.encounter ON do2.person_id = encounter.person_id;
        """,
        output=False
    )
    return (do2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Data Customization
    Inputs:
    - Time Window: Postop time window in days (default 3)
    > Sets the duration of time to pull DO2 data over
    - Binning window: time windows to rebin time-varying data over
    > Options are `'hour','q4h','q8h','q12h','day'`
    - Delirium time window (default 7d)
    > A positive CAM-ICU score in this window places you in the delirium group
    """)
    return


@app.cell
def _(mo):
    # Define filter controls
    do2_win_slider = mo.ui.slider(start=3,stop=7,step=0.5, label='Time window (days)')
    bin_select = mo.ui.dropdown(options=['hour','q4h','q8h','q12h','day'], value='q4h',label='Binning window')
    pod_win_select = mo.ui.dropdown(options=['72hr','7d','ever'], value='7d',label='Delirium window')
    mo.hstack([pod_win_select, bin_select, do2_win_slider],justify='space-around')
    return bin_select, do2_win_slider, pod_win_select


@app.cell
def _(bin_select, ddb_to_pandas, do2, pod_labels, pod_win_select, rebin_time):
    group_cols = [f"delirium.{pod_win_select.value}",'gender','person_id','btime']
    do2_binned = rebin_time(ddb_to_pandas(do2), bin_select.value) \
        .merge(pod_labels.to_pandas(),how='left',on='person_id') \
        .groupby(group_cols).value.mean().reset_index()
    return (do2_binned,)


@app.cell
def _(duckdb):
    enc_pqt_fp = "/Users/elijahc/data/compass/SWAN_latest/raw/Table1_Encounters.parquet"
    enc_tab = duckdb.read_parquet(enc_pqt_fp).to_arrow_table()
    return (enc_tab,)


@app.cell
def _(pd):
    # enc = Table('/Users/elijahc/data/compass/SWAN_latest/raw/Table1_Encounter_Info.csv')
    # id_map = enc.sel(encounter_id=None)[['person_id','encounter_id']].drop_duplicates()

    # Load cache instead
    id_map = pd.read_parquet('./public/id_map.parquet')
    return


@app.cell
def _(do2_binned, enc_tab, pa):
    do2_p2e = enc_tab.select(['person_id','encounter_id']) \
        .filter(pa.compute.field('person_id').isin(list(do2_binned.person_id.unique()))) \
        .to_pandas()
    print('Number of unique patient IDs:')
    print(do2_p2e.person_id.nunique())
    return


@app.cell
def _():
    # ev = ddb_to_pandas(do2)[['person_id','offset']].drop_duplicates()
    # cci_plot_df = align_metric(f_c,events=ev)[['person_id','time','name','value']]
    # cci_plot_df = rebin_time(cci_plot_df,bin_select.value) \
    #     .merge(plot_df[['person_id','gender','delirium.7d']].drop_duplicates(),how='left',on='person_id') \
    #     .groupby([f"delirium.{pod_win}",'gender','person_id','btime']).value.mean().reset_index() \
    #     .query("btime >=0 and btime <= {}".format(int(24*do2_win_slider.value))) \
    #     .assign(CI=lambda x: x['value']) \
    #     .drop(columns=['value']).reset_index(drop=True)

    # cci_plot_df
    return


@app.cell
def _(do2_binned, do2_win_slider, pd, pod_win_select):
    pod_col_name = f"delirium.{pod_win_select.value}"
    plot_df = do2_binned.assign(hour=lambda x: x.btime) \
        .query("hour >=0 and hour <= {}".format(int(24*do2_win_slider.value))) \
        .assign(DO2i=lambda x: x['value']) \
        .reset_index(drop=True) \
        .drop(columns=['btime','value']) \
        .astype({'person_id':pd.UInt64Dtype()})[['person_id','gender',pod_col_name,'hour','DO2i']];
    plot_df
    return (plot_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Timeseries plots

    ### Overall DO2i by Delirium status
    """)
    return


@app.cell
def _(plot_df, plt, pod_win_select, sns):
    plt.style.use(['edc-science','notebook'])
    sns.lineplot(x='hour', y='DO2i', hue='delirium.{}'.format(pod_win_select.value),data=plot_df,ax=plt.subplot())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### DO2i and Delirium stratified by sex
    """)
    return


@app.cell
def _(plot_df, plt, pod_win_select, sns):
    plt.style.use(['edc-science','notebook'])

    g = sns.FacetGrid(plot_df, row='gender',aspect=2.5,margin_titles=True,sharex=False,legend_out=False)
    g.map(sns.lineplot, 'hour','DO2i',f"delirium.{pod_win_select.value}")
    g.add_legend()
    # g.savefig('./assets/m_vs_f_do2i.pdf',dpi=300)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Power Analysis

    The below power analysis is assuming 80 percent power to detect 10% difference in DO2i
    """)
    return


@app.cell
def _(np, plt):
    import pingouin as pg
    # sns.set(style='ticks', context='notebook', font_scale=1.2)
    d = 35/353  # Fixed effect size
    n = np.arange(50, 1500, 50)  # Incrementing sample size
    # Compute the achieved power
    pwr = pg.power_ttest(d=d, n=n, contrast='paired')
    # Start the plot
    plt.plot(n, pwr, 'ko-.')
    plt.axhline(0.8, color='r', ls=':')
    plt.xlabel('Sample size')
    plt.ylabel('Power (1 - type II error)')
    plt.title('Achieved power of a paired T-test')
    # sns.despine()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ##
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aggregate Data
    The data below is aggregated over window set by "Time Window".

    - **Mean DO2i** is the per-patient mean taken over the binned windows to counteract interpatient differences in sampling
    - **Nadir DO2i** is the min DO2i over the Time window
    - **DO2i<310** is the cumulative time in minutes a patient spent under the DO2i threshold of 310 in the time window
    - **DO2i<358** is the cumulative time in minutes a patient spent under the DO2i threshold of 358 in the time window
    """)
    return


@app.cell
def _(ddb_to_pandas, do2, do2_win_slider):
    def min_under_310(df):
        # print(df)
        df['dt'] = (df['time']-df['time'].shift()) 
        return df[df['value']<310].dt.sum()

    def min_under_358(df):
        # print(df)
        df['dt'] = (df['time']-df['time'].shift())
        return df[df['value']<358].dt.sum()


    twin_end = int(24*do2_win_slider.value)
    do2_df = ddb_to_pandas(do2,t_start=0,t_end=twin_end).reset_index(drop=True)
    # do2_df = do2_df.groupby('person_id').apply(add_dt); do2_df
    # grps = do2_df.groupby('person_id').groups
    # grps
    return do2_df, min_under_310, min_under_358


@app.cell
def _(do2_df, min_under_310, min_under_358, np, plot_df):
    nadir_df = plot_df.groupby(['person_id']).apply(lambda s: s['DO2i'].min()).rename('Nadir DO2i')
    mean_df = plot_df.groupby(['person_id']).apply(lambda s: s['DO2i'].mean()).rename('Mean DO2i')
    sub310 = do2_df.groupby('person_id').apply(min_under_310).rename('DO2i<310') / np.timedelta64(1,'D') * 24.0
    sub310 = (sub310 * 60).astype(int)
    sub358 = do2_df.groupby('person_id').apply(min_under_358).rename('DO2i<358') / np.timedelta64(1,'D') * 24.0
    sub358 = (sub358 * 60).astype(int)
    return mean_df, nadir_df, sub310, sub358


@app.cell
def _():
    # do2_df.groupby('person_id').apply(min_under_310).rename('sub310') 
    # do2_df[do2_df.person_id==39503556].groupby('person_id').apply(min_under_310)
    return


@app.cell
def _():
    # do2_df['dt'] = (do2_df['time'] - do2_df['time'].shift())
    # do2_df['sub358'] = do2_df['value']<358
    # do2_df['sub310'] = do2_df['value']<310

    # do2_df.groupby('person_id').apply(lambda s: s[s['value']<358].dt.sum())
    return


@app.cell
def _(mean_df, nadir_df, plot_df, sub310, sub358):
    agg_plot_df = plot_df[['person_id','delirium.7d','gender']].drop_duplicates()
    for df in [nadir_df, mean_df, sub310, sub358]:
        agg_plot_df = agg_plot_df.merge(df.reset_index(), on='person_id', how='left')
    agg_plot_df
    return (agg_plot_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aggregate Plots
    """)
    return


@app.cell
def _(agg_plot_df, pd, plt, pod_win_select, sns):
    def make_agg_plots(df,yaxis_label=None, use_cols=None):
        params = {'id_vars':['delirium.7d','gender','person_id'],'var_name':'metric'}
        if use_cols is not None:
            params['value_vars']=use_cols
        plt_df = pd.melt(df, **params)
        g = sns.catplot(plt_df, x='gender',y='value',hue=f"delirium.{pod_win_select.value}",col='metric',kind='bar',
            aspect=0.6,col_wrap=2,sharey=True,legend=False,
            # split=True,bw_adjust=0.75,cut=0
        )
        # g.set(ylim=(0,650))
        g.set_titles("{col_name}")
        if yaxis_label is not None:
            g.set_axis_labels(y_var=yaxis_label)
        # g = sns.FacetGrid(df, col='metric', aspect=1.25)
        # g.map(sns.barplot, 'value',hue=f"delirium.{pod_win_select.value}")
        # g.add_legend()
        # g.despine(bottom=True)
        return g.fig

    plt.style.use(['edc-science','notebook'])
    agg_do2_fig = make_agg_plots(agg_plot_df,yaxis_label='DO2i (ml/min/m2)',use_cols=['Nadir DO2i','Mean DO2i']); agg_do2_fig
    return agg_do2_fig, make_agg_plots


@app.cell
def _(agg_do2_fig):
    agg_do2_fig.savefig('./assets/agg_do2i_no_legend.pdf',dpi=300)
    return


@app.cell
def _(agg_plot_df, make_agg_plots):
    agg_min_fig = make_agg_plots(agg_plot_df,yaxis_label='minutes',use_cols=['DO2i<310','DO2i<358']); agg_min_fig
    return (agg_min_fig,)


@app.cell
def _(agg_min_fig):
    agg_min_fig.savefig('./assets/agg_min_no_legend.pdf',dpi=300)
    return


@app.cell
def _():
    # cci = flow_ds.to_table(filter=(f_ex[0] | f_ex[1])) \
    #     .filter(~pa.compute.field('encounter_id').isin(list(do2_p2e.encounter_id.unique()))) \
    #     .filter(~pa.compute.field('flowsheet_days_since_birth').isin(['','>32507'])) \
    #     .to_pandas() \
    #     .astype({'flowsheet_days_since_birth':pd.UInt64Dtype(),
    #              # 'flowsheet_value':float,
    #             });

    # cci = enc_tab.select(['person_id','encounter_id']).to_pandas().astype(pd.UInt64Dtype()) \
    #     .merge(cci, how='inner', on='encounter_id')

    #     # .filter(~pa.compute.field('flowsheet_value').isin([''])) \
    return


@app.cell
def _():
    # list(plot_df.person_id.unique())
    return


@app.cell
def _():
    # cci[cci.person_id.isin(list(plot_df.person_id.unique()))]
    return


@app.cell
def _():
    # cci[cci.person_id.isin(plot_df.person_id)]
    return


@app.cell
def _():
    import os
    from glob import glob
    import polars as po

    return


@app.cell
def _():
    # flow_path = "/Users/elijahc/data/compass/SWAN_latest/raw/Table2_Flowsheet.part/*/*.parquet"
    # flow_tab = duckdb.read_parquet(flow_path,hive_partitioning=True) \
    #     .fetch_arrow_table() \
    #     .filter(pa.compute.field("flowsheet_days_since_birth") != "") \
    #     .filter(pa.compute.field("flowsheet_days_since_birth") != ">32507")
    return


@app.cell
def _():
    # df_slice(df=cci, against=plot_df, by='person_id')
    return


@app.cell
def _():
    def pa_slice(tab, against, by, columns=None):
        assert by in against.columns.to_list()
        assert by in tab.column_names

        bool_mask = tab.select([by]).to_pandas()[by].isin(against[by].unique())

        if columns is None:
            columns = tab.column_names

        return tab.select(columns).filter(bool_mask.values)


    def df_slice(df, against, by, columns=None):
        assert by in against.columns.to_list()
        assert by in df.columns.to_list()

        bool_mask = df[by].isin(against[by].unique())

        if columns is None:
            columns = df.columns.to_list()

        return df[columns][bool_mask].reset_index(drop=True)

    return


@app.cell
def _():
    # flow_tab.filter(pa.compute.field("display_name")=="CCI")
    return


@app.cell
def _():
    # pa_slice(enc_tab,against=plot_df, by='person_id',columns=['encounter_id','person_id']).to_pandas()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
