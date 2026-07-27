import marimo

__generated_with = "0.23.14"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pipeline_monitor_backend as backend

    return backend, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Pipeline record monitor

    Compare records across **2–5 ordered stages**. Each stage may use its own
    database schema and table, or a pandas DataFrame registered under the
    **`notebook`** schema.
    """)
    return


@app.cell
def _():
    import sqlalchemy as _sqlalchemy

    ATHENA_URL = _sqlalchemy.URL.create(
        drivername="awsathena+rest",
        host="athena.us-east-1.amazonaws.com",
        port=443,
        database="sandbox_db",
        query={
            "s3_staging_dir": "s3://sandbox-data-706059253616-us-east-1",
            "region_name": "us-east-1",
            "catalog_name": "AwsDataCatalog",
            "work_group": "primary",
        },
    )
    engine = _sqlalchemy.create_engine(ATHENA_URL)
    return (engine,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Notebook DataFrames

    To make a pandas DataFrame available as a stage table, register it below.
    Its schema will appear as **`notebook`** and the dictionary key will appear
    in that stage's **Table** dropdown.

    ```python
    notebook_tables = {
        "raw_events": raw_events_df,
        "processed_events": processed_events_df,
    }
    ```
    """)
    return


@app.cell
def _():
    # Add DataFrames defined in this notebook, for example:
    # notebook_tables = {"raw_events": raw_events_df}
    notebook_tables = {}
    return (notebook_tables,)


@app.cell
def _(backend, engine, mo):
    catalog = backend.inspect_database(engine)
    if catalog.error:
        _status = mo.callout(
            mo.md(
                f"**Database metadata unavailable:** `{catalog.error}`  \n"
                "You can still configure stages from the `notebook` schema."
            ),
            kind="warn",
        )
    else:
        _status = mo.callout(
            mo.md(
                f"**Connected.** Found {len(catalog.schemas)} database schema(s)."
            ),
            kind="success",
        )
    _status
    return (catalog,)


@app.cell
def _(mo):
    stage_count = mo.ui.dropdown(
        options=[2, 3, 4, 5],
        value=2,
        label="Number of pipeline stages",
        allow_select_none=False,
        full_width=True,
    )
    mo.vstack([mo.md("## 1. Choose the pipeline length"), stage_count])
    return (stage_count,)


@app.cell
def _(backend, catalog, mo, stage_count):
    _schema_options = backend.schema_options(catalog)
    _schema_default = backend.default_schema_option(catalog)
    stage_schema_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_schema_options,
                value=_schema_default,
                label="Schema",
                searchable=True,
                allow_select_none=False,
                full_width=True,
            )
            for _position in range(stage_count.value)
        ]
    )
    return (stage_schema_selectors,)


@app.cell
def _(
    backend,
    catalog,
    mo,
    notebook_tables,
    stage_schema_selectors,
):
    stage_table_errors = []
    stage_table_options = []
    for _position, _schema in enumerate(stage_schema_selectors.value, start=1):
        try:
            _tables = backend.list_stage_tables(
                catalog.inspector, _schema, notebook_tables
            )
            _error = None
        except Exception as _exc:
            _tables = []
            _error = f"Stage {_position}: {_exc}"
        stage_table_options.append(_tables)
        stage_table_errors.append(_error)

    stage_table_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_tables,
                value=_tables[0] if _tables else None,
                label="Table",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _tables,
            )
            for _tables in stage_table_options
        ]
    )
    return stage_table_errors, stage_table_options, stage_table_selectors


@app.cell
def _(
    backend,
    catalog,
    mo,
    notebook_tables,
    stage_schema_selectors,
    stage_table_selectors,
):
    stage_column_errors = []
    stage_column_options = []
    for _position, (_schema, _table) in enumerate(
        zip(stage_schema_selectors.value, stage_table_selectors.value), start=1
    ):
        try:
            _columns = backend.list_stage_columns(
                catalog.inspector, _schema, _table, notebook_tables
            )
            _error = None
        except Exception as _exc:
            _columns = []
            _error = f"Stage {_position}: {_exc}"
        stage_column_options.append(_columns)
        stage_column_errors.append(_error)

    stage_comparison_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_columns,
                value=backend.default_comparison_column(_columns),
                label="Comparison key",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _columns,
            )
            for _columns in stage_column_options
        ]
    )
    stage_timestamp_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_columns,
                value=backend.default_timestamp_column(_columns),
                label="Timestamp column",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _columns,
            )
            for _columns in stage_column_options
        ]
    )
    return (
        stage_column_errors,
        stage_column_options,
        stage_comparison_selectors,
        stage_timestamp_selectors,
    )


@app.cell
def _(mo):
    duplicate_policy = mo.ui.dropdown(
        options={
            "Latest timestamp": "latest",
            "Earliest timestamp": "earliest",
        },
        value="Latest timestamp",
        label="Duplicate handling",
        allow_select_none=False,
        full_width=True,
    )
    lookback_days = mo.ui.number(
        start=0,
        step=1,
        value=0,
        label="Lookback days (0 means all records)",
        full_width=True,
    )
    run_monitor = mo.ui.run_button(
        label="Run monitor", kind="success", full_width=True
    )
    return duplicate_policy, lookback_days, run_monitor


@app.cell
def _(
    duplicate_policy,
    lookback_days,
    mo,
    run_monitor,
    stage_column_errors,
    stage_comparison_selectors,
    stage_count,
    stage_schema_selectors,
    stage_table_errors,
    stage_table_options,
    stage_table_selectors,
    stage_timestamp_selectors,
):
    _stage_cards = []
    for _index in range(stage_count.value):
        _source = mo.hstack(
            [stage_schema_selectors[_index], stage_table_selectors[_index]],
            widths="equal",
            align="start",
            gap=1,
        )
        _columns = mo.hstack(
            [
                stage_comparison_selectors[_index],
                stage_timestamp_selectors[_index],
            ],
            widths="equal",
            align="start",
            gap=1,
        )
        _empty_hint = (
            mo.callout(
                "No tables are available. Register a DataFrame above or choose another schema.",
                kind="info",
            )
            if not stage_table_options[_index]
            else mo.md("")
        )
        _stage_cards.append(
            mo.callout(
                mo.vstack(
                    [
                        mo.md(f"### Stage {_index + 1}"),
                        _source,
                        _columns,
                        _empty_hint,
                    ],
                    gap=1,
                ),
                kind="neutral",
            )
        )

    _metadata_errors = [
        _error
        for _error in [*stage_table_errors, *stage_column_errors]
        if _error
    ]
    _error_display = (
        mo.callout(mo.md("  \n".join(_metadata_errors)), kind="danger")
        if _metadata_errors
        else mo.md("")
    )
    mo.vstack(
        [
            mo.md("## 2. Configure each stage"),
            *_stage_cards,
            _error_display,
            mo.md("## 3. Query settings"),
            mo.hstack(
                [duplicate_policy, lookback_days],
                widths="equal",
                align="start",
                gap=1,
            ),
            run_monitor,
        ],
        gap=1,
    )
    return


@app.cell
def _(
    backend,
    duplicate_policy,
    engine,
    lookback_days,
    notebook_tables,
    run_monitor,
    stage_comparison_selectors,
    stage_schema_selectors,
    stage_table_selectors,
    stage_timestamp_selectors,
):
    monitor_error = None
    monitor_ran = False
    stage_names = []
    pipeline_result = backend.empty_pipeline_result()

    if run_monitor.value:
        monitor_ran = True
        _stages = [
            {
                "schema": _schema,
                "table": _table,
                "comparison_column": _comparison,
                "timestamp_column": _timestamp,
            }
            for _schema, _table, _comparison, _timestamp in zip(
                stage_schema_selectors.value,
                stage_table_selectors.value,
                stage_comparison_selectors.value,
                stage_timestamp_selectors.value,
            )
        ]
        try:
            pipeline_result, stage_names = backend.run_pipeline(
                engine=engine,
                stages=_stages,
                notebook_tables=notebook_tables,
                duplicate_policy=duplicate_policy.value,
                lookback_days=lookback_days.value,
            )
        except Exception as _exc:
            monitor_error = str(_exc)
            pipeline_result = backend.empty_pipeline_result()
    return monitor_error, monitor_ran, pipeline_result, stage_names


@app.cell
def _(backend, mo, monitor_error, monitor_ran, pipeline_result, stage_names):
    if monitor_error:
        _results = mo.callout(
            mo.md(f"**Monitor failed**\n\n{monitor_error}"), kind="danger"
        )
    elif not monitor_ran:
        _results = mo.callout(
            "Configure all stages, then select Run monitor.", kind="info"
        )
    else:
        _results = mo.vstack(
            [
                mo.md(f"## Results ({len(pipeline_result):,} records)"),
                mo.ui.table(
                    pipeline_result,
                    style_cell=backend.make_complete_row_styler(
                        pipeline_result, stage_names
                    ),
                ),
            ]
        )
    _results
    return


@app.cell
def _(mo, monitor_error, monitor_ran, pipeline_result):
    if monitor_ran and not monitor_error and not pipeline_result.empty:
        _download = mo.download(
            data=pipeline_result.to_csv(index=False).encode("utf-8"),
            filename="pipeline-monitor.csv",
            label="Download CSV",
        )
    else:
        _download = mo.md("")
    _download
    return


if __name__ == "__main__":
    app.run()
