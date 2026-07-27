import marimo

__generated_with = "0.23.14"
app = marimo.App(width="full", app_title="Sentinel")


@app.cell
def _():
    import marimo as mo
    import pipeline_monitor_backend as backend

    return backend, mo


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
def _():
    # Add DataFrames defined in this notebook, for example:
    # notebook_tables = {"raw_events": raw_events_df}
    notebook_tables = {}
    return (notebook_tables,)


@app.cell
def _(backend, engine):
    catalog = backend.inspect_database(engine)
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
    mo.center(mo.md("# Sentinel"))
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
                label="",
                searchable=True,
                allow_select_none=False,
                full_width=True,
            )
            for _position in range(stage_count.value)
        ]
    )
    return (stage_schema_selectors,)


@app.cell
def _(backend, catalog, mo, notebook_tables, stage_schema_selectors):
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
                label="",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _tables,
            )
            for _tables in stage_table_options
        ]
    )
    return stage_table_errors, stage_table_selectors


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
                label="",
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
                label="",
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
        label="Lookback days (0: all records)",
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
    stage_table_selectors,
    stage_timestamp_selectors,
):
    _field_headers = ["Schema", "Table", "Comparison key", "Timestamp"]
    _header = mo.hstack(
        [mo.md("**Stage**"), *[mo.md(f"**{name}**") for name in _field_headers]],
        widths=[0.45, 1, 1.35, 1, 1],
        align="end",
        gap=0.5,
    )
    _rows = [_header]
    for _index in range(stage_count.value):
        _rows.append(
            mo.hstack(
                [
                    mo.md(f"**{_index + 1}**"),
                    stage_schema_selectors[_index],
                    stage_table_selectors[_index],
                    stage_comparison_selectors[_index],
                    stage_timestamp_selectors[_index],
                ],
                widths=[0.45, 1, 1.35, 1, 1],
                align="center",
                gap=0.5,
            )
        )
    _stage_layout = mo.vstack(_rows, gap=0.35)

    _metadata_errors = [
        _error
        for _error in [*stage_table_errors, *stage_column_errors]
        if _error
    ]
    _messages = []
    if _metadata_errors:
        _messages.append(
            mo.callout(mo.md("  \n".join(_metadata_errors)), kind="danger")
        )

    _query_controls = mo.hstack(
        [duplicate_policy, lookback_days, run_monitor],
        widths=[1, 1, 0.8],
        align="end",
        gap=0.75,
    )
    _left_aligned_query_controls = mo.hstack(
        [_query_controls, mo.md("")],
        widths="equal",
        align="end",
        gap=1,
    )

    mo.vstack(
        [
            mo.md("---"),
            mo.md("## Configure Stages"),
            mo.hstack(
                [stage_count, mo.md("")],
                widths=[1, 5],
                align="start",
                gap=1,
            ),
            _stage_layout,
            *_messages,
            mo.md("---"),
            mo.md("## Query Settings"),
            _left_aligned_query_controls,
        ],
        gap=0.75,
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
        _results = mo.md("")
    else:
        _results = mo.vstack(
            [
                mo.md("---"),
                mo.md("## Results"),
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
