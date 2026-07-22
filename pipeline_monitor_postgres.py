import marimo

__generated_with = "0.23.14"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from datetime import datetime, timedelta
    from functools import reduce
    from sqlalchemy import MetaData, Table, func, inspect as sa_inspect, select

    return (
        MetaData,
        Table,
        datetime,
        func,
        mo,
        pd,
        reduce,
        sa_inspect,
        select,
        timedelta,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Pipeline record monitor

    Follow a record across an ordered set of database tables. For every stage,
    choose the record key (the **compare column**) and the timestamp to show.
    The monitor outer-joins the stages, so records that have not reached the
    final stage remain visible.
    """)
    return


@app.cell
def _():
    import sqlalchemy

    DATABASE_URL = sqlalchemy.URL.create(
        drivername="postgresql+psycopg2",
        username="username",
        password="password",
        host="host_name",
        port=5432,
        database="database_name",
    )
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell
def _(engine, mo, sa_inspect):
    _connection_error = None
    inspector = None
    schema_names = []
    default_schema = None

    if engine is None:
        _connection_status = mo.callout(
            mo.md(
                "**Engine not configured.** Edit the `DATABASE ENGINE` cell and "
                "assign your existing SQLAlchemy engine to `engine`."
            ),
            kind="warn",
        )
    else:
        try:
            inspector = sa_inspect(engine)
            default_schema = inspector.default_schema_name
            schema_names = [
                _name
                for _name in inspector.get_schema_names()
                if _name not in {"information_schema"}
                and not _name.startswith("pg_")
            ]
            _connection_status = mo.callout(
                mo.md("**Connected.** Choose a schema and the tables that represent your stages."),
                kind="success",
            )
        except Exception as _exc:
            _connection_error = str(_exc)
            _connection_status = mo.callout(
                mo.md(f"**Could not inspect the engine:** `{_connection_error}`"),
                kind="danger",
            )
    return default_schema, inspector, schema_names


@app.cell
def _(default_schema, inspector, mo, schema_names):
    _default_label = (
        f"<default: {default_schema}>" if default_schema else "<default schema>"
    )
    _schema_options = [_default_label] + [
        _schema for _schema in schema_names if _schema != default_schema
    ]
    schema_picker = mo.ui.dropdown(
        options=_schema_options,
        value=_default_label,
        label="Schema",
        searchable=True,
        disabled=inspector is None,
    )
    schema_picker
    return (schema_picker,)


@app.cell
def _(default_schema, inspector, mo, schema_picker):
    _selected_label = schema_picker.value
    selected_schema = (
        default_schema
        if _selected_label is not None and _selected_label.startswith("<default")
        else _selected_label
    )
    _table_error = None
    if inspector is None:
        available_tables = []
    else:
        try:
            available_tables = inspector.get_table_names(schema=selected_schema)
        except Exception as _exc:
            available_tables = []
            _table_error = str(_exc)

    table_picker = mo.ui.multiselect(
        options=available_tables,
        label="Stage tables (the selected order is used as the pipeline order)",
        disabled=not available_tables,
    )
    mo.vstack(
        [
            mo.callout(_table_error, kind="danger") if _table_error else mo.md(""),
            table_picker,
        ]
    )
    return selected_schema, table_picker


@app.cell
def _(inspector, mo, selected_schema, table_picker):
    selected_tables = list(table_picker.value or [])
    column_metadata = {}
    _metadata_errors = []

    if inspector is not None:
        for _table_name in selected_tables:
            try:
                column_metadata[_table_name] = inspector.get_columns(
                    _table_name, schema=selected_schema
                )
            except Exception as _exc:
                _metadata_errors.append(f"{_table_name}: {_exc}")

    if _metadata_errors:
        _metadata_message = mo.callout(
            "; ".join(_metadata_errors), kind="danger"
        )
    elif not selected_tables:
        _metadata_message = mo.callout(
            "Choose at least one table to configure the pipeline.", kind="info"
        )
    else:
        _metadata_message = mo.md(
            f"Configuring **{len(selected_tables)}** pipeline stage(s)."
        )
    _metadata_message
    return column_metadata, selected_tables


@app.cell
def _(column_metadata, mo, selected_tables):
    def _column_names(_table_name):
        return [
            _column["name"]
            for _column in column_metadata.get(_table_name, [])
        ]


    def _find_preferred_column(_columns, _preferred):
        _lower_to_actual = {
            _column.lower(): _column
            for _column in _columns
        }

        for _candidate in _preferred:
            if _candidate in _lower_to_actual:
                return _lower_to_actual[_candidate]

        return _columns[0] if _columns else None


    def _default_timestamp(_columns):
        return _find_preferred_column(
            _columns,
            (
                "ingestion_timestamp",
                "updated_at",
                "processed_at",
                "created_at",
                "timestamp",
                "event_time",
                "date",
            ),
        )


    def _default_compare(_columns):
        return _find_preferred_column(
            _columns,
            (
                "uid",
                "record_id",
                "id",
                "event_id",
                "transaction_id",
                "file_name",
                "key",
            ),
        )


    if selected_tables:
        _form_elements = {}
        _stage_sections = []

        for _position, _table_name in enumerate(selected_tables, start=1):
            _columns = _column_names(_table_name)
            _prefix = f"stage_{_position}"

            _form_elements[f"{_prefix}_stage_name"] = mo.ui.dropdown(
                options=selected_tables,
                value=_table_name,
                label="Stage name",
                searchable=True,
                full_width=True,
            )

            _form_elements[f"{_prefix}_timestamp_column"] = mo.ui.dropdown(
                options=_columns,
                value=_default_timestamp(_columns),
                label="Timestamp column",
                searchable=True,
                full_width=True,
            )

            _form_elements[f"{_prefix}_compare_column"] = mo.ui.dropdown(
                options=_columns,
                value=_default_compare(_columns),
                label="Compare column",
                searchable=True,
                full_width=True,
            )

            _stage_sections.append(
                f"""
    <div style="
        border: 1px solid var(--sl-color-neutral-300);
        border-radius: 12px;
        padding: 18px;
        margin-bottom: 14px;
    ">
        <div style="
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 14px;
        ">
            <div>
                <div style="font-size: 0.8rem; opacity: 0.65;">
                    STAGE {_position}
                </div>
                <div style="font-size: 1.15rem; font-weight: 600;">
                    {_table_name}
                </div>
            </div>

            <code>{_table_name}</code>
        </div>

        <div style="
            display: grid;
            grid-template-columns: repeat(
                auto-fit,
                minmax(220px, 1fr)
            );
            gap: 16px;
        ">
            <div>{{{_prefix}_stage_name}}</div>
            <div>{{{_prefix}_timestamp_column}}</div>
            <div>{{{_prefix}_compare_column}}</div>
        </div>
    </div>
    """
            )

        _form_elements["duplicate_policy"] = mo.ui.dropdown(
            options={
                "Latest timestamp": "latest",
                "Earliest timestamp": "earliest",
            },
            value="Latest timestamp",
            label="Duplicate handling",
            full_width=True,
        )

        _form_elements["lookback_days"] = mo.ui.number(
            start=0,
            step=1,
            value=0,
            label="Lookback days",
            full_width=True,
        )

        _pipeline_path = " → ".join(selected_tables)

        _form_layout = mo.md(
            f"""
    ## Pipeline monitor

    **Selected pipeline:** `{_pipeline_path}`

    {"".join(_stage_sections)}

    ### Query settings

    <div style="
        display: grid;
        grid-template-columns: repeat(
            auto-fit,
            minmax(240px, 1fr)
        );
        gap: 16px;
        margin-bottom: 12px;
    ">
        <div>{{duplicate_policy}}</div>
        <div>{{lookback_days}}</div>
    </div>
    """
        )

        monitor_form = _form_layout.batch(
            **_form_elements
        ).form(
            submit_button_label="Run monitor",
            show_clear_button=False,
            bordered=False,
        )

        _form_display = monitor_form

    else:
        monitor_form = None

        _form_display = mo.callout(
            mo.md(
                """
    ### No stages selected

    Select at least one table to configure the pipeline monitor.
    """
            ),
            kind="info",
        )


    _form_display
    return (monitor_form,)


@app.cell
def _(MetaData, Table, datetime, func, pd, select, timedelta):
    def load_stage(
        engine,
        schema,
        table_name,
        compare_column,
        timestamp_column,
        stage_name,
        marker_name,
        duplicate_policy="latest",
        lookback_days=0,
    ):
        """Load one row per comparison key without interpolating SQL identifiers."""
        _metadata = MetaData()
        _source = Table(
            table_name,
            _metadata,
            schema=schema,
            autoload_with=engine,
        )
        _compare = _source.c[compare_column]
        _timestamp = _source.c[timestamp_column]
        _aggregate = func.max if duplicate_policy == "latest" else func.min

        _statement = (
            select(
                _compare.label("compare_column"),
                _aggregate(_timestamp).label("stage_timestamp"),
            )
            .where(_compare.is_not(None))
            .group_by(_compare)
        )
        if lookback_days and lookback_days > 0:
            _cutoff = datetime.now() - timedelta(days=int(lookback_days))
            _statement = _statement.where(_timestamp >= _cutoff)

        with engine.connect() as _connection:
            _result = _connection.execute(_statement)
            _frame = pd.DataFrame(_result.fetchall(), columns=_result.keys())

        # Normalizing keys lets stages use differently named/inferred key columns.
        _frame["compare_column"] = _frame["compare_column"].astype("string")
        _frame = _frame.rename(columns={"stage_timestamp": stage_name})
        _frame[marker_name] = True
        return _frame

    return (load_stage,)


@app.cell
def _(
    engine,
    load_stage,
    mo,
    monitor_form,
    pd,
    reduce,
    selected_schema,
    selected_tables,
):
    submitted_config = monitor_form.value if monitor_form is not None else None
    query_error = None
    validation_errors = []
    stage_names = []
    stage_frames = []

    if submitted_config is not None:
        _stages = []
        for _position, _table_name in enumerate(selected_tables, start=1):
            _prefix = f"stage_{_position}"
            _stages.append(
                {
                    "table": _table_name,
                    "stage_name": submitted_config.get(f"{_prefix}_stage_name"),
                    "timestamp_column": submitted_config.get(
                        f"{_prefix}_timestamp_column"
                    ),
                    "compare_column": submitted_config.get(
                        f"{_prefix}_compare_column"
                    ),
                }
            )

        _settings = {
            "duplicate_policy": submitted_config.get(
                "duplicate_policy", "latest"
            ),
            "lookback_days": submitted_config.get("lookback_days", 0),
        }
        stage_names = [
            (_stage.get("stage_name") or "").strip() for _stage in _stages
        ]

        if engine is None:
            validation_errors.append("Configure the SQLAlchemy engine first.")
        if not _stages:
            validation_errors.append("Choose at least one stage table.")
        if any(not _name for _name in stage_names):
            validation_errors.append("Every stage needs a non-empty output name.")
        if len(stage_names) != len(set(stage_names)):
            validation_errors.append("Stage output names must be unique.")
        _reserved = {"compare_column", "exists_in"}
        if _reserved.intersection(stage_names):
            validation_errors.append(
                "Stage output names cannot be `compare_column` or `exists_in`."
            )
        if any("," in _name for _name in stage_names):
            validation_errors.append("Stage output names cannot contain commas.")
        if any(_name.startswith("__pipeline_exists_") for _name in stage_names):
            validation_errors.append(
                "Stage output names cannot start with `__pipeline_exists_`."
            )
        for _stage in _stages:
            if not _stage.get("timestamp_column") or not _stage.get("compare_column"):
                validation_errors.append(
                    f"Choose both columns for stage `{_stage.get('stage_name')}`."
                )

        if not validation_errors:
            try:
                for _position, _stage in enumerate(_stages):
                    stage_frames.append(
                        load_stage(
                            engine=engine,
                            schema=selected_schema,
                            table_name=_stage["table"],
                            compare_column=_stage["compare_column"],
                            timestamp_column=_stage["timestamp_column"],
                            stage_name=_stage["stage_name"].strip(),
                            marker_name=f"__pipeline_exists_{_position}",
                            duplicate_policy=_settings.get("duplicate_policy", "latest"),
                            lookback_days=_settings.get("lookback_days", 0),
                        )
                    )
            except Exception as _exc:
                query_error = str(_exc)

    if submitted_config is None or validation_errors or query_error or not stage_frames:
        pipeline_result = pd.DataFrame(
            columns=["compare_column", *stage_names, "exists_in"]
        )
    else:
        _merged = reduce(
            lambda _left, _right: _left.merge(
                _right, on="compare_column", how="outer", sort=False
            ),
            stage_frames,
        )
        _marker_columns = [
            f"__pipeline_exists_{_position}"
            for _position in range(len(stage_names))
        ]
        _merged["exists_in"] = _merged.apply(
            lambda _row: ", ".join(
                _name
                for _name, _marker in zip(stage_names, _marker_columns)
                if pd.notna(_row.get(_marker)) and bool(_row.get(_marker))
            ),
            axis=1,
        )
        pipeline_result = (
            _merged[["compare_column", *stage_names, "exists_in"]]
            .sort_values("compare_column", kind="stable")
            .reset_index(drop=True)
        )

    if validation_errors:
        _query_message = mo.callout(
            mo.md("\n".join(f"- {_error}" for _error in validation_errors)),
            kind="danger",
        )
    elif query_error:
        _query_message = mo.callout(
            mo.md(f"**Query failed:** `{query_error}`"), kind="danger"
        )
    elif submitted_config is None:
        _query_message = mo.callout(
            "Configure the stages, then select Run monitor.", kind="info"
        )
    else:
        _query_message = mo.md("")

    _query_message
    return pipeline_result, stage_names, submitted_config


@app.cell
def _(mo, pipeline_result, stage_names, submitted_config):
    if submitted_config is None:
        _results_display = mo.md("")
    else:
        _all_stages = set(stage_names)
        _complete_row_ids = {
            str(_row_id)
            for _row_id, _exists_in in pipeline_result["exists_in"]
            .fillna("")
            .items()
            if set(filter(None, _exists_in.split(", "))) == _all_stages
            and bool(_all_stages)
        }

        def _style_pipeline_cell(_row_id, _column_name, _value):
            if _row_id in _complete_row_ids:
                return {"backgroundColor": "rgba(34, 197, 94, 0.22)"}
            return {}

        _results_display = mo.vstack(
            [
                mo.md("## Results"),
                mo.ui.table(
                    pipeline_result,
                    style_cell=_style_pipeline_cell,
                ),
            ]
        )
    _results_display
    return


@app.cell
def _(mo, pipeline_result, submitted_config):
    if submitted_config is not None and not pipeline_result.empty:
        _download_display = mo.download(
            data=pipeline_result.to_csv(index=False).encode("utf-8"),
            filename="pipeline-monitor.csv",
            label="Download CSV",
        )
    else:
        _download_display = mo.md("")
    _download_display
    return


if __name__ == "__main__":
    app.run()
