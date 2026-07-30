import marimo

__generated_with = "0.23.14"
app = marimo.App(
    width="full",
    app_title="Sentinel",
    css_file="sentinel.css",
)


@app.cell
def _():
    import importlib
    import marimo as mo
    import sentinel_backend

    backend = importlib.reload(sentinel_backend)
    return backend, mo


@app.cell
def _():
    import sqlalchemy as _sqlalchemy

    DATABASE_URL = _sqlalchemy.URL.create(
        drivername="postgresql+psycopg2",
        username="username",
        password="password",
        host="host_name",
        port=5432,
        database="database_name",
    )
    engine = _sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell
def _():
    notebook_tables = {}
    return (notebook_tables,)


@app.cell
def _(backend, engine):
    catalog = backend.inspect_database(engine)
    return (catalog,)


@app.cell
def _(backend, mo):
    _initial_store = backend.load_monitor_presets()
    get_preset_store, set_preset_store = mo.state(_initial_store)
    get_active_preset_name, set_active_preset_name = mo.state("")
    get_preset_notice, set_preset_notice = mo.state(None)
    return (
        get_active_preset_name,
        get_preset_notice,
        get_preset_store,
        set_active_preset_name,
        set_preset_notice,
        set_preset_store,
    )


@app.cell
def _(
    get_active_preset_name,
    get_preset_store,
    mo,
    set_active_preset_name,
):
    preset_store = get_preset_store()
    _active_name = get_active_preset_name()
    _options = {"New monitor": "", **{name: name for name in preset_store.presets}}
    _selected_label = _active_name if _active_name in preset_store.presets else "New monitor"
    preset_selector = mo.ui.dropdown(
        options=_options,
        value=_selected_label,
        label="Saved monitor",
        allow_select_none=False,
        searchable=True,
        full_width=True,
        on_change=set_active_preset_name,
    )
    return preset_selector, preset_store


@app.cell
def _(preset_selector, preset_store):
    selected_preset = preset_store.presets.get(preset_selector.value)
    return (selected_preset,)


@app.cell
def _(backend, mo, selected_preset):
    preset_stages = backend.preset_stages(selected_preset)
    _stage_count = len(preset_stages) if 2 <= len(preset_stages) <= 5 else 2
    stage_count = mo.ui.dropdown(
        options=[2, 3, 4, 5],
        value=_stage_count,
        label="Number of pipeline stages",
        allow_select_none=False,
        full_width=True,
    )
    mo.Html(
        """
        <header class="sentinel-brand">
          <div class="sentinel-mark" aria-hidden="true"><span></span></div>
          <div>
            <h1>Sentinel</h1>
            <div class="sentinel-eyebrow">Pipeline observability</div>
          </div>
        </header>
        """
    )
    return preset_stages, stage_count


@app.cell
def _(mo, preset_stages, stage_count):
    _initial_tables = [
        preset_stages[_position].get("table")
        if _position < len(preset_stages)
        else None
        for _position in range(stage_count.value)
    ]
    _initial_comparison_columns = [
        preset_stages[_position].get("comparison_column")
        if _position < len(preset_stages)
        else None
        for _position in range(stage_count.value)
    ]
    _initial_timestamp_columns = [
        preset_stages[_position].get("timestamp_column")
        if _position < len(preset_stages)
        else None
        for _position in range(stage_count.value)
    ]
    get_stage_tables, set_stage_tables = mo.state(_initial_tables)
    get_stage_comparison_columns, set_stage_comparison_columns = mo.state(
        _initial_comparison_columns
    )
    get_stage_timestamp_columns, set_stage_timestamp_columns = mo.state(
        _initial_timestamp_columns
    )
    return (
        get_stage_comparison_columns,
        get_stage_tables,
        get_stage_timestamp_columns,
        set_stage_comparison_columns,
        set_stage_tables,
        set_stage_timestamp_columns,
    )


@app.cell
def _(backend, catalog, mo, preset_stages, stage_count):
    _schema_options = backend.schema_options(catalog)
    _schema_default = backend.default_schema_option(catalog)
    _schema_defaults = [
        backend.dropdown_label_for_value(
            _schema_options,
            preset_stages[_position].get("schema")
            if _position < len(preset_stages)
            else None,
            _schema_default,
        )
        for _position in range(stage_count.value)
    ]
    stage_schema_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_schema_options,
                value=_default,
                label="",
                searchable=True,
                allow_select_none=False,
                full_width=True,
            )
            for _default in _schema_defaults
        ]
    )
    return (stage_schema_selectors,)


@app.cell
def _(
    backend,
    catalog,
    get_stage_tables,
    mo,
    notebook_tables,
    set_stage_tables,
    stage_schema_selectors,
):
    _remembered_tables = get_stage_tables()
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
                value=(
                    _remembered_tables[_position]
                    if _position < len(_remembered_tables)
                    and _remembered_tables[_position] in _tables
                    else (_tables[0] if _tables else None)
                ),
                label="",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _tables,
            )
            for _position, _tables in enumerate(stage_table_options)
        ],
        on_change=set_stage_tables,
    )
    return stage_table_errors, stage_table_selectors


@app.cell
def _(backend, mo, preset_stages, stage_count):
    stage_name_selectors = mo.ui.array(
        [
            mo.ui.text(
                value=(
                    preset_stages[_position].get("stage_name", "")
                    if _position < len(preset_stages)
                    and isinstance(preset_stages[_position].get("stage_name", ""), str)
                    else ""
                ),
                label="",
                placeholder="Use table name",
                full_width=True,
            )
            for _position in range(stage_count.value)
        ]
    )
    _normalization_options = backend.key_normalization_options()
    stage_key_normalization_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_normalization_options,
                value=backend.dropdown_label_for_value(
                    _normalization_options,
                    preset_stages[_position].get("key_normalization", "exact")
                    if _position < len(preset_stages)
                    else "exact",
                    "Exact",
                ),
                label="",
                allow_select_none=False,
                full_width=True,
            )
            for _position in range(stage_count.value)
        ]
    )
    return stage_key_normalization_selectors, stage_name_selectors


@app.cell
def _(
    backend,
    catalog,
    get_stage_comparison_columns,
    get_stage_timestamp_columns,
    mo,
    notebook_tables,
    set_stage_comparison_columns,
    set_stage_timestamp_columns,
    stage_schema_selectors,
    stage_table_selectors,
):
    _remembered_comparison_columns = get_stage_comparison_columns()
    _remembered_timestamp_columns = get_stage_timestamp_columns()
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
                value=(
                    _remembered_comparison_columns[_position]
                    if _position < len(_remembered_comparison_columns)
                    and _remembered_comparison_columns[_position] in _columns
                    else backend.default_comparison_column(_columns)
                ),
                label="",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _columns,
            )
            for _position, _columns in enumerate(stage_column_options)
        ],
        on_change=set_stage_comparison_columns,
    )
    stage_timestamp_selectors = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_columns,
                value=(
                    _remembered_timestamp_columns[_position]
                    if _position < len(_remembered_timestamp_columns)
                    and _remembered_timestamp_columns[_position] in _columns
                    else backend.default_timestamp_column(_columns)
                ),
                label="",
                searchable=True,
                allow_select_none=True,
                full_width=True,
                disabled=not _columns,
            )
            for _position, _columns in enumerate(stage_column_options)
        ],
        on_change=set_stage_timestamp_columns,
    )
    return (
        stage_column_errors,
        stage_comparison_selectors,
        stage_timestamp_selectors,
    )


@app.cell
def _(backend, mo, selected_preset):
    _duplicate_options = {
        "Latest timestamp": "latest",
        "Earliest timestamp": "earliest",
    }
    duplicate_policy = mo.ui.radio(
        options=_duplicate_options,
        value=backend.dropdown_label_for_value(
            _duplicate_options,
            selected_preset.get("duplicate_policy") if selected_preset else None,
            "Latest timestamp",
        ),
        label="Duplicate handling",
        inline=False,
    )
    _saved_lookback = selected_preset.get("lookback_days", 0) if selected_preset else 0
    _lookback_value = (
        int(_saved_lookback)
        if isinstance(_saved_lookback, (int, float))
        else 0
    )
    lookback_days = mo.ui.number(
        start=0,
        step=1,
        value=_lookback_value,
        label="Lookback days",
        full_width=True,
    )
    mismatched_records_only = mo.ui.switch(
        value=(
            selected_preset.get("mismatched_records_only") is True
            if selected_preset
            else False
        ),
        label="Mismatched records",
    )
    run_monitor = mo.ui.run_button(
        label="Run monitor",
        kind="success",
        tooltip="Run monitor (Ctrl+Enter)",
        full_width=False,
        keyboard_shortcut="Ctrl-Enter",
    )
    return duplicate_policy, lookback_days, mismatched_records_only, run_monitor


@app.cell
def _(mo, preset_selector):
    preset_name = mo.ui.text(
        value=preset_selector.value or "",
        label="Preset name",
        full_width=True,
    )
    save_preset_button = mo.ui.run_button(
        label="Save preset",
        kind="success",
        full_width=True,
    )
    delete_preset_button = mo.ui.run_button(
        label="Delete",
        kind="danger",
        disabled=not bool(preset_selector.value),
        full_width=True,
    )
    return delete_preset_button, preset_name, save_preset_button


@app.cell
def _(
    backend,
    delete_preset_button,
    duplicate_policy,
    lookback_days,
    mismatched_records_only,
    preset_name,
    preset_selector,
    save_preset_button,
    set_active_preset_name,
    set_preset_notice,
    set_preset_store,
    stage_comparison_selectors,
    stage_key_normalization_selectors,
    stage_name_selectors,
    stage_schema_selectors,
    stage_table_selectors,
    stage_timestamp_selectors,
):
    if save_preset_button.value:
        _stages = [
            {
                "stage_name": _custom_name,
                "schema": _schema,
                "table": _table,
                "comparison_column": _comparison,
                "timestamp_column": _timestamp,
                "key_normalization": _key_normalization,
            }
            for (
                _custom_name,
                _schema,
                _table,
                _comparison,
                _timestamp,
                _key_normalization,
            ) in zip(
                stage_name_selectors.value,
                stage_schema_selectors.value,
                stage_table_selectors.value,
                stage_comparison_selectors.value,
                stage_timestamp_selectors.value,
                stage_key_normalization_selectors.value,
            )
        ]
        _clean_name = preset_name.value.strip()
        try:
            _config = backend.make_monitor_preset(
                stages=_stages,
                duplicate_policy=duplicate_policy.value,
                lookback_days=lookback_days.value,
                mismatched_records_only=mismatched_records_only.value,
            )
            _updated_store = backend.save_monitor_preset(_clean_name, _config)
        except Exception as _exc:
            set_preset_notice(("danger", str(_exc)))
        else:
            set_preset_store(_updated_store)
            set_active_preset_name(_clean_name)
            set_preset_notice(("success", f"Saved preset **{_clean_name}**."))
    elif delete_preset_button.value and preset_selector.value:
        _deleted_name = preset_selector.value
        try:
            _updated_store = backend.delete_monitor_preset(_deleted_name)
        except Exception as _exc:
            set_preset_notice(("danger", str(_exc)))
        else:
            set_preset_store(_updated_store)
            set_active_preset_name("")
            set_preset_notice(("success", f"Deleted preset **{_deleted_name}**."))
    return


@app.cell
def _(
    delete_preset_button,
    duplicate_policy,
    get_preset_notice,
    lookback_days,
    mismatched_records_only,
    mo,
    preset_name,
    preset_selector,
    preset_store,
    run_monitor,
    save_preset_button,
    stage_column_errors,
    stage_comparison_selectors,
    stage_count,
    stage_key_normalization_selectors,
    stage_name_selectors,
    stage_schema_selectors,
    stage_table_errors,
    stage_table_selectors,
    stage_timestamp_selectors,
):
    _field_headers = [
        "Stage name (optional)",
        "Schema",
        "Table",
        "Comparison key",
        "Key handling",
        "Timestamp",
    ]
    _header = mo.hstack(
        [mo.md("**Stage**"), *[mo.md(f"**{name}**") for name in _field_headers]],
        widths=[0.35, 1, 0.9, 1.2, 1, 1, 1],
        align="end",
        gap=0.5,
    )
    _rows = [_header]
    for _index in range(stage_count.value):
        _rows.append(
            mo.hstack(
                [
                    mo.md(f"**{_index + 1}**"),
                    stage_name_selectors[_index],
                    stage_schema_selectors[_index],
                    stage_table_selectors[_index],
                    stage_comparison_selectors[_index],
                    stage_key_normalization_selectors[_index],
                    stage_timestamp_selectors[_index],
                ],
                widths=[0.35, 1, 0.9, 1.2, 1, 1, 1],
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

    _preset_messages = []
    if preset_store.error:
        _preset_messages.append(
            mo.callout(
                mo.md(f"**Could not load presets:** {preset_store.error}"),
                kind="danger",
            )
        )
    _preset_notice = get_preset_notice()
    if _preset_notice:
        _notice_kind, _notice_text = _preset_notice
        _preset_messages.append(
            mo.callout(mo.md(_notice_text), kind=_notice_kind)
        )

    _preset_controls = mo.hstack(
        [
            preset_selector,
            preset_name,
            save_preset_button,
            delete_preset_button,
            mo.md(""),
        ],
        widths=[1, 1, 0.8, 0.65, 2.55],
        align="end",
        gap=0.75,
    )
    _preset_panel = mo.accordion(
        {
            "Saved Monitors": mo.vstack(
                [
                    mo.md(
                        "Load a configuration or save the current stage setup for later."
                    ),
                    _preset_controls,
                    *_preset_messages,
                ],
                gap=0.65,
            )
        }
    ).style(
        {
            "background": "var(--sentinel-panel)",
            "border": "1px solid var(--sentinel-border)",
            "border-radius": "14px",
            "padding": "1rem 1.1rem",
        }
    )

    _query_panel_style = {
        "background": "var(--sentinel-panel)",
        "border": "1px solid var(--sentinel-border)",
        "border-radius": "10px",
        "box-sizing": "border-box",
        "min-height": "7rem",
        "padding": "0.8rem 0.9rem",
    }
    _duplicate_policy_panel = mo.vstack([duplicate_policy]).style(
        _query_panel_style
    )
    _lookback_days_panel = mo.vstack([lookback_days]).style(
        _query_panel_style
    )
    _mismatched_records_panel = mo.vstack(
        [mismatched_records_only], justify="center"
    ).style(_query_panel_style)
    _query_controls = mo.hstack(
        [
            _duplicate_policy_panel,
            _lookback_days_panel,
            _mismatched_records_panel,
            run_monitor,
        ],
        widths=[1, 1, 1, 0.5],
        align="end",
        gap=0.75,
    )
    _left_aligned_query_controls = mo.hstack(
        [_query_controls, mo.md("")],
        widths=[2, 1],
        align="end",
        gap=1,
    )

    mo.vstack(
        [
            _preset_panel,
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
    stage_key_normalization_selectors,
    stage_name_selectors,
    stage_schema_selectors,
    stage_table_selectors,
    stage_timestamp_selectors,
):
    monitor_error = None
    monitor_ran = False
    monitor_warnings = []
    stage_names = []
    pipeline_result = backend.empty_pipeline_result()

    if run_monitor.value:
        monitor_ran = True
        _stages = [
            {
                "stage_name": _custom_name,
                "schema": _schema,
                "table": _table,
                "comparison_column": _comparison,
                "timestamp_column": _timestamp,
                "key_normalization": _key_normalization,
            }
            for (
                _custom_name,
                _schema,
                _table,
                _comparison,
                _timestamp,
                _key_normalization,
            ) in zip(
                stage_name_selectors.value,
                stage_schema_selectors.value,
                stage_table_selectors.value,
                stage_comparison_selectors.value,
                stage_timestamp_selectors.value,
                stage_key_normalization_selectors.value,
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
            monitor_warnings = pipeline_result.attrs.get("matching_warnings", [])
        except Exception as _exc:
            monitor_error = str(_exc)
            pipeline_result = backend.empty_pipeline_result()
    return monitor_error, monitor_ran, monitor_warnings, pipeline_result, stage_names


@app.cell
def _(
    backend,
    mismatched_records_only,
    mo,
    monitor_error,
    monitor_ran,
    monitor_warnings,
    pipeline_result,
    stage_names,
):
    if monitor_error:
        _results = mo.callout(
            mo.md(f"**Monitor failed**\n\n{monitor_error}"), kind="danger"
        )
    elif not monitor_ran:
        _results = mo.md("")
    else:
        _visible_result = (
            backend.filter_mismatched_records(pipeline_result, stage_names)
            if mismatched_records_only.value
            else pipeline_result
        )
        _result_table = mo.ui.table(
            _visible_result,
            freeze_columns_left=["compare_column"],
            style_cell=backend.make_complete_row_styler(
                _visible_result, stage_names
            ),
        )
        _result_views = mo.ui.tabs(
            {
                "Table": _result_table,
                "Visualize": mo.ui.data_explorer(_visible_result),
            }
        )
        _scroll_to_results = mo.iframe(
            """
            <script>
              window.setTimeout(() => {
                const results = window.parent.document.getElementById(
                  "sentinel-results"
                );
                results?.scrollIntoView({ behavior: "smooth", block: "start" });
              }, 0);
            </script>
            """,
            width="0",
            height="0",
        )
        _results_heading = mo.Html(
            f'<h2 id="sentinel-results" tabindex="-1">Results</h2>{_scroll_to_results}'
        )
        _matching_notice = (
            mo.callout(
                mo.vstack(
                    [
                        mo.md("**Key matching notice**"),
                        *[
                            mo.plain_text(f"• {warning}")
                            for warning in monitor_warnings
                        ],
                    ],
                    gap=0.25,
                ),
                kind="warn",
            )
            if monitor_warnings
            else mo.md("")
        )
        _results = mo.vstack(
            [
                mo.md("---"),
                _results_heading,
                _matching_notice,
                _result_views,
            ]
        )
    _results
    return


if __name__ == "__main__":
    app.run()
