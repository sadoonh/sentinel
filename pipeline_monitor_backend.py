"""Backend operations shared by the pipeline-monitor marimo notebooks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from typing import Any, Callable, Mapping, Sequence

import pandas as pd
from sqlalchemy import MetaData, Table, func, inspect as sa_inspect, select

NOTEBOOK_SCHEMA = "notebook"
_TIMESTAMP_CANDIDATES = (
    "ingestion_timestamp",
    "updated_at",
    "processed_at",
    "created_at",
    "timestamp",
    "event_time",
    "date",
)
_COMPARISON_CANDIDATES = (
    "uid",
    "record_id",
    "id",
    "event_id",
    "transaction_id",
    "file_name",
    "key",
)


@dataclass(frozen=True)
class DatabaseCatalog:
    """The database metadata needed to populate the stage selectors."""

    inspector: Any | None
    default_schema: str | None
    schemas: tuple[str, ...]
    error: str | None = None


class PipelineValidationError(ValueError):
    """Raised when a submitted pipeline configuration is incomplete."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("\n".join(f"- {error}" for error in self.errors))


def inspect_database(engine: Any | None) -> DatabaseCatalog:
    """Inspect an engine without making notebook UI cells handle exceptions."""
    if engine is None:
        return DatabaseCatalog(None, None, (), "No SQLAlchemy engine is configured.")

    try:
        inspector = sa_inspect(engine)
        default_schema = inspector.default_schema_name
        schemas = [
            schema
            for schema in inspector.get_schema_names()
            if schema != "information_schema" and not schema.startswith("pg_")
        ]
        if default_schema and default_schema not in schemas:
            schemas.insert(0, default_schema)
        return DatabaseCatalog(
            inspector=inspector,
            default_schema=default_schema,
            schemas=tuple(dict.fromkeys(schemas)),
        )
    except Exception as exc:
        return DatabaseCatalog(None, None, (), str(exc))


def schema_options(catalog: DatabaseCatalog) -> dict[str, str]:
    """Return dropdown labels mapped to schema values, including notebook data."""
    options: dict[str, str] = {NOTEBOOK_SCHEMA: NOTEBOOK_SCHEMA}
    for schema in catalog.schemas:
        label = f"{schema} (default)" if schema == catalog.default_schema else schema
        options[label] = schema
    return options


def default_schema_option(catalog: DatabaseCatalog) -> str:
    """Return the dropdown label that should initially be selected."""
    if catalog.default_schema:
        return f"{catalog.default_schema} (default)"
    if catalog.schemas:
        return catalog.schemas[0]
    return NOTEBOOK_SCHEMA


def list_stage_tables(
    inspector: Any | None,
    schema: str | None,
    notebook_tables: Mapping[str, pd.DataFrame],
) -> list[str]:
    """List tables for either a database schema or the notebook schema."""
    if schema == NOTEBOOK_SCHEMA:
        return sorted(notebook_tables)
    if inspector is None or schema is None:
        return []
    return sorted(inspector.get_table_names(schema=schema))


def list_stage_columns(
    inspector: Any | None,
    schema: str | None,
    table_name: str | None,
    notebook_tables: Mapping[str, pd.DataFrame],
) -> list[Any]:
    """List columns for a selected database table or registered DataFrame."""
    if not table_name:
        return []
    if schema == NOTEBOOK_SCHEMA:
        frame = notebook_tables.get(table_name)
        return list(frame.columns) if isinstance(frame, pd.DataFrame) else []
    if inspector is None or schema is None:
        return []
    return [column["name"] for column in inspector.get_columns(table_name, schema=schema)]


def _preferred_column(columns: Sequence[Any], candidates: Sequence[str]) -> Any | None:
    by_lower_name = {str(column).lower(): column for column in columns}
    for candidate in candidates:
        if candidate in by_lower_name:
            return by_lower_name[candidate]
    return columns[0] if columns else None


def default_timestamp_column(columns: Sequence[Any]) -> Any | None:
    """Choose a likely timestamp column from a table's columns."""
    return _preferred_column(columns, _TIMESTAMP_CANDIDATES)


def default_comparison_column(columns: Sequence[Any]) -> Any | None:
    """Choose a likely record-key column from a table's columns."""
    return _preferred_column(columns, _COMPARISON_CANDIDATES)


def _stage_name(position: int, schema: str, table_name: str) -> str:
    return f"Stage {position}: {schema}.{table_name}"


def _validate_stages(
    engine: Any | None,
    stages: Sequence[Mapping[str, Any]],
    notebook_tables: Mapping[str, pd.DataFrame],
    duplicate_policy: str,
    lookback_days: int | float,
) -> None:
    errors: list[str] = []
    if not 2 <= len(stages) <= 5:
        errors.append("Choose between 2 and 5 stages.")
    if duplicate_policy not in {"latest", "earliest"}:
        errors.append("Duplicate handling must be `latest` or `earliest`.")
    if lookback_days < 0:
        errors.append("Lookback days cannot be negative.")

    for position, stage in enumerate(stages, start=1):
        schema = stage.get("schema")
        table_name = stage.get("table")
        comparison_column = stage.get("comparison_column")
        timestamp_column = stage.get("timestamp_column")
        if not schema:
            errors.append(f"Stage {position} needs a schema.")
        if not table_name:
            errors.append(f"Stage {position} needs a table.")
        if comparison_column is None:
            errors.append(f"Stage {position} needs a comparison key.")
        if timestamp_column is None:
            errors.append(f"Stage {position} needs a timestamp column.")

        if schema == NOTEBOOK_SCHEMA and table_name:
            frame = notebook_tables.get(table_name)
            if not isinstance(frame, pd.DataFrame):
                errors.append(
                    f"Notebook table `{table_name}` is missing or is not a pandas DataFrame."
                )
            elif comparison_column is not None and comparison_column not in frame.columns:
                errors.append(
                    f"Comparison key `{comparison_column}` is not in notebook table `{table_name}`."
                )
            elif timestamp_column is not None and timestamp_column not in frame.columns:
                errors.append(
                    f"Timestamp column `{timestamp_column}` is not in notebook table `{table_name}`."
                )
        elif schema and schema != NOTEBOOK_SCHEMA and engine is None:
            errors.append(f"Stage {position} uses a database table, but no engine is configured.")

    if errors:
        raise PipelineValidationError(errors)


def _load_database_stage(
    engine: Any,
    schema: str,
    table_name: str,
    comparison_column: str,
    timestamp_column: str,
    output_name: str,
    marker_name: str,
    duplicate_policy: str,
    lookback_days: int | float,
) -> pd.DataFrame:
    metadata = MetaData()
    source = Table(table_name, metadata, schema=schema, autoload_with=engine)
    comparison = source.c[comparison_column]
    timestamp = source.c[timestamp_column]
    aggregate = func.max if duplicate_policy == "latest" else func.min

    statement = (
        select(
            comparison.label("compare_column"),
            aggregate(timestamp).label("stage_timestamp"),
        )
        .where(comparison.is_not(None))
        .group_by(comparison)
    )
    if lookback_days > 0:
        cutoff = datetime.now() - timedelta(days=int(lookback_days))
        statement = statement.where(timestamp >= cutoff)

    with engine.connect() as connection:
        result = connection.execute(statement)
        frame = pd.DataFrame(result.fetchall(), columns=result.keys())

    frame["compare_column"] = frame["compare_column"].astype("string")
    frame = frame.rename(columns={"stage_timestamp": output_name})
    frame[marker_name] = True
    return frame


def _load_notebook_stage(
    source: pd.DataFrame,
    comparison_column: Any,
    timestamp_column: Any,
    output_name: str,
    marker_name: str,
    duplicate_policy: str,
    lookback_days: int | float,
) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "compare_column": source[comparison_column].astype("string"),
            "stage_timestamp": source[timestamp_column],
        }
    ).dropna(subset=["compare_column"])

    if lookback_days > 0:
        parsed_timestamps = pd.to_datetime(
            frame["stage_timestamp"], errors="coerce", utc=True
        )
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=int(lookback_days))
        frame = frame.loc[parsed_timestamps >= cutoff]

    aggregate = "max" if duplicate_policy == "latest" else "min"
    frame = (
        frame.groupby("compare_column", as_index=False, sort=False)["stage_timestamp"]
        .agg(aggregate)
        .rename(columns={"stage_timestamp": output_name})
    )
    frame[marker_name] = True
    return frame


def empty_pipeline_result(stage_names: Sequence[str] = ()) -> pd.DataFrame:
    """Create an empty result with the same columns as a completed run."""
    return pd.DataFrame(columns=["compare_column", *stage_names, "exists_in"])


def run_pipeline(
    *,
    engine: Any | None,
    stages: Sequence[Mapping[str, Any]],
    notebook_tables: Mapping[str, pd.DataFrame],
    duplicate_policy: str = "latest",
    lookback_days: int | float = 0,
) -> tuple[pd.DataFrame, list[str]]:
    """Load, aggregate, and outer-join all configured pipeline stages."""
    stages = list(stages)
    lookback_days = float(lookback_days or 0)
    _validate_stages(
        engine, stages, notebook_tables, duplicate_policy, lookback_days
    )

    stage_names = [
        _stage_name(position, stage["schema"], stage["table"])
        for position, stage in enumerate(stages, start=1)
    ]
    stage_frames: list[pd.DataFrame] = []
    marker_columns: list[str] = []

    for position, (stage, output_name) in enumerate(
        zip(stages, stage_names), start=1
    ):
        marker_name = f"__pipeline_exists_{position}"
        marker_columns.append(marker_name)
        if stage["schema"] == NOTEBOOK_SCHEMA:
            frame = _load_notebook_stage(
                source=notebook_tables[stage["table"]],
                comparison_column=stage["comparison_column"],
                timestamp_column=stage["timestamp_column"],
                output_name=output_name,
                marker_name=marker_name,
                duplicate_policy=duplicate_policy,
                lookback_days=lookback_days,
            )
        else:
            frame = _load_database_stage(
                engine=engine,
                schema=stage["schema"],
                table_name=stage["table"],
                comparison_column=stage["comparison_column"],
                timestamp_column=stage["timestamp_column"],
                output_name=output_name,
                marker_name=marker_name,
                duplicate_policy=duplicate_policy,
                lookback_days=lookback_days,
            )
        stage_frames.append(frame)

    merged = reduce(
        lambda left, right: left.merge(
            right, on="compare_column", how="outer", sort=False
        ),
        stage_frames,
    )
    merged["exists_in"] = merged.apply(
        lambda row: ", ".join(
            name
            for name, marker in zip(stage_names, marker_columns)
            if pd.notna(row.get(marker)) and bool(row.get(marker))
        ),
        axis=1,
    )
    result = (
        merged[["compare_column", *stage_names, "exists_in"]]
        .sort_values("compare_column", kind="stable")
        .reset_index(drop=True)
    )
    return result, stage_names


def make_complete_row_styler(
    result: pd.DataFrame, stage_names: Sequence[str]
) -> Callable[[str, str, Any], dict[str, str]]:
    """Create a marimo table callback that highlights fully completed records."""
    expected = ", ".join(stage_names)
    complete_row_ids = {
        str(row_id)
        for row_id, exists_in in result.get("exists_in", pd.Series(dtype="string"))
        .fillna("")
        .items()
        if exists_in == expected and bool(expected)
    }

    def style_cell(row_id: str, _column_name: str, _value: Any) -> dict[str, str]:
        if row_id in complete_row_ids:
            return {"backgroundColor": "rgba(34, 197, 94, 0.22)"}
        return {}

    return style_cell
