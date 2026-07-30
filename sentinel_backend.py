"""Backend operations shared by the Sentinel marimo notebooks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
import json
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence
import unicodedata

import pandas as pd
from sqlalchemy import MetaData, Table, func, inspect as sa_inspect, select

NOTEBOOK_SCHEMA = "notebook"
PRESET_FILE = Path(__file__).resolve().parent / ".sentinel" / "presets.json"
PRESET_VERSION = 1
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
KEY_NORMALIZATION_OPTIONS = {
    "Exact": "exact",
    "Normalize text": "text",
    "Normalize filename": "filename",
}
_KEY_NORMALIZATION_STRATEGIES = frozenset(KEY_NORMALIZATION_OPTIONS.values())
_FILE_SUFFIXES = (
    ".parquet",
    ".jsonl",
    ".avro",
    ".xlsx",
    ".xlsm",
    ".csv",
    ".json",
    ".xml",
    ".txt",
    ".pdf",
    ".xls",
    ".zip",
    ".bz2",
    ".tar",
    ".gz",
    ".xz",
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


@dataclass(frozen=True)
class MonitorPresetStore:
    """Saved monitor presets plus any non-fatal loading error."""

    presets: dict[str, dict[str, Any]]
    error: str | None = None


def load_monitor_presets(path: Path = PRESET_FILE) -> MonitorPresetStore:
    """Load local monitor presets without making a corrupt file break the app."""
    if not path.exists():
        return MonitorPresetStore({})

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("version") != PRESET_VERSION:
            raise ValueError(
                f"Unsupported preset version: {payload.get('version')!r}."
            )
        raw_presets = payload.get("presets")
        if not isinstance(raw_presets, dict):
            raise ValueError("The preset file must contain a `presets` object.")

        presets: dict[str, dict[str, Any]] = {}
        for name, config in raw_presets.items():
            if not isinstance(name, str) or not isinstance(config, dict):
                raise ValueError("Every preset must have a name and configuration.")
            presets[name] = config
        return MonitorPresetStore(dict(sorted(presets.items())))
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return MonitorPresetStore({}, str(exc))


def make_monitor_preset(
    *,
    stages: Sequence[Mapping[str, Any]],
    duplicate_policy: str,
    lookback_days: int | float,
    mismatched_records_only: bool = False,
) -> dict[str, Any]:
    """Build the serializable portion of a monitor configuration."""
    return {
        "stages": [
            {
                "stage_name": stage.get("stage_name", ""),
                "schema": stage.get("schema"),
                "table": stage.get("table"),
                "comparison_column": stage.get("comparison_column"),
                "timestamp_column": stage.get("timestamp_column"),
                "key_normalization": stage.get("key_normalization", "exact"),
            }
            for stage in stages
        ],
        "duplicate_policy": duplicate_policy,
        "lookback_days": lookback_days,
        "mismatched_records_only": mismatched_records_only,
    }


def _write_monitor_presets(
    presets: Mapping[str, Mapping[str, Any]], path: Path
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": PRESET_VERSION,
        "presets": dict(sorted(presets.items())),
    }
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


def save_monitor_preset(
    name: str,
    config: Mapping[str, Any],
    path: Path = PRESET_FILE,
) -> MonitorPresetStore:
    """Create or replace a named preset in the local preset file."""
    clean_name = name.strip()
    if not clean_name:
        raise ValueError("Enter a preset name before saving.")
    if len(clean_name) > 80:
        raise ValueError("Preset names must be 80 characters or fewer.")

    store = load_monitor_presets(path)
    if store.error:
        raise ValueError(f"Cannot update the preset file: {store.error}")
    presets = dict(store.presets)
    presets[clean_name] = dict(config)
    _write_monitor_presets(presets, path)
    return MonitorPresetStore(dict(sorted(presets.items())))


def delete_monitor_preset(
    name: str, path: Path = PRESET_FILE
) -> MonitorPresetStore:
    """Delete a named preset if it exists."""
    store = load_monitor_presets(path)
    if store.error:
        raise ValueError(f"Cannot update the preset file: {store.error}")
    presets = dict(store.presets)
    presets.pop(name, None)
    _write_monitor_presets(presets, path)
    return MonitorPresetStore(dict(sorted(presets.items())))


def preset_stages(config: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Return a defensive copy of valid-looking stages from a preset."""
    if not config:
        return []
    stages = config.get("stages", [])
    if not isinstance(stages, list):
        return []
    return [dict(stage) for stage in stages if isinstance(stage, dict)]


def dropdown_label_for_value(
    options: Mapping[str, Any], desired: Any, fallback: str
) -> str:
    """Find the dropdown label whose submitted value matches a preset value."""
    return next(
        (label for label, value in options.items() if value == desired), fallback
    )


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


def key_normalization_options() -> dict[str, str]:
    """Return user-facing key-normalization labels and stored values."""
    return dict(KEY_NORMALIZATION_OPTIONS)


def normalize_comparison_key(value: Any, strategy: str = "exact") -> Any:
    """Normalize one comparison key using a deterministic strategy."""
    if pd.isna(value):
        return pd.NA

    text = str(value)
    if strategy == "exact":
        return text
    if strategy not in _KEY_NORMALIZATION_STRATEGIES:
        raise ValueError(f"Unsupported key normalization strategy: {strategy!r}.")

    normalized = unicodedata.normalize("NFKC", text).strip().casefold()
    normalized = re.sub(r"\s+", " ", normalized)
    if strategy == "filename":
        normalized = normalized.replace("\\", "/").rsplit("/", maxsplit=1)[-1]
        while normalized:
            matching_suffix = next(
                (suffix for suffix in _FILE_SUFFIXES if normalized.endswith(suffix)),
                None,
            )
            if matching_suffix is None:
                break
            normalized = normalized[: -len(matching_suffix)]

    return normalized or pd.NA


def _stage_name(stage: Mapping[str, Any]) -> str:
    custom_name = stage.get("stage_name")
    if isinstance(custom_name, str) and custom_name.strip():
        return custom_name.strip()
    return str(stage.get("table") or "")


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
        stage_name = stage.get("stage_name", "")
        schema = stage.get("schema")
        table_name = stage.get("table")
        comparison_column = stage.get("comparison_column")
        timestamp_column = stage.get("timestamp_column")
        key_normalization = stage.get("key_normalization", "exact")
        if not isinstance(stage_name, str):
            errors.append(f"Stage {position} name must be text.")
        elif len(stage_name.strip()) > 80:
            errors.append(f"Stage {position} name must be 80 characters or fewer.")
        if not schema:
            errors.append(f"Stage {position} needs a schema.")
        if not table_name:
            errors.append(f"Stage {position} needs a table.")
        if comparison_column is None:
            errors.append(f"Stage {position} needs a comparison key.")
        if timestamp_column is None:
            errors.append(f"Stage {position} needs a timestamp column.")
        if key_normalization not in _KEY_NORMALIZATION_STRATEGIES:
            errors.append(
                f"Stage {position} has an unsupported key normalization strategy."
            )

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

    result_names = [_stage_name(stage) for stage in stages]
    duplicate_names = sorted(
        {name for name in result_names if name and result_names.count(name) > 1}
    )
    if duplicate_names:
        errors.append(
            "Result stage names must be unique. Add custom names for: "
            + ", ".join(f"`{name}`" for name in duplicate_names)
            + "."
        )

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


def _short_key_repr(value: Any, limit: int = 80) -> str:
    rendered = repr(value)
    return rendered if len(rendered) <= limit else f"{rendered[: limit - 1]}…"


def _prepare_stage_keys(
    frame: pd.DataFrame,
    *,
    stage_name: str,
    output_name: str,
    marker_name: str,
    strategy: str,
    duplicate_policy: str,
) -> tuple[pd.DataFrame, list[str]]:
    """Normalize stage keys, re-aggregate duplicates, and report collisions."""
    prepared = frame.copy()
    prepared["__raw_compare_column"] = prepared["compare_column"].astype("string")
    prepared["compare_column"] = prepared["__raw_compare_column"].map(
        lambda value: normalize_comparison_key(value, strategy)
    )
    prepared = prepared.dropna(subset=["compare_column"])

    warnings: list[str] = []
    if strategy != "exact" and not prepared.empty:
        variants = prepared.groupby("compare_column", sort=False)[
            "__raw_compare_column"
        ].agg(lambda values: tuple(dict.fromkeys(str(value) for value in values)))
        collisions = variants[variants.map(len) > 1]
        if not collisions.empty:
            examples = []
            for canonical, raw_values in collisions.head(3).items():
                displayed_values = ", ".join(
                    _short_key_repr(value) for value in raw_values[:3]
                )
                examples.append(
                    f"{_short_key_repr(canonical)} from {displayed_values}"
                )
            warnings.append(
                f"{stage_name}: {strategy} normalization combined "
                f"{len(collisions)} canonical key(s). Review: "
                + "; ".join(examples)
            )

    aggregate = "max" if duplicate_policy == "latest" else "min"
    prepared = (
        prepared.groupby("compare_column", as_index=False, sort=False)[output_name]
        .agg(aggregate)
        .reset_index(drop=True)
    )
    prepared[marker_name] = True
    return prepared, warnings


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

    stage_names = [_stage_name(stage) for stage in stages]
    stage_frames: list[pd.DataFrame] = []
    marker_columns: list[str] = []
    matching_warnings: list[str] = []

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
        frame, stage_warnings = _prepare_stage_keys(
            frame,
            stage_name=output_name,
            output_name=output_name,
            marker_name=marker_name,
            strategy=stage.get("key_normalization", "exact"),
            duplicate_policy=duplicate_policy,
        )
        stage_frames.append(frame)
        matching_warnings.extend(stage_warnings)

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
    result.attrs["matching_warnings"] = matching_warnings
    return result, stage_names


def filter_mismatched_records(
    result: pd.DataFrame, stage_names: Sequence[str]
) -> pd.DataFrame:
    """Return records missing from at least one stage with a clean row index."""
    expected = ", ".join(stage_names)
    exists_in = result.get(
        "exists_in", pd.Series("", index=result.index, dtype="string")
    ).fillna("")
    complete_rows = (
        exists_in.eq(expected)
        if expected
        else pd.Series(False, index=result.index, dtype="bool")
    )
    filtered = result.loc[~complete_rows].reset_index(drop=True)
    filtered.attrs = result.attrs.copy()
    return filtered


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
            return {"backgroundColor": "rgba(52, 211, 153, 0.22)"}
        return {}

    return style_cell
