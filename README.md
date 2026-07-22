# Mosent pipeline record monitor

Interactive [marimo](https://marimo.io) notebooks that outer-join record keys across an ordered set of data-pipeline tables. Results contain the comparison key, one timestamp column per stage, and an `exists_in` summary. Records present in every selected stage are highlighted in green.

## Notebooks

- `pipeline_monitor.py` — Amazon Athena through PyAthena.
- `pipeline_monitor_postgres.py` — PostgreSQL through psycopg2.

Before running a notebook, edit its database-engine cell with the correct connection details. Do not commit real database passwords or other credentials.

## Run with uv

Synchronize the locked environment:

```bash
uv sync
```

Open the PostgreSQL notebook:

```bash
uv run marimo edit pipeline_monitor_postgres.py
```

Or open the Athena notebook:

```bash
uv run marimo edit pipeline_monitor.py
```

Run either notebook as a read-only application by replacing `edit` with `run`.

## Behavior

- Choose a schema and an ordered list of stage tables.
- For each table, choose its stage name, comparison key, and timestamp column.
- Select the earliest or latest timestamp when a key occurs more than once.
- Optionally restrict queries to a lookback window.
- Outer-join stages so records that have not reached the final stage remain visible.
- Download results as CSV.

The notebooks use SQLAlchemy reflection and expression objects rather than interpolated SQL identifiers. Comparison keys are normalized to pandas string values before stages are merged.
