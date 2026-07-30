# Sentinel

Sentinel is a local, browser-based tool for checking whether records have moved through every stage of a data pipeline. It compares the same logical record key across 2–5 PostgreSQL or Amazon Athena tables, shows when each stage saw the record, and highlights records found in every stage.

Sentinel runs on your computer as a [marimo](https://marimo.io) application. It does not provide a hosted web application from this GitHub page.

## Before you start

You need:

- access to the PostgreSQL database or Amazon Athena environment you want to inspect;
- read permission for the schemas and tables involved;
- a terminal and a text editor; and
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) for installing and running Sentinel.

Do not put real credentials into a commit or share a configured notebook containing secrets.

## Step 1: Download Sentinel

From the GitHub repository page, select **Code**, then either:

- select **Download ZIP** and extract the downloaded file; or
- copy the repository URL and clone it with Git:

  ```bash
  git clone <repository-url>
  ```

Open a terminal and change into the downloaded repository directory:

```bash
cd <downloaded-repository-folder>
```

## Step 2: Install uv

If `uv` is not already installed, follow the [official installation guide](https://docs.astral.sh/uv/getting-started/installation/).

On macOS or Linux, you can use:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

On Windows PowerShell, you can use:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Restart your terminal if necessary, then confirm the installation:

```bash
uv --version
```

## Step 3: Configure your database connection

Choose the notebook for your database:

- [`sentinel_postgres.py`](./sentinel_postgres.py) for PostgreSQL
- [`sentinel_athena.py`](./sentinel_athena.py) for Amazon Athena

Open the chosen file in a text editor and update its connection settings near the top of the file.

### PostgreSQL

In `sentinel_postgres.py`, replace the placeholder values in `DATABASE_URL`:

```python
DATABASE_URL = _sqlalchemy.URL.create(
    drivername="postgresql+psycopg2",
    username="your_username",
    password="your_password",
    host="database.example.com",
    port=5432,
    database="your_database",
)
```

Your computer must be able to reach the database host. A VPN or SSH tunnel may be required in some environments.

### Amazon Athena

In `sentinel_athena.py`, update `ATHENA_URL` with your region, database, query-results S3 location, catalog, and workgroup:

```python
ATHENA_URL = _sqlalchemy.URL.create(
    drivername="awsathena+rest",
    host="athena.us-east-1.amazonaws.com",
    port=443,
    database="your_database",
    query={
        "s3_staging_dir": "s3://your-query-results-bucket/path/",
        "region_name": "us-east-1",
        "catalog_name": "AwsDataCatalog",
        "work_group": "primary",
    },
)
```

Athena uses the standard AWS credential chain. Configure credentials before starting Sentinel, for example with an AWS profile, environment variables, or an attached IAM role. The identity also needs permission to run Athena queries, read the selected data, and use the query-results S3 location.

## Step 4: Install the dependencies

From the repository directory, run:

```bash
uv sync
```

This creates the local environment and installs Sentinel's locked dependencies.

## Step 5: Start Sentinel

For PostgreSQL, run:

```bash
uv run marimo run sentinel_postgres.py
```

For Athena, run:

```bash
uv run marimo run sentinel_athena.py
```

Marimo should open Sentinel in your default browser. If it does not, open the local URL printed in the terminal. Keep the terminal process running while using the application; press `Ctrl+C` in the terminal to stop it.

## Step 6: Configure the pipeline stages

Stages should be ordered from the beginning of the pipeline to the end.

1. Choose the **Number of pipeline stages** (2–5).
2. Configure each stage:
   - **Stage name (optional):** A readable, unique result-column name. If left blank, Sentinel uses the table name.
   - **Schema:** The schema containing the table.
   - **Table:** The table representing this pipeline stage.
   - **Comparison key:** The column that identifies the same logical record between stages, such as `record_id` or `transaction_id`. Column names may differ between stages, but their values must identify the same records.
   - **Timestamp:** The column showing when the record reached or was processed by that stage.
3. Make sure each stage has a unique stage name. This is especially important when two stages use tables with the same name.

Changing a schema refreshes the available tables; changing a table refreshes the available columns.

## Step 7: Choose the query settings

Under **Query Settings**:

- **Duplicate handling** controls records that appear more than once in a stage:
  - **Latest timestamp** keeps the most recent timestamp.
  - **Earliest timestamp** keeps the oldest timestamp.
- **Lookback days** limits each stage to recent records based on its selected timestamp column. Enter `0` to include all available records.

Select **Run monitor**, or use `Ctrl+Enter`. Sentinel runs the queries and scrolls to the results when they are ready.

## Step 8: Read the results

The **Table** tab contains:

- `compare_column`: the normalized comparison-key value;
- one timestamp column for each configured stage; and
- `exists_in`: the stages in which the record was found.

Sentinel outer-joins the stages, so records missing from later stages remain visible. A missing stage timestamp indicates that Sentinel did not find that key in that stage. Green rows are records found in every configured stage.

Use the **Visualize** tab for interactive exploration of the result data.

## Save a monitor configuration

To reuse a setup:

1. Expand **Saved Monitors**.
2. Enter a **Preset name**.
3. Select **Save preset**.

Select a saved monitor from the **Saved monitor** list to load it. Saving with an existing name replaces that preset; **Delete** removes the selected preset.

Presets are stored locally in `.sentinel/presets.json`. They include stage and query settings but do not include database credentials.

## Troubleshooting

- **No schemas or tables appear:** Check the connection values, network/VPN access, and database permissions, then restart Sentinel.
- **Athena authentication fails:** Confirm that AWS credentials are available in the same terminal where Sentinel was started and that the region and permissions are correct.
- **A stage reports missing fields:** Select a schema, table, comparison key, and timestamp for every stage.
- **Duplicate stage-name error:** Enter unique optional stage names for tables that would otherwise produce the same result-column name.
- **Expected older records are missing:** Set **Lookback days** to `0` and run the monitor again.
- **The browser page closes or stops responding:** Check that the terminal process is still running and reopen the local URL printed there.
