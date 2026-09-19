# Energy platform — landing zone + bronze ingestion

Contract-based ingestion with [dlt](https://dlthub.com/product/dlt) from dltHub.

1. **Landing zone** — raw JSONL in `/Volumes/nxp/landing/raw/<source>/`, kept for **7 days**
2. **Bronze tables** — `nxp.bronze.electricity_mix_latest` and `nxp.bronze.electricity_flows_latest` with `extracted_at`, `source_url`, and `payload`, plus dlt metadata tables in `nxp.bronze`

Silver and gold stay out of this step; they will live in a PySpark wheel that reads the same contracts.

Do not confuse this with Databricks Delta Live Tables. This project uses the Python `dlt` library.

## Layout

```text
contracts/electricity_maps.yaml    # mix source
contracts/electricity_flows.yaml   # flows source
src/energy_platform/               # dlt bronze ingestion
electricity_etl/                   # PySpark silver/gold wheel
databricks.yml                     # Databricks Asset Bundle (ETL job)
resources/jobs/electricity_etl.yml # one job task per silver/gold table
airflow/dags/                      # Airflow: ingest DAG + Databricks ETL DAG
airflow/Dockerfile                 # Airflow image (DockerOperator + Databricks)
.github/actions/deploy-etl/        # composite action: Databricks bundle deploy
.github/actions/deploy-docker/     # composite action: ingest + Airflow images
.github/workflows/deploy-etl.yml
.github/workflows/deploy-docker.yml
```

The YAML contract is the interface. Ingestion does not hardcode the API path, zones, table name, or write disposition. The PySpark wheel in `electricity_etl/` reads the same file for bronze inputs and silver/gold outputs.

## Prerequisites

1. Python 3.10+
2. An Electricity Maps API token (header `auth-token`)
3. For Unity Catalog loads: a Databricks Free Edition workspace, a SQL warehouse, and a personal access token
4. Docker Engine **24.0+** and Docker Compose **v2.24+** (Docker Desktop 4.28+ includes both)

Rotate the API token if it was pasted into chat or committed anywhere. Keep it in `.dlt/secrets.toml` only.

## Setup

```bash
poetry install --with dev
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Fill in `.dlt/secrets.toml`:

```toml
[sources.electricity_maps]
api_key = "em_..."

[destination.databricks.credentials]
server_hostname = "your-workspace.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/your_warehouse_id"
access_token = "dapi..."
catalog = "nxp"
```

The Unity Catalog target is `nxp`. Get hostname and HTTP path from the SQL warehouse **Connection details** tab.

## Run

Ingest from your laptop into Databricks (landing zone + bronze). Docker is optional; the destination is the same workspace.

```bash
poetry run energy-ingest --contract contracts/electricity_maps.yaml --destination databricks
poetry run energy-ingest --contract contracts/electricity_flows.yaml --destination databricks
```

Or both contracts:

```bash
poetry run energy-ingest --all-contracts --destination databricks
```

## Docker

The image runs `energy-ingest`. Secrets stay on the host; they are mounted at runtime and are not baked into the image.

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml   # if you have not already
cp .env.example .env                             # Airflow UI + Databricks job connection
docker compose build ingest airflow
```

Run both sources into Databricks (one-shot, no Airflow):

```bash
docker compose --profile ingest run --rm ingest
```

Call ingest from the container with explicit arguments:

```bash
docker compose --profile ingest run --rm ingest --contract contracts/electricity_maps.yaml --destination databricks
docker compose --profile ingest run --rm ingest --contract contracts/electricity_flows.yaml --destination databricks
docker compose --profile ingest run --rm ingest --all-contracts --destination databricks
```

Open a shell in the container, then call the CLI yourself:

```bash
docker compose --profile ingest run --rm --entrypoint bash ingest
energy-ingest --all-contracts --destination databricks
```

Equivalent without Compose:

```bash
docker build -t energy-platform-ingest .
docker run --rm \
  -v "$PWD/.dlt/secrets.toml:/app/.dlt/secrets.toml:ro" \
  energy-platform-ingest --all-contracts --destination databricks
```

That creates:

- Volume files: `/Volumes/nxp/landing/raw/electricity_mix_latest/*.jsonl` and `/Volumes/nxp/landing/raw/electricity_flows_latest/*.jsonl` (files older than 7 days are removed on each ingest)
- Tables `nxp.bronze.electricity_mix_latest` and `nxp.bronze.electricity_flows_latest` with columns `extracted_at`, `source_url`, `payload`
- dlt metadata tables in `nxp.bronze`: `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`

Bronze does not flatten the API JSON. The silver/gold wheel parses `payload`.

Failed API calls raise `SourceApiError`. HTTP 429 (request limit) and 5xx are retried up to 5 times, honoring `Retry-After` when present. 401/403 fail immediately.

## Reset Databricks

Drop every table and volume in catalog `nxp` (landing, bronze, silver, gold, and dlt metadata). Uses the same `.dlt/secrets.toml` warehouse credentials as ingest. Preview first, then delete:

```bash
poetry run energy-cleanup --catalog nxp
poetry run energy-cleanup --catalog nxp --yes
```

Limit to one schema, or also drop the schemas:

```bash
poetry run energy-cleanup --schema bronze --yes
poetry run energy-cleanup --catalog nxp --drop-schemas --yes
```

Without `--yes` the command only prints the `DROP` statements.

## Tests

```bash
poetry run pytest
```

Tests cover contract parsing and extract behavior with a fake HTTP client. They do not call Electricity Maps or Databricks.

## Silver / gold ETL wheel

`electricity_etl/` is a separate PySpark package. It reads bronze tables, writes **Delta tables and partitioned parquet**, and does not share the ingestion runtime.

| Layer | Mix | Flows |
|---|---|---|
| Silver | `nxp.silver.fact_electricity_mix` | `nxp.silver.fact_electricity_flows` |
| Shared dims | `nxp.silver.dim_zone`, `nxp.silver.dim_date` | same |
| Gold | `nxp.gold.electricity_mix_daily_relative` | `nxp.gold.electricity_flows_fr_imports_daily`, `nxp.gold.electricity_flows_fr_exports_daily` |
| Parquet | `/Volumes/nxp/silver/...`, `/Volumes/nxp/gold/...` | same pattern |
| Partition | Facts: `year=YYYY/month=MM/day=DD`. Dims: unpartitioned. Gold: `dt` | same pattern |

Silver is a **star schema**: facts hold measures and foreign keys (`zone`, `dt`); `dim_zone` holds zone names/country/region; `dim_date` holds the calendar attributes. Facts are deduplicated on `keys.business_keys` (`updated_at`, then `extracted_at`). Bronze tables are partitioned by ingest time (`extracted_at`). Silver facts are partitioned by the data timestamp (`datetime`). Gold products:

1. **Daily relative electricity mix** — MWh and % share per generation source, with zone metadata and reference timestamps.
2. **Daily net import/export for France** — MWh into France by source zone, and MWh out of France by destination zone (`net_import_mwh` = import − export).

Build the wheel:

```bash
cd electricity_etl
poetry install --with dev
poetry build
```

Run ETL from the repo root after `poetry install --with dev`:

```bash
poetry run electricity-etl --layer all --contracts-dir contracts
```

Or from `electricity_etl/` after `poetry install --with dev` there:

```bash
poetry run electricity-etl --layer all --contracts-dir ../contracts
```

The artifact is `electricity_etl/dist/electricity_etl-0.1.0-py3-none-any.whl`. It uses the same `contracts/` YAML as ingestion. On Databricks, install the wheel and point at that folder:

```bash
electricity-etl --layer all --contracts-dir /path/to/contracts
```

From the repo root, `--contracts-dir` is optional; the CLI walks up until it finds `contracts/`.

Local parquet-only run (no Unity Catalog tables):

```bash
cd electricity_etl
poetry run electricity-etl --layer all --no-tables --output-root ./output
```

On Databricks, omit `--output-root` and `--no-tables` so outputs go to the catalog tables and `/Volumes` paths in the contracts.

A single table (used by Databricks job tasks):

```bash
cd electricity_etl
poetry run electricity-etl --table dim_zone --contracts-dir ../contracts
poetry run electricity-etl --table fact_electricity_mix --contracts-dir ../contracts
poetry run electricity-etl --table electricity_mix_daily_relative --contracts-dir ../contracts
```

## Databricks Asset Bundle (ETL jobs)

The ETL wheel is deployed with [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html). Ingestion stays on the existing CLI/Docker path. The job has **one serverless task per table**:

- Silver in parallel: `dim_zone`, `fact_electricity_mix`, `fact_electricity_flows`
- Then `dim_date` (needs both facts)
- Gold in parallel after the silver tables each product reads: mix daily relative, France imports, France exports

Use the same workspace as ingest (`dbc-81265a1a-0ee9.cloud.databricks.com` in `.dlt/secrets.toml`), not an Azure `adb-…` profile. One-time login:

```bash
databricks auth login --host https://dbc-81265a1a-0ee9.cloud.databricks.com --profile databricks-free
```

Then from the repo root:

```bash
export DATABRICKS_CONFIG_PROFILE=databricks-free
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run electricity_etl -t dev
```

`databricks bundle deploy` builds `electricity_etl` with Poetry, uploads the wheel, syncs `contracts/`, and creates the job named **`electricity-etl`** (no `[dev <user>]` prefix). Needs Databricks CLI 0.218+ and Poetry on the machine that deploys. Airflow starts that job by name.

## GitHub Actions

Composite **actions** live in `.github/actions/`. **Workflows** call them separately:

1. **Deploy ETL** (`.github/workflows/deploy-etl.yml`) — `databricks bundle deploy` (job name `electricity-etl`)
2. **Deploy Docker** (`.github/workflows/deploy-docker.yml`) — build/push ingest + Airflow images

Add repository secrets (Settings → Secrets and variables → Actions):

- `DATABRICKS_HOST` — `https://dbc-81265a1a-0ee9.cloud.databricks.com`
- `DATABRICKS_TOKEN` — a PAT for that workspace

Run order:

1. **Actions → Deploy ETL → Run workflow**
2. **Actions → Deploy Docker → Run workflow**

Images:

- `ghcr.io/<owner>/<repo>/ingest:<sha>`
- `ghcr.io/<owner>/<repo>/airflow:<sha>`

The Docker workflow uploads `docker-deploy.env`. On the host that runs Compose:

```bash
# download docker-deploy.env from the Docker workflow run, then:
set -a && source docker-deploy.env && set +a
docker compose pull
docker compose up airflow
```

## Airflow

Two DAGs orchestrate the full path. Ingest tasks start the **ingest Docker image**; ETL tasks call the Databricks job. Secrets stay in `.dlt/secrets.toml` and `.env`. Do not put tokens in the DAG files.

| DAG | What it does |
|---|---|
| `energy_ingest` | Mix and flows ingest in **parallel**, every hour (via `energy-platform-ingest`) |
| `electricity_etl` | Starts the deployed Databricks job daily at **07:00 UTC** |

Fill `.env` (from `.env.example`) with `AIRFLOW_CONN_DATABRICKS_DEFAULT` using a PAT for the same workspace as ingest. Then:

```bash
docker compose build ingest airflow
docker compose up airflow
```

Open http://localhost:8080 (user `admin`, password from `AIRFLOW_ADMIN_PASSWORD`, default `admin`). Unpause both DAGs and trigger `energy_ingest` or `electricity_etl`.

Stop Airflow with Ctrl+C in that terminal, or:

```bash
docker compose down
```

The ingest DAG talks to Docker on the host (`/var/run/docker.sock`) and mounts `.dlt/secrets.toml` into each ingest container. Build `energy-platform-ingest` before the first DAG run. The Databricks job name is `electricity-etl`.
