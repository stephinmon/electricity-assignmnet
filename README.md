# Energy platform

Contract-based Electricity Maps pipeline: **dltHub dlt** ingest into Unity Catalog landing + bronze, then a **PySpark** wheel for silver/gold, orchestrated by **Airflow in Docker** and a Databricks job named `electricity-etl`.

This is **not** Databricks Delta Live Tables. Ingest uses the Python `dlt` library.

**Code for review:** [https://github.com/stephinmon/electricity-assignmnet](https://github.com/stephinmon/electricity-assignmnet)

---

## Evaluation: run this in your environment

Follow the sections **in order**. Run **one command at a time**. Do not paste two commands on the same line.

There are two ways to run it after you clone and fill secrets:

| Path | Use when |
|---|---|
| **A — local Docker images** | Fastest review. No GitHub Actions. |
| **B — GitHub Actions + GHCR** | You fork/push to your GitHub, deploy the Databricks job and images from Actions, then pull those images to your laptop. |

Path B still needs Path A’s secret files on the laptop (`secrets.toml` and `.env`). Actions never receive the Electricity Maps token.

### 1. Software

Install these **before** clone:

| Software | Version | Why |
|---|---|---|
| Git | any recent | Clone the repo |
| Docker Engine | **24.0+** | Ingest + Airflow containers |
| Docker Compose | **v2.24+** | `docker compose` (plugin, not `docker-compose`) |
| Docker Desktop | **4.28+** on Mac/Windows | Includes Engine + Compose |

Optional (only for tests, local CLI ingest, or local `databricks bundle` without Actions):

| Software | Version |
|---|---|
| Python | **3.10, 3.11, or 3.12** (`python3 --version`) |
| Poetry | **1.8+** |
| Databricks CLI | **0.218+** (`databricks -v`) |

Check Docker:

```bash
docker version
docker compose version
```

You must see `Docker Compose version v2…`. If the command is `docker-compose` (hyphen) only, install Compose v2.

On Linux, your user must be able to run `docker` without sudo (docker group), or prefix docker commands with `sudo`.

### 2. Accounts and values to collect

You need **four** values. Keep them in a notes file; you will paste them into local files and (for Path B) GitHub secrets. **Never commit them.**

**A. Electricity Maps API token**

1. Create a token at [Electricity Maps](https://www.electricitymaps.com/) (API / token page).
2. It is sent as HTTP header `auth-token`.
3. It usually looks like `em_…`.

**B. Databricks workspace URL (Free Edition is fine)**

1. Open your workspace in the browser.
2. Copy the URL, for example `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`.
3. You will use:
   - **Host URL** (with `https://`) for GitHub secret `DATABRICKS_HOST` and `databricks.yml`
   - **Hostname only** (no `https://`) for `.dlt/secrets.toml` `server_hostname` and for the Airflow connection

**C. Databricks personal access token (PAT)**

1. Workspace → your user → **Settings** → **Developer** → **Access tokens** → **Generate new token**.
2. Copy it once. It usually starts with `dapi`.
3. The token must be able to: use a SQL warehouse, create schemas/tables/volumes in catalog `nxp`, and create/run Jobs.

**D. SQL warehouse HTTP path**

1. Workspace → **SQL** → **SQL warehouses**.
2. Create a warehouse if you have none (serverless is fine). **Start it** and wait until state is running.
3. Open the warehouse → **Connection details**.
4. Copy **HTTP path**, for example `/sql/1.0/warehouses/abc123def456`.

Also create the catalog once, in a Databricks SQL editor (while the warehouse is running):

```sql
CREATE CATALOG IF NOT EXISTS nxp;
```

Leave the warehouse **running** while you ingest or run ETL.

### 3. Get the code

Clone the review repo:

```bash
git clone https://github.com/stephinmon/electricity-assignmnet.git
cd electricity-assignmnet
```

To deploy from **your** GitHub (Path B), fork that repository in the GitHub UI, then clone **your fork** instead:

```bash
git clone https://github.com/<your-github-user>/<your-repo>.git
cd <your-repo>
```

### 4. Files you must change (do this on every machine)

These files are gitignored or must be edited. They are not in the image.

**4a. Databricks bundle host**

Open `databricks.yml` and set `workspace.host` to **your** workspace URL (with `https://`):

```yaml
workspace:
  host: https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

If you leave the original host, `databricks bundle deploy` and **Deploy ETL** will target the author’s workspace and fail with 403.

**4b. Ingest / ETL secrets**

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Edit `.dlt/secrets.toml` (no quotes around values, no `https://` on `server_hostname`):

```toml
[sources.electricity_maps]
api_key = "em_your_token"

[destination.databricks.credentials]
server_hostname = "dbc-xxxxxxxx-xxxx.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/your_warehouse_id"
access_token = "dapi_your_token"
catalog = "nxp"
```

**4c. Airflow connection**

```bash
cp .env.example .env
```

Edit `.env`. Same PAT and hostname as above. **No `https://` in the connection host:**

```bash
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://token:dapi_your_token@dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

If the PAT contains `@`, `:`, `/`, or `#`, URL-encode those characters.

Confirm the secret files exist (names only; this must not print token values):

```bash
test -f .dlt/secrets.toml && test -f .env && echo "secret files ready"
```

You should see `secret files ready`.

---

## Path A — build and run on your machine (no GitHub Actions)

Use this if you only need to review and run. You still need a Databricks workspace (step 2) and the files from step 4.

### A1. Build images

From the **repo root**:

```bash
docker compose build ingest airflow
```

Wait until both images finish. First build of Airflow can take several minutes.

### A2. Deploy the Databricks ETL job

The Airflow DAG `electricity_etl` starts a workspace job named **`electricity-etl`**. That job must exist before you trigger the DAG.

**Option 1 — GitHub Actions (Path B below)** after you push to your fork.

**Option 2 — from your laptop** (needs Databricks CLI + Poetry + Python 3.11):

```bash
pipx install poetry==1.8.5
```

Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), then:

```bash
export DATABRICKS_HOST="https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token"
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

`validate` must print `Validation OK!`. `deploy` creates job **`electricity-etl`**. Confirm in the workspace: **Workflows → Jobs → electricity-etl**.

### A3. Run ingest once (optional smoke test)

Warehouse must be running. This writes landing JSONL and bronze tables:

```bash
docker compose --profile ingest run --rm ingest
```

Expected: container exits 0. In the workspace, check:

- Volume `/Volumes/nxp/landing/raw/electricity_mix_latest/`
- Volume `/Volumes/nxp/landing/raw/electricity_flows_latest/`
- Tables `nxp.bronze.electricity_mix_latest` and `nxp.bronze.electricity_flows_latest`

### A4. Start Airflow

```bash
docker compose up airflow
```

Wait until the log shows the standalone UI is up (often 1–2 minutes the first time). Leave this terminal running.

Open [http://localhost:8080](http://localhost:8080)

- User: `admin`
- Password: value of `AIRFLOW_ADMIN_PASSWORD` in `.env` (default `admin`)

Unpause **`energy_ingest`** and **`electricity_etl`**.

1. Trigger **`energy_ingest`** first. Both mix and flows tasks should succeed.
2. Trigger **`electricity_etl`**. It starts Databricks job `electricity-etl` and waits.

Stop Airflow: Ctrl+C in that terminal, then:

```bash
docker compose down
```

---

## Path B — your GitHub, Actions, then GHCR images on your laptop

Do **step 4** on your laptop first (secrets.toml + .env + `databricks.yml` host). Then use **your fork** so Actions deploy to **your** workspace and **your** GHCR namespace.

### B1. Push to your GitHub

If you cloned the review URL, add your fork as a remote (or clone the fork from the start):

```bash
git remote add myfork https://github.com/<your-github-user>/<your-repo>.git
git push -u myfork HEAD:main
```

Include the `databricks.yml` host change in what you push. **Do not** commit `.env` or `.dlt/secrets.toml`.

### B2. GitHub Actions secrets

In **your** repo: **Settings → Secrets and variables → Actions → New repository secret**.

| Name | Value |
|---|---|
| `DATABRICKS_HOST` | `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com` (include `https://`) |
| `DATABRICKS_TOKEN` | your PAT (`dapi…`) |

No Electricity Maps token in GitHub. That stays in local `.dlt/secrets.toml`.

Enable Actions if the fork has them disabled: **Actions → I understand my workflows, go ahead and enable them**.

**Unit tests** run automatically on push and pull request (`.github/workflows/unit-tests.yml`). **Deploy ETL** and **Deploy Docker** run the same tests first and skip deploy if pytest fails. No Databricks or Electricity Maps credentials are required for tests.

### B3. Run Deploy ETL

1. **Actions → Deploy ETL → Run workflow**
2. Target: `prod` (or `dev`; both create job name `electricity-etl`)
3. Wait for green.

If this fails with `403` or `Invalid token`, the PAT or `DATABRICKS_HOST` does not match `databricks.yml` `workspace.host`.

If this fails with `must set workspace.root_path`, you are not on the current `databricks.yml` (pull latest).

### B4. Run Deploy Docker

1. **Actions → Deploy Docker → Run workflow**
2. Wait for green.
3. Open the successful run → **Artifacts → docker-deploy-env** → download and unzip. You get a file `docker-deploy.env` with two lines:

```bash
INGEST_IMAGE=ghcr.io/<your-github-user>/<your-repo>/ingest:<7-char-sha>
AIRFLOW_IMAGE=ghcr.io/<your-github-user>/<your-repo>/airflow:<7-char-sha>
```

Owner and repo in that URL are **lowercase**.

The same tags are also published as `:latest`:

- `ghcr.io/<your-github-user>/<your-repo>/ingest:latest`
- `ghcr.io/<your-github-user>/<your-repo>/airflow:latest`

### B5. Allow Docker to pull from GHCR

GitHub Container Registry packages are **private by default**. On your laptop:

```bash
docker login ghcr.io
```

- Username: your GitHub username
- Password: a GitHub PAT with **`read:packages`** (classic) or a token that can read packages. A GitHub **password will not work**.

Create the PAT: GitHub → **Settings → Developer settings → Personal access tokens**.

Optional: make the two packages public (**your repo → Packages → ingest / airflow → Package settings → Change visibility**) so `docker pull` works without login.

### B6. Pull images and start Airflow

Copy `docker-deploy.env` into the **repo root** (same folder as `docker-compose.yml`).

```bash
cd /path/to/electricity-assignmnet
```

Load the image names, pull, then start Airflow. **Three separate commands:**

```bash
set -a && source docker-deploy.env && set +a
```

```bash
docker compose pull
```

```bash
docker compose up airflow
```

`source docker-deploy.env` must be in the **same shell** as `pull` and `up`, or export the two variables yourself:

```bash
export INGEST_IMAGE=ghcr.io/<your-github-user>/<your-repo>/ingest:<7-char-sha>
export AIRFLOW_IMAGE=ghcr.io/<your-github-user>/<your-repo>/airflow:<7-char-sha>
docker compose pull
docker compose up airflow
```

Compose uses those names for the Airflow service **and** for the ingest containers the DAG starts. `.env` still supplies `AIRFLOW_CONN_DATABRICKS_DEFAULT`. `.dlt/secrets.toml` must exist on the host; Compose bind-mounts it.

Then use the UI as in **A4**.

Pull only (no compose) if you want to inspect tags:

```bash
docker pull ghcr.io/<your-github-user>/<your-repo>/ingest:latest
docker pull ghcr.io/<your-github-user>/<your-repo>/airflow:latest
```

---

## What to expect in Databricks after a full run

| Layer | Where | Write |
|---|---|---|
| Landing | `/Volumes/nxp/landing/raw/electricity_mix_latest/*.jsonl` and `…/electricity_flows_latest/*.jsonl` | replace files, 7-day retention |
| Bronze | `nxp.bronze.electricity_mix_latest`, `nxp.bronze.electricity_flows_latest` | append |
| Silver | `nxp.silver.fact_electricity_mix`, `fact_electricity_flows`, `dim_zone`, `dim_date` | merge on business keys |
| Gold | `nxp.gold.electricity_mix_daily_relative`, `electricity_flows_fr_imports_daily`, `electricity_flows_fr_exports_daily` | overwrite |

Airflow schedules (after you unpause): ingest **hourly**, ETL **07:00 UTC**. Manual trigger is enough for evaluation.

---

## Optional: Python on the laptop (no Docker)

Only if you want unit tests or ingest without Compose.

```bash
poetry install --with dev
poetry run pytest
cd electricity_etl
poetry install --with dev
poetry run pytest
cd ..
```

```bash
poetry run energy-ingest --all-contracts --destination databricks
```

```bash
cd electricity_etl
poetry install --with dev
cd ..
poetry run electricity-etl --layer all --contracts-dir contracts
```

Local ETL talks to the SQL warehouse with the same `.dlt/secrets.toml`. Warehouse must be running.

---

## Reset Databricks

Preview, then delete catalog objects:

```bash
poetry run energy-cleanup --catalog nxp
poetry run energy-cleanup --catalog nxp --yes
```

---

## If something fails

| Symptom | Fix |
|---|---|
| `403` / Invalid token on bundle deploy | PAT, `DATABRICKS_HOST`, and `databricks.yml` `workspace.host` must be the **same** workspace. |
| `Catalog 'nxp' not found` | Run `CREATE CATALOG IF NOT EXISTS nxp;` and start the warehouse. |
| Warehouse timeout / HTTP 404 | Start the SQL warehouse; `http_path` must match **Connection details**. |
| Electricity Maps `401` / `403` | Wrong `api_key` in `.dlt/secrets.toml`. |
| `docker pull` denied / not found | `docker login ghcr.io` with a **packages** PAT, or run **Deploy Docker** on **your** fork (your `ghcr.io/user/repo/...`). |
| `docker pull` requires 1 argument | You pasted two commands on one line. Run `docker pull` once per image. |
| `no matching manifest for linux/arm64` | Use images from this repo’s **Deploy Docker** workflow (amd64+arm64). Or `docker compose build` on Path A. |
| Airflow ETL: job `electricity-etl` not found | Run **Deploy ETL** or local `databricks bundle deploy` first. |
| Airflow ingest cannot see secrets | `.dlt/secrets.toml` must exist on the **host** at repo root. Restart `docker compose up airflow`. |
| Port 8080 in use | Stop the other process, or change the port mapping in `docker-compose.yml`. |

Do not commit `.dlt/secrets.toml`, `.env`, or tokens.

---

## Layout

```text
contracts/                         # mix + flows YAML (ingest and ETL)
src/energy_platform/               # dlt landing + bronze
electricity_etl/                   # PySpark silver/gold wheel
databricks.yml                     # Databricks Asset Bundle
resources/jobs/electricity_etl.yml # one serverless task per table
airflow/dags/                      # energy_ingest, electricity_etl
Dockerfile                         # ingest image
airflow/Dockerfile                 # Airflow image
.github/workflows/deploy-etl.yml
.github/workflows/deploy-docker.yml
```

Bronze columns: `extracted_at`, `source_url`, `payload` (JSON not expanded). HTTP 429/5xx are retried up to 5 times.
