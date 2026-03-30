# Currency Rates Pipeline — Spark + dbt

A data pipeline that fetches currency exchange rates from the [CurrencyBeacon API](https://currencybeacon.com/), processes them through a Medallion architecture (Bronze → Silver → Gold), and materialises a star-schema data warehouse in PostgreSQL.

Built with **Apache Spark + Delta Lake** for the ingestion and transformation layers and **dbt** for the Gold layer.

> [!NOTE]
>
> **Note on AI usage**
>
> AI assistance (Claude) was used throughout this project as a learning tool and mentor — providing guidance on project structure, explaining concepts, and helping debug issues. I explicitly asked it not to write code for me. 
>
> An exception: Claude generated a complete reference project (`est-address-pipeline` based on In-ADS [dataset](https://geoportaal.maaamet.ee/eng/spatial-data/address-data-p313.html)) at the start, which I studied line by line before building this pipeline myself. Mermaid diagrams and Python docstrings were AI-assisted. Core pipeline logic, dbt models, and integration work were written by me.

| Layer | Technology |
|-------|-----------|
| Ingestion & transformation | PySpark 3.5.5, Delta Lake 3.1.0 |
| Storage | Delta Lake (local filesystem), PostgreSQL 16 |
| Gold layer | dbt-postgres |
| Orchestration | Docker Compose, Bash |

---

## Prerequisites

- Docker & Docker Compose
- CurrencyBeacon API key (free tier is sufficient)

---

## Setup

**1. Install dependencies:**

   with `uv` ( [install instructions](https://docs.astral.sh/uv/getting-started/installation/))  

```bash  
uv sync
```

Please note that the installed versions (PySpark, dbt, Python, etc) are a older to ensure compatibility with Docker image `apache/spark:3.5.5` which is used as main platform. The code which is in the repo is mapped into the `spark` container which have all necessary components for running the pipeline.

**2. Copy the environment template and fill in your values:**

```bash
cp .env-dummy .env
```

Mandatory fields in `.env`:

```env
CURRENCYBEACON_API_KEY=your-api-key
DB_PASSWORD=your-password
POSTGRES_PASSWORD=your-password   # must match DB_PASSWORD
```

**3. Build the Docker image:**

```bash
docker compose build
```

---

## Running the Pipeline

**Start the database first (required for both options):**

```bash
docker compose up -d postgres
```

### Option A — single command

```bash
bash scripts/run_pipeline.sh
```

```bash
bash scripts/run_dbt.sh
```

### Option B — step by step

**Bronze — ingest raw data from the API**

```bash
docker compose run --remove-orphans -p 4040:4040 spark python3 ingestion/ingest_bronze.py
```

**Silver — clean and validate**

```bash
docker compose run --remove-orphans -p 4040:4040 spark python3 transformation/transform_silver.py
```

**Load silver to PostgreSQL staging**

```bash
docker compose run --remove-orphans -p 4040:4040 spark python3 scripts/load_silver_to_postgres.py
```

**Gold — run dbt dimensional models**

```bash
docker compose run --remove-orphans -p 4040:4040 spark dbt run --project-dir dbt/ --profiles-dir dbt/
```

> Spark UI is available at **http://localhost:4040** while a job is running.

---

## Scheduling

The pipeline scripts are self-contained and straightforward to automate.

**Crontab (Linux/macOS)** — simplest option, schedules the two shell scripts:

```bash
# Daily at 03:00
0 3 * * * cd /path/to/repo && bash scripts/run_pipeline.sh >> logs/cron.log 2>&1
5 3 * * * cd /path/to/repo && bash scripts/run_dbt.sh >> logs/cron.log 2>&1
```

**Apache Airflow** — the production-appropriate choice for dependency management, retries, and observability. Each pipeline step maps naturally to a task in a DAG.

---

## Architecture

| Diagram | Description |
|---------|-------------|
| [`diagrams/pipeline.md`](diagrams/pipeline.md) | Medallion data flow overview |
| [`diagrams/system-architecture.md`](diagrams/system-architecture.md) | Docker services, ports, storage layout |
| [`diagrams/etl-sequence.md`](diagrams/etl-sequence.md) | Full pipeline execution sequence |
| [`diagrams/validation.md`](diagrams/validation.md) | Silver layer cleaning, validation, and quarantine |
| [`diagrams/data_model.md`](diagrams/data_model.md) | Gold layer star schema |

### Medallion layers

| Layer | Location | Format | Description |
|-------|----------|--------|-------------|
| Bronze | `data/bronze/` | Delta Lake | Raw API responses; schema-enforced on write |
| Silver | `data/silver/` | Delta Lake | Cleaned and validated; invalid rows → quarantine |
| Staging | PostgreSQL `public.*_stage` | Tables | Bridge between Spark and dbt |
| Gold | PostgreSQL `gold.*` | Tables | Star schema ready for analysis |

Quarantine tables (`silver/currencies_quarantine`, `silver/rates_quarantine`) retain rejected rows for inspection without blocking the pipeline.

### Gold schema

| Table | Type | Description |
|-------|------|-------------|
| `dim_currencies` | Dimension | ISO 4217 currency metadata; `currency_key = short_code` |
| `dim_date` | Dimension | One row per distinct rate timestamp |
| `fact_rates` | Fact | Exchange rate per base currency, target currency, and date |

`fact_rates` references `dim_currencies` twice (base and target currency) and `dim_date`. Both dimension models are incremental; full re-runs are safe.

---

## Validation

Rules are defined in `conf/base/parameters.yml`. Key constraints:

- Currency codes must be exactly 3 characters (ISO 4217)
- Rates must be between 0.000001 and 100,000,000
- Rate dates must be ≥ 2019-01-01
- Required fields checked for null / empty

---

## Assumptions & Decisions

- **Delta Lake over plain Parquet** — ACID semantics are needed because rates are appended per-run while currencies are overwritten; Delta handles both cleanly.
- **Local Delta instead of S3** — keeps the setup self-contained. The Spark code is cloud-agnostic; swapping the path for an `s3a://` URI is the only change required.
- **PostgreSQL staging tables** — act as the interface between Spark and dbt so that dbt does not need Delta Lake support.
- **dbt incremental models** for `dim_date` and `fact_rates` — repeated runs do not reprocess existing data.
- **Quarantine rather than drop** — invalid rows are preserved for debugging. Further quarantine processing pipelines are outside of the project scope right now.
- The CurrencyBeacon free tier returns ~161 currencies. Ingesting each as a base produces ~25,921 rate pairs per run.
