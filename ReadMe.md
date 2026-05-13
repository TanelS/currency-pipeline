# Currency Rates Pipeline — Spark + dbt + AWS S3

A data pipeline that fetches currency exchange rates from the [CurrencyBeacon API](https://currencybeacon.com/), processes them through a Medallion architecture (Bronze → Silver → Gold), and materialises a star-schema data warehouse in PostgreSQL.

Built with **Apache Spark + Delta Lake** for the ingestion and transformation layers, **dbt** for the Gold layer, and **AWS S3** as the storage backend for Bronze and Silver layers.

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
| Storage | AWS S3 (Delta Lake), PostgreSQL 16 |
| Gold layer | dbt-postgres |
| Orchestration | Docker Compose, Bash |

---

## Prerequisites

- Docker & Docker Compose
- CurrencyBeacon API key (free tier is sufficient)
- AWS account with an IAM user that has S3 access
- An S3 bucket in your preferred region

---

## Setup

**1. Install dependencies:**

   with `uv` ( [install instructions](https://docs.astral.sh/uv/getting-started/installation/))

```bash
uv sync
```

Please note that the installed versions (PySpark, dbt, Python, etc) are older to ensure compatibility with Docker image `apache/spark:3.5.5` which is used as main platform. The code which is in the repo is mapped into the `spark` container which have all necessary components for running the pipeline.

**2. Copy the environment template and fill in your values:**

```bash
cp .env-dummy .env
```

Mandatory fields in `.env`:

```env
CURRENCYBEACON_API_KEY=your-api-key
DB_PASSWORD=your-password
POSTGRES_PASSWORD=your-password       # must match DB_PASSWORD

RUNNING_LOCAL=false
RUNNING_AWS=true

AWS_ACCESS_KEY_ID=your-iam-access-key
AWS_SECRET_ACCESS_KEY=your-iam-secret-key
AWS_S3_BUCKET=your-bucket-name
```

> [!IMPORTANT]
> `RUNNING_LOCAL` and `RUNNING_AWS` cannot both be `true`. The pipeline will raise an error if they are.

**3. Build the Docker image:**

> [!IMPORTANT]
>
> If you are running on x86 platform, uncomment the following line in the `docker-compose.yml` file:
> `#    platform: linux/amd64             # uncomment if needed for x86-only environments`
>
> On Apple Silicon Mac the pipeline run takes ages if that line is not commented out!

after that run:

```bash
docker compose build
```

---

## Running the Pipeline

### Data flow per step

| Step | Script | Reads from | Writes to |
|------|--------|------------|-----------|
| 1. Bronze | `ingestion/ingest_bronze.py` | CurrencyBeacon API | S3 `bronze/currencies/`, `bronze/rates/` (Delta Lake) |
| 2. Silver | `transformation/transform_silver.py` | S3 `bronze/` | S3 `silver/currencies/`, `silver/rates/` (Delta Lake) |
| 3. Staging | `scripts/load_silver_to_postgres.py` | S3 `silver/` | PostgreSQL `public.currencies_stage`, `public.rates_stage` |
| 4. Gold | dbt models | PostgreSQL staging tables | PostgreSQL `gold.dim_currencies`, `gold.dim_date`, `gold.fact_rates` |

**Start the database first:**

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

## AWS Glue Catalog Setup

The Bronze and Silver layers are queryable via Amazon Athena once the Glue catalog tables are created manually in the AWS Console. This is a one-time setup per environment.

Go to **AWS Glue → Databases → movies_db → Add table** for each table below.

> **Note:** The database is named `movies_db` for historical reasons — it was created during initial Glue exploration. The name has no functional impact.

> **Note:** Select **Parquet** as the data format for all four tables. This allows Athena to read the underlying Parquet files directly. See Assumptions & Decisions for the known limitation of this approach.

### bronze_currencies

**S3 path:** `s3://your-bucket/bronze/currencies/`

```json
[
  {"Name": "id", "Type": "int"},
  {"Name": "name", "Type": "string"},
  {"Name": "short_code", "Type": "string"},
  {"Name": "code", "Type": "string"},
  {"Name": "precision", "Type": "int"},
  {"Name": "subunit", "Type": "int"},
  {"Name": "symbol", "Type": "string"},
  {"Name": "symbol_first", "Type": "boolean"},
  {"Name": "decimal_mark", "Type": "string"},
  {"Name": "thousands_separator", "Type": "string"},
  {"Name": "_ingested_at", "Type": "timestamp"},
  {"Name": "_source_file", "Type": "string"},
  {"Name": "_batch_id", "Type": "string"}
]
```

### bronze_rates

**S3 path:** `s3://your-bucket/bronze/rates/`

```json
[
  {"Name": "curr_base", "Type": "string"},
  {"Name": "currency", "Type": "string"},
  {"Name": "rate_date", "Type": "timestamp"},
  {"Name": "rate", "Type": "decimal(20,10)"},
  {"Name": "_ingested_at", "Type": "timestamp"},
  {"Name": "_source_file", "Type": "string"},
  {"Name": "_batch_id", "Type": "string"}
]
```

### silver_currencies

**S3 path:** `s3://your-bucket/silver/currencies/`

Same schema as `bronze_currencies` above.

### silver_rates

**S3 path:** `s3://your-bucket/silver/rates/`

Same schema as `bronze_rates` above.

---

## Scheduling

**AWS Step Functions** is the AWS-native orchestration alternative — a serverless workflow engine where the pipeline is defined as a state machine. It integrates natively with EMR, Lambda, and ECS. For this project it is not practical since the pipeline runs Spark jobs in Docker, which Step Functions cannot invoke directly. In a production deployment where Spark runs on EMR, Step Functions would be the natural orchestration choice.

### Apache Airflow (local)

**Apache Airflow** is the intended orchestration tool for this pipeline. Each step maps naturally to a task in a DAG, with dependency management, retries, and observability built in. Airflow runs locally via Docker Compose as recommended by the official Airflow documentation for local development.

#### Setup

**1. Download the official Airflow Compose file:**

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```

Rename the downloaded file to `docker-compose.airflow.yml`. Then make these changes to it:

- Rename the `postgres` service to `airflow-postgres` (to avoid conflict with the pipeline's PostgreSQL) and update all references to it
- Update `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` and `AIRFLOW__CELERY__RESULT_BACKEND` — change `@postgres/` to `@airflow-postgres/` in both connection strings
- Set `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`
- Update `depends_on` references from `postgres` to `airflow-postgres`
- Change the `airflow-postgres` volume from a named volume to a bind mount: `./airflow/postgres_data:/var/lib/postgresql/data` and remove `postgres-db-volume` from the `volumes:` section at the bottom
- Add a dedicated network at the bottom to isolate Airflow from the pipeline's PostgreSQL:

```yaml
networks:
  default:
    name: airflow-network
```

**2. Create the Airflow directories:**

```
airflow/
  dags/
  logs/
  plugins/
```

**3. Generate a Fernet key** (required for encrypting Airflow credentials):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**4. Add to `.env`:**

```env
FERNET_KEY=your-generated-fernet-key
AIRFLOW_PROJ_DIR=./airflow
AIRFLOW_UID=50000
```

#### Running Airflow

**First time only — initialise the database and create admin user:**

```bash
docker compose -f docker-compose.airflow.yml up airflow-init
```

Wait for `exited with code 0` before proceeding.

**Start all Airflow services:**

```bash
docker compose -f docker-compose.airflow.yml up -d
```

Airflow UI is available at **http://localhost:8080** (default credentials: `airflow` / `airflow`).

**Stop Airflow:**

```bash
docker compose -f docker-compose.airflow.yml down
```

**Run pipeline and Airflow together:**

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
```

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
| Bronze | `s3a://your-bucket/bronze/` | Delta Lake | Raw API responses; schema-enforced on write |
| Silver | `s3a://your-bucket/silver/` | Delta Lake | Cleaned and validated; invalid rows → quarantine |
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
- **AWS S3 as storage backend** — Bronze and Silver layers are stored as Delta Lake tables in S3. The Spark code is cloud-agnostic; the storage path is the only environment-specific setting.
- **Spark runs locally** — the pipeline uses `local[*]` mode, meaning Spark runs on the developer's machine while data is written to S3. This keeps the setup self-contained without requiring a managed Spark cluster.
- **PostgreSQL staging tables** — act as the interface between Spark and dbt so that dbt does not need Delta Lake support.
- **dbt incremental models** for `dim_date` and `fact_rates` — repeated runs do not reprocess existing data.
- **Quarantine rather than drop** — invalid rows are preserved for debugging. Further quarantine processing pipelines are outside of the project scope right now.
- **Athena queries Bronze/Silver as Parquet, not native Delta** — Glue catalog tables for Bronze and Silver layers are registered as Parquet format. Athena reads the underlying Parquet files directly, bypassing the Delta transaction log. This means Athena does not benefit from Delta's time travel or snapshot isolation — it reads all Parquet files present in the folder. Full Delta Lake support via the Athena Delta connector is a future improvement.
- **PostgreSQL stands in for Redshift** — in a production pipeline the Gold layer (star schema) would live in Amazon Redshift, a columnar data warehouse optimised for analytical queries at scale. The dimensional model (`dim_currencies`, `dim_date`, `fact_rates`) is exactly the structure Redshift is designed for. PostgreSQL is used here as a cost-free equivalent; the dbt models would transfer to Redshift with only a `profiles.yml` connection change. Redshift is a paid AWS service not available on the free tier.
- **Glue and Athena are managed via AWS Console only** — the PyCharm AWS Toolkit does not support Glue catalog or Athena. The Glue tables (Bronze and Silver) were created manually in the console and are not managed from the repository. As a result the repo is partially detached from the AWS catalog layer — the pipeline writes Delta Lake files to S3 correctly, but Glue table definitions and Athena queries exist outside the codebase. Infrastructure-as-code tooling (e.g. AWS CDK or Terraform) would be the proper solution to manage these as part of the project.
- The CurrencyBeacon free tier returns ~161 currencies. Ingesting each as a base produces ~25,921 rate pairs per run.
