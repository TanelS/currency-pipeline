# Currency Rates Pipeline — Spark + dbt + AWS S3

A data pipeline that fetches currency exchange rates from the [CurrencyBeacon API](https://currencybeacon.com/), processes them through a Medallion architecture (Bronze → Silver → Gold), and materialises a star-schema data warehouse in PostgreSQL.

Built with **Apache Spark + Delta Lake** for the ingestion and transformation layers, **dbt** for the Gold layer, and **AWS S3** as the storage backend for Bronze and Silver layers.

> [!NOTE]
>
> **Note on AI usage**
>
> AI assistance (Claude) was used throughout this project as a learning tool and mentor — providing guidance on project structure, explaining concepts, and helping debug issues.
>
> An exception: Claude generated a complete reference project (`est-address-pipeline` based on In-ADS [dataset](https://geoportaal.maaamet.ee/eng/spatial-data/address-data-p313.html)) at the start, which I studied line by line before building this pipeline myself.
>
> The following parts were written or substantially rewritten by AI: the Airflow DAG (`airflow/dags/currency_pipeline.py`), the Silver layer validation logic (`spark/utils/validation.py`), Mermaid diagrams, and Python docstrings. Core pipeline logic, ingestion, transformation, dbt models, and the overall architecture were written by me.

| Layer | Technology |
|-------|-----------|
| Ingestion & transformation | PySpark 3.5.5, Delta Lake 3.1.0 |
| Storage | AWS S3 (Delta Lake), PostgreSQL 16 |
| Gold layer | dbt-postgres |
| Orchestration | Apache Airflow 3.2.1 (Docker Compose), DockerOperator |

---

## Prerequisites

- Docker & Docker Compose
- CurrencyBeacon API key (free tier is sufficient)
- AWS account (free tier is sufficient — only S3 is required by the pipeline) with an IAM user that has S3 read/write access
- An S3 bucket in your preferred region

---

## Setup

**1. Install dependencies:**

   with `uv` ( [install instructions](https://docs.astral.sh/uv/getting-started/installation/))

```bash
uv sync
```

`uv sync` installs a local virtual environment for IDE code intelligence only — the pipeline itself runs entirely inside Docker containers. Dependency versions are pinned to match `apache/spark:3.5.5`; you do not need to run the scripts locally.

**2. Copy the environment template and fill in your values:**

```bash
cp .env-dummy .env
```

`.env-dummy` contains all environment variables with their defaults. After copying, fill in the values marked `your_*`:

```env
DB_DATABASE=currencies
DB_PORT=5443
DB_USERNAME=postgres
DB_PASSWORD=your_postgresql_password
DB_HOST=localhost
DB_SSLMODE=disable

DBT_POSTGRES_HOST=postgres
DBT_POSTGRES_PORT=5432

DOMAIN=localhost

POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgresql_password

CURRENCYBEACON_API_KEY=your_api_key
CURRENCYBEACON_API_ROOT=https://api.currencybeacon.com/v1

RUNNING_LOCAL=False
RUNNING_AWS=True

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_S3_BUCKET=your_aws_s3_bucket_name

FERNET_KEY=your-generated-fernet-key
AIRFLOW_PROJ_DIR=./airflow
AIRFLOW_UID=50000
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
docker compose run --remove-orphans spark dbt run --project-dir dbt/ --profiles-dir dbt/
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

## Lambda Health Check

`lambda/pipeline_health_check.py` is a demonstration AWS Lambda function. It inspects the Bronze and Silver S3 prefixes and returns the object count and the UTC timestamp of the most recently modified file for each layer — a lightweight sanity check that data is landing in S3.

It is not part of the pipeline execution. To use it: create a Lambda function in the AWS Console (Python 3.x runtime), paste the file contents, and set `AWS_S3_BUCKET` as an environment variable in the Lambda configuration. The IAM execution role needs `s3:ListBucket` on your bucket.

---

## Scheduling

**AWS Step Functions** is the AWS-native orchestration alternative — a serverless workflow engine where the pipeline is defined as a state machine. It integrates natively with EMR, Lambda, and ECS. For this project it is not practical since the pipeline runs Spark jobs in Docker, which Step Functions cannot invoke directly. In a production deployment where Spark runs on EMR, Step Functions would be the natural orchestration choice.

### Apache Airflow (local)

**Apache Airflow** is the intended orchestration tool for this pipeline. Each step maps to a task in a DAG (`airflow/dags/currency_pipeline.py`), with dependency management, retries, and observability built in. Airflow runs locally via Docker Compose alongside the pipeline stack.

#### Operator choice — DockerOperator

Each pipeline task runs via **DockerOperator**, which instructs Docker to start a fresh `pipeline-spark` container, execute one script, and remove the container on success.

This design was chosen deliberately:

- **Airflow stays thin** — the Airflow image only contains `apache-airflow-providers-docker`. No Spark, no Python pipeline deps, no dbt. The alternative (`SparkSubmitOperator` with `local[*]`) would require duplicating all Spark and dbt dependencies into the Airflow image.
- **Spark container is the executor** — all pipeline logic runs in the same image used for manual runs. No divergence between scheduled and manual execution.
- **Mirrors production** — on AWS, Airflow would use `ECSOperator` (run this container on ECS) or `KubernetesPodOperator`. `DockerOperator` is the local equivalent of the same pattern.

In production: change `DockerOperator` → `ECSOperator`, point at ECR instead of a local image. The DAG structure and commands remain identical.

#### Network architecture

Two Docker networks are used:

- `airflow-network` — internal to Airflow (scheduler, workers, Redis, Airflow's own PostgreSQL)
- `pipeline-network` — shared between the Spark container and the pipeline's PostgreSQL

The Airflow **worker** joins both networks. It can communicate with Airflow internals via `airflow-network` and spawn containers that join `pipeline-network` (where the pipeline's `postgres` service is reachable by hostname).

All other Airflow containers (apiserver, scheduler, dag-processor, triggerer) stay on `airflow-network` only.

#### Host path resolution

DockerOperator mounts the project directory into each spawned container. The mount source must be the absolute path **on the Docker host** — not a path inside any container.

This is resolved via `${PWD}` in `docker-compose.airflow.yml`:

```yaml
PIPELINE_HOST_PATH: ${PWD}
```

Docker Compose expands `${PWD}` from the shell's working directory at the moment `docker compose up` is run. Since the user always starts from the project root, `${PWD}` is always the correct absolute path — on Linux, Mac, and Windows with Docker Desktop — without any hardcoding or manual configuration.

#### Windows `.env` line endings

The `.env` file on Windows has CRLF line endings. When the dbt task sources `.env` inside a Linux container, each variable gets a trailing `\r`, causing authentication failures. The dbt command strips carriage returns before sourcing:

```bash
set -a && source <(tr -d '\r' < /app/.env) && set +a && dbt run ...
```

This handles both CRLF (Windows) and LF (Linux/Mac) `.env` files identically.

#### Setup

**1. `docker-compose.airflow.yml` is already in the repo — no download needed.**

It was derived from the [official Airflow Docker Compose file](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml) with the following changes applied (listed here for reference):

- Renamed the `postgres` service to `airflow-postgres` (avoids hostname conflict with the pipeline's PostgreSQL) and updated all references in `SQL_ALCHEMY_CONN`, `RESULT_BACKEND`, and `depends_on`
- Changed `@postgres/` to `@airflow-postgres/` in both connection strings
- Set `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`
- Changed the `airflow-postgres` volume from a named volume to a bind mount: `./airflow/postgres_data:/var/lib/postgresql/data` and removed `postgres-db-volume` from the bottom `volumes:` section
- Added a dedicated internal network and the shared pipeline network:

```yaml
networks:
  default:
    name: airflow-network
  pipeline-network:
    external: true
    name: pipeline-network
```

- Added the Docker socket and explicit volumes to the `airflow-worker` service (the socket must only be on the worker, not all services — adding it to `x-airflow-common` would expose it unnecessarily):

```yaml
airflow-worker:
  <<: *airflow-common
  command: celery worker
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - /var/run/docker.sock:/var/run/docker.sock
  networks:
    - default
    - pipeline-network
```

> Note: defining `volumes:` on the worker overrides the inherited `*airflow-common` volumes entirely (YAML merge does not deep-merge lists). All four Airflow volume mounts must be repeated explicitly.

- Added to `x-airflow-common` environment to cancel out pipeline-specific `.env` variables and inject the host path:

```yaml
DB_HOST: ""
DB_PORT: ""
DBT_POSTGRES_HOST: ""
DBT_POSTGRES_PORT: ""
PIPELINE_HOST_PATH: ${PWD}
```

**2. Build the custom Airflow image:**

`Dockerfile.airflow` extends the official Airflow image with only the Docker provider:

```dockerfile
FROM apache/airflow:3.2.1
RUN pip install apache-airflow-providers-docker
```

```bash
docker compose -f docker-compose.airflow.yml build
```

**3. Create the Airflow directories:**

```
airflow/
  config/
  dags/
  logs/
  plugins/
```

**4. Generate a Fernet key** (required for encrypting Airflow credentials):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**5. Add to `.env`:**

```env
FERNET_KEY=your-generated-fernet-key
AIRFLOW_PROJ_DIR=./airflow
AIRFLOW_UID=50000
```

#### Running Airflow

The pipeline stack must start **before** Airflow because it creates `pipeline-network` and builds the `pipeline-spark` image that DockerOperator references.

**Step 1 — start the pipeline stack (creates the network and builds the image):**

```bash
docker compose up -d
```

**Step 2 — first time only, initialise Airflow's database and admin user:**

```bash
docker compose -f docker-compose.airflow.yml up airflow-init
```

Wait for `exited with code 0` before proceeding.

**Step 3 — start all Airflow services:**

```bash
docker compose -f docker-compose.airflow.yml up -d
```

Airflow UI is available at **http://localhost:8080** (default credentials: `airflow` / `airflow`).

**Stop Airflow:**

```bash
docker compose -f docker-compose.airflow.yml down
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
- **EMR Serverless not used** — Spark runs in `local[*]` mode on the developer's machine. EMR Serverless is not available on the AWS free tier; in a production setup it would be the natural managed execution layer for the Spark jobs.
- **PostgreSQL stands in for Redshift** — in a production pipeline the Gold layer (star schema) would live in Amazon Redshift, a columnar data warehouse optimised for analytical queries at scale. The dimensional model (`dim_currencies`, `dim_date`, `fact_rates`) is exactly the structure Redshift is designed for. PostgreSQL is used here as a cost-free equivalent; the dbt models would transfer to Redshift with only a `profiles.yml` connection change. Redshift is a paid AWS service not available on the free tier.
- **Glue and Athena are managed via AWS Console only** — the PyCharm AWS Toolkit does not support Glue catalog or Athena. The Glue tables (Bronze and Silver) were created manually in the console and are not managed from the repository. As a result the repo is partially detached from the AWS catalog layer — the pipeline writes Delta Lake files to S3 correctly, but Glue table definitions and Athena queries exist outside the codebase. Infrastructure-as-code tooling (e.g. AWS CDK or Terraform) would be the proper solution to manage these as part of the project.
- The CurrencyBeacon free tier returns ~161 currencies. Ingesting each as a base produces ~25,921 rate pairs per run.
