# Currency Rates Pipeline — Spark + dbt + AWS S3

An **ELT** pipeline that fetches currency exchange rates from the [CurrencyBeacon API](https://currencybeacon.com/) and processes them through a Medallion architecture (Bronze → Silver → Gold), materialising a star-schema data warehouse in PostgreSQL. Raw data is loaded to S3 first and kept permanently in the Bronze layer — transformations happen after storage, not before. If a validation rule or model turns out to be wrong, the pipeline can be reprocessed from Bronze without re-fetching from the source. This matters in production: API calls can be rate-limited, costly, or slow, and fetching from operational databases adds load on systems that are not meant for bulk reads. Keeping the raw layer means the source is called exactly once per run; everything downstream is derived from what was already captured.

Built with **Apache Spark + Delta Lake** for the ingestion and transformation layers, **dbt** for the Gold layer, and **AWS S3** as the storage backend for Bronze and Silver layers.

> [!NOTE]
>
> **Note on AI usage**
>
> AI assistance (Claude) was used throughout this project as a learning tool and mentor — explaining concepts, suggesting approaches, and helping debug issues. Every AI-generated piece of code was reviewed line by line, questioned, and argued over until the logic and syntax were understood. After enough back-and-forth, the boundary between "AI wrote this" and "I wrote this" becomes genuinely blurry.
>
> Some parts are clearly AI-generated: the Spark session builder (`spark/session/builder.py`) uses Java-style chained configuration that is hard to write from memory; the Airflow DAG, Mermaid diagrams, tests, CI workflow, and Python docstrings were also produced by AI. The overall architecture, medallion layer design, ingestion and transformation logic, dbt models, and most of the README reflect my own decisions.
>
> Before starting, Claude generated a complete reference project (`est-address-pipeline` based on the In-ADS [dataset](https://geoportaal.maaamet.ee/eng/spatial-data/address-data-p313.html)), which I studied line by line before building this pipeline.

| Layer | Technology |
|-------|-----------|
| Ingestion & transformation | PySpark 3.5.5, Delta Lake 3.1.0 |
| Storage | AWS S3 (Delta Lake), PostgreSQL 16 |
| Gold layer | dbt-postgres |
| Orchestration | Apache Airflow 3.2.1 (Docker Compose), DockerOperator |

> [!TIP]
>
> If you do not have an AWS account or do not have time to set up the Glue catalog, the pipeline can be run fully locally:
> 1. In `.env` set `RUNNING_LOCAL=True` and `RUNNING_AWS=False`
> 2. Leave all `AWS_*` fields empty
> 3. Follow the regular setup and running steps below — Bronze and Silver layers will be written to `./data/` on your machine instead of S3, and the AWS Glue setup section can be skipped

---

## Prerequisites

- Docker & Docker Compose
- CurrencyBeacon API key — register at [currencybeacon.com](https://currencybeacon.com) (free tier is sufficient). The free **Developer Sandbox** plan gives 5,000 requests/month with hourly data updates. The pipeline makes ~162 requests per run (1 for `/currencies` + ~161 for `/latest?base=...`), so the free tier supports roughly 30 runs per month — enough for the default once-daily schedule, though a 31-day month at daily cadence will hit the cap.
- AWS account (free tier is sufficient — only S3 is required by the pipeline) with an IAM user and its access keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) *(not required if running locally — see the tip above)*. This project uses an IAM user with `AdministratorAccess` for development simplicity. In production this would be replaced by an IAM execution role scoped to only the following S3 permissions on the pipeline bucket:
  - `s3:PutObject` — writing Bronze and Silver Delta Lake files
  - `s3:GetObject` — reading between layers
  - `s3:DeleteObject` — required by Delta Lake for compaction and vacuum
  - `s3:ListBucket` — listing objects and checking prefixes
  - `s3:GetBucketLocation` — required by the S3 client on session init
- An S3 bucket in your preferred region — must be created before running the pipeline. See the [Terraform](#terraform) section for a minimal example of provisioning it.

---

## Setup

**1. Install dependencies (optional — for IDE code intelligence only):**

The pipeline runs entirely inside Docker containers, so you do not need a local Python environment to run it. `uv sync` is only needed if you want your IDE to resolve imports and show type hints.

   with `uv` ( [install instructions](https://docs.astral.sh/uv/getting-started/installation/))

```bash
uv sync
```

Dependency versions are pinned to match `apache/spark:3.5.5`.

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
AWS_REGION=your_aws_region        # e.g. eu-north-1, us-east-1

FERNET_KEY=your-generated-fernet-key
AIRFLOW_PROJ_DIR=./airflow
AIRFLOW_UID=50000
```

> [!IMPORTANT]
> `RUNNING_LOCAL` and `RUNNING_AWS` cannot both be `True`. The pipeline will raise an error if they are.

**3. Build the Docker image:**

> [!IMPORTANT]
>
> If you are running on x86 platform, uncomment the following line in the `docker-compose.yml` file:
> `#    platform: linux/amd64             # uncomment if needed for x86-only environments`
>
> On Apple Silicon Mac the pipeline run takes ages if that line is not commented out!

Then run:

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

> [!IMPORTANT]
> Options A and B run the pipeline in whichever mode is set in `.env`. With the default settings (`RUNNING_AWS=True`) all data is read from and written to AWS S3. To run fully locally without AWS, set `RUNNING_LOCAL=True` and `RUNNING_AWS=False` — data will be stored under `/app/data/` inside the container instead.

**Start the database first:**

```bash
docker compose up -d postgres
```

### Option A — single command

`run_pipeline.sh` runs Bronze, Silver, and the staging load in sequence. `run_dbt.sh` runs the dbt Gold models. They are equivalent to the individual steps in Option B.

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

**Snapshot — update SCD Type 2 dimension history**

```bash
docker compose run --remove-orphans spark dbt snapshot --project-dir dbt/ --profiles-dir dbt/
```

**Gold — run dbt dimensional models**

```bash
docker compose run --remove-orphans spark dbt run --project-dir dbt/ --profiles-dir dbt/
```

> **Snapshot rebuild** — `dbt snapshot` does not support `--full-refresh`. To rebuild `currencies_snapshot` from scratch (e.g. after changing `check_cols`), drop the table manually in PostgreSQL first, then re-run the snapshot:
>
> ```sql
> DROP TABLE public.currencies_snapshot;
> ```
> ```bash
> docker compose run --remove-orphans spark dbt snapshot --project-dir dbt/ --profiles-dir dbt/
> ```
>
> This wipes all SCD Type 2 history. All currencies will be re-inserted as new current records.

> **Gold full refresh** — if the Gold layer needs to be rebuilt from scratch after schema changes, run with `--full-refresh`:
>
> ```bash
> docker compose run --remove-orphans spark dbt run --full-refresh --project-dir dbt/ --profiles-dir dbt/
> ```
>
> This drops and rebuilds all Gold tables in dependency order. Bronze and Silver data in S3 is not affected.
>
> [!WARNING]
> `--full-refresh` causes data loss in `dim_date`. Because `rates_stage` is always overwritten with the current run's data only, a full refresh of `dim_date` can only repopulate dates from the latest pipeline run — all historical dates from previous runs are lost. In a production DW you would never do a full refresh on a growing dimension; you would migrate in place. Use `--full-refresh` only when a schema change makes it unavoidable, and re-run the pipeline several times afterwards to rebuild date history.

> Spark UI is available at **http://localhost:4040** while a job is running.

---

## Notebooks

Two Jupyter notebooks are provided for exploring the pipeline data outside of Spark:

| Notebook | Description |
|----------|-------------|
| `notebooks/currencies_local.ipynb` | Reads Bronze and Silver Delta Lake tables from the local `./data/` directory. Requires the pipeline to have been run with `RUNNING_LOCAL=True`. |
| `notebooks/currencies_aws.ipynb` | Reads Bronze and Silver Delta Lake tables directly from S3 using PyArrow. Requires `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_BUCKET`, and `AWS_REGION` to be set in `.env`. |

Both notebooks use `pandas` + `PyArrow` for reading — no Spark session needed. Quarantine tables are handled gracefully: if the prefix does not exist in S3 (or locally), a message is printed instead of raising an error.

---

## AWS Glue Catalog Setup

The Bronze and Silver layers are stored as Delta Lake, which consists of Parquet data files plus a `_delta_log/` transaction log directory. AWS Glue has no native Delta Lake support and cannot parse the transaction log — it cannot crawl these tables automatically. The tables must therefore be created manually in the AWS Console, defined as **Parquet** format and pointed directly at the S3 prefix. Athena then reads the underlying Parquet files directly, bypassing the Delta transaction log entirely.

This is a one-time setup per environment.

First, create a **Glue database** if you don't have one: go to **AWS Glue → Data Catalog → Databases → Add database**. A Glue database is not a real database — it is a logical namespace in the Data Catalog used to group related table definitions. Name it something descriptive, e.g. `currency_pipeline`.

> **Note:** The reference setup in this repo used a database named `movies_db` — a leftover from initial Glue exploration. Use a more descriptive name for your own setup; the name has no functional impact on the pipeline.

Then go to **AWS Glue → Data Catalog → Tables → Add table** and create each of the four tables below inside your database.

> **Note:** Select **Parquet** as the data format for all four tables. See Assumptions & Decisions for the known limitation of this approach (no Delta time travel or snapshot isolation via Athena).

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

### silver_rates

**S3 path:** `s3://your-bucket/silver/rates/`

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

---

## Lambda Health Check

`lambda/pipeline_health_check.py` is a demonstration AWS Lambda function. It inspects the Bronze and Silver S3 prefixes and returns the object count and the UTC timestamp of the most recently modified file for each layer — a lightweight sanity check that data is landing in S3.

It is not part of the pipeline execution. To use it: create a Lambda function in the AWS Console (Python 3.x runtime), paste the file contents, and set `AWS_S3_BUCKET` as an environment variable in the Lambda configuration. The IAM execution role needs `s3:ListBucket` on your bucket.

---

## Terraform

> [!IMPORTANT]
> **The Terraform code in this repository is a portfolio demonstration only. It has no practical value for running this project.** The S3 bucket must already exist before the pipeline can run — and a reviewer cannot reuse the bucket name from `terraform.tfvars` since S3 bucket names are globally unique. They would need to pick their own name, create the bucket, and set it in both `terraform.tfvars` and `.env` manually. Terraform saves no steps here; it just shows familiarity with Infrastructure-as-Code tooling.

In theory, Terraform could be more useful for automating the AWS Glue Catalog setup (databases, table definitions for Bronze and Silver layers). However, Glue requires a significant number of IAM permissions to manage via API, and the initial AWS account and IAM user setup still has to be done through the AWS Console regardless — so the practical gain would still be limited.

The `terraform/terraform.lock.hcl` is committed to pin the exact provider version. `terraform/terraform.tfvars` is gitignored — it holds credentials and the bucket name.

**Install Terraform:**

- **macOS:** `brew install terraform`
- **Windows:** `winget install HashiCorp.Terraform` (or download from [developer.hashicorp.com/terraform/install](https://developer.hashicorp.com/terraform/install))
- **Linux:** follow the [official install guide](https://developer.hashicorp.com/terraform/install) for your distro

**Configure:**

Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and fill in your values:

```hcl
aws_region            = "eu-north-1"
bucket_name           = "your_aws_s3_bucket_name"
aws_access_key_id     = "your_aws_access_key"
aws_secret_access_key = "your_aws_secret_access_key"
```

```bash
cd terraform
terraform init      # first time only — downloads the AWS provider
terraform plan
terraform apply
```

---

## Scheduling

**AWS Step Functions** is the AWS-native orchestration alternative — a serverless workflow engine where the pipeline is defined as a state machine. It integrates natively with EMR, Lambda, and ECS. For this project it is not practical since the pipeline runs Spark jobs in Docker, which Step Functions cannot invoke directly. In a production deployment where Spark runs on EMR, Step Functions would be the natural orchestration choice.

### Apache Airflow (local)

**Apache Airflow** is the intended orchestration tool for this pipeline. Each step maps to a task in a DAG (`airflow/dags/currency_pipeline.py`), with dependency management, retries, SLAs, and observability built in. Airflow runs locally via Docker Compose alongside the pipeline stack. The DAG runs as: `check_api_availability` (HttpSensor — confirms the CurrencyBeacon API is reachable before starting) → `ingest_bronze` → `transform_silver` → `load_to_postgres` → `run_dbt_snapshot` → `run_dbt`.

#### Sensor

The first task, `check_api_availability`, is an `HttpSensor` that pokes the CurrencyBeacon `/v1/status` endpoint before any pipeline work begins. It accepts HTTP 200 or 401 as success — 401 means the API is up but unauthenticated, which is sufficient to confirm reachability. If the API does not respond within 5 minutes the sensor times out and the DAG run fails, preventing a bronze ingestion attempt against an unavailable source. The sensor uses `mode='reschedule'` so it releases its Celery worker slot between pokes rather than holding it for the full timeout duration.

The API connection is configured as an Airflow connection (`currencybeacon_api`) via the `AIRFLOW_CONN_CURRENCYBEACON_API` environment variable in `docker-compose.airflow.yml` — no manual connection setup in the UI required.

#### XCom

XCom is Airflow's mechanism for passing small values between tasks (job IDs, row counts, status flags). It is not used in this pipeline because there is nothing to share between tasks — each task reads its input from storage and writes its output to storage independently. Data flows through S3 and PostgreSQL, not through Airflow.

#### Backfill

`catchup=False` is set intentionally. The pipeline fetches the current exchange rates at run time — there is no historical data available from the API to backfill with. Running missed past intervals would just re-fetch today's rates and write duplicate data. If the pipeline misses a scheduled run, the next scheduled run picks up normally; no backfill is attempted.

#### Operator choice — DockerOperator

Each pipeline task runs via **DockerOperator**, which instructs Docker to start a fresh `pipeline-spark` container, execute one script, and remove the container on success.

This design was chosen deliberately:

- **Airflow stays thin** — the Airflow image only contains `apache-airflow-providers-docker` and `apache-airflow-providers-http` (for the API sensor). No Spark, no Python pipeline deps, no dbt. The alternative (`SparkSubmitOperator` with `local[*]`) would require duplicating all Spark and dbt dependencies into the Airflow image.
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

**5. Set the Fernet key in `.env`:**

`AIRFLOW_PROJ_DIR` and `AIRFLOW_UID` are already in `.env-dummy` with correct defaults. Only `FERNET_KEY` needs to be filled in with the value generated above:

```env
FERNET_KEY=your-generated-fernet-key
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

DAGs are paused by default on first start. To activate the pipeline:
1. Open the UI and find `currency_pipeline` in the DAG list
2. Toggle the pause switch on the left to unpause it — the DAG will now run on its daily schedule
3. To trigger a run immediately without waiting for the schedule, click the **Trigger DAG** (▶) button on the right

> [!NOTE]
> The DAG is configured with `max_active_runs=1`. If you trigger it manually while a scheduled run is already in progress, the new run will queue and start automatically once the current one finishes.

**Stop Airflow:**

```bash
docker compose -f docker-compose.airflow.yml down
```

---

## Migrating to a Full AWS Stack (paid tier)

The pipeline currently uses `local[*]` Spark on the developer's machine, PostgreSQL for the Gold layer, and a locally-hosted Airflow. With access to AWS paid services the natural migration path is:

| Current | AWS equivalent |
|---------|---------------|
| `local[*]` Spark in Docker | EMR Serverless |
| PostgreSQL (Gold layer) | Amazon Redshift |
| Local Airflow (Docker Compose) | Amazon MWAA |
| IAM user access keys in `.env` | IAM execution roles (no keys) |
| `.env` secrets file | AWS Secrets Manager |

The Bronze and Silver layers (S3 + Delta Lake) and all pipeline Python scripts remain unchanged. The changes are in how jobs are submitted, where the Gold layer is hosted, and how Airflow is run.

AWS Secrets Manager would replace the `.env` file entirely — API keys, database passwords, and other credentials are stored in AWS and fetched at runtime by `config.py` via `boto3`, with access controlled by IAM roles. Example:

```python
import boto3, json

def get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager")  # region resolved automatically from IAM role context
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

secrets = get_secret("currency-pipeline/prod")
CURRENCYBEACON_API_KEY = secrets["CURRENCYBEACON_API_KEY"]
DATABASE_PASSWORD = secrets["DB_PASSWORD"]
```

No `.env` file, no credentials in the codebase. The service's IAM execution role grants access to Secrets Manager — no access keys needed anywhere.

### 1. Eliminate the manual Glue table setup

The manual Glue table creation (see [AWS Glue Catalog Setup](#aws-glue-catalog-setup)) exists because the Glue Crawler cannot parse Delta Lake files written by local Spark. On EMR Serverless you can configure the Spark session to use the **Glue Data Catalog as the Delta Lake metastore**. Delta Lake then writes table metadata directly into Glue on every job run — no Crawler, no manual table definitions.

Add to `spark/session/builder.py`:

```python
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.config("spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
```

Athena supports querying Delta tables registered this way natively (as of late 2023), so you get full Delta semantics — including time travel — rather than the Parquet-only view the current workaround provides.

### 2. Package the pipeline for EMR Serverless

EMR Serverless runs the PySpark scripts from S3, not from a local Docker mount. Two approaches:

**Option A — virtual environment archive (simpler)**

Package all pipeline dependencies into a `.venv.tar.gz` and upload to S3:

```bash
pip install venv-pack
python -m venv pyspark-env
source pyspark-env/bin/activate
pip install -r requirements.txt
venv-pack -o pyspark-env.tar.gz
aws s3 cp pyspark-env.tar.gz s3://your-bucket/emr/pyspark-env.tar.gz
```

Upload the source code as a zip too:
```bash
zip -r pipeline.zip ingestion/ transformation/ scripts/ spark/ utils/ conf/ config.py
aws s3 cp pipeline.zip s3://your-bucket/emr/pipeline.zip
```

**Option B — container image (mirrors the current Docker setup)**

Tag and push the existing `pipeline-spark` image to ECR:

```bash
aws ecr create-repository --repository-name pipeline-spark
docker tag pipeline-spark:latest <account>.dkr.ecr.<region>.amazonaws.com/pipeline-spark:latest
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker push <account>.dkr.ecr.<region>.amazonaws.com/pipeline-spark:latest
```

EMR Serverless supports custom container images since EMR 6.9 — use `applicationConfiguration` in the job run to point at the ECR image.

### 3. Update the Airflow DAG

Replace `DockerOperator` with `EmrServerlessStartJobRunOperator`. The DAG structure, schedule, and task order stay the same.

```python
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobRunOperator

bronze = EmrServerlessStartJobRunOperator(
    task_id="ingest_bronze",
    application_id="<your-emr-serverless-application-id>",
    execution_role_arn="arn:aws:iam::<account>:role/EMRServerlessExecutionRole",
    job_driver={
        "sparkSubmit": {
            "entryPoint": "s3://your-bucket/emr/pipeline.zip",
            "entryPointArguments": [],
            "sparkSubmitParameters": (
                "--conf spark.submit.pyFiles=s3://your-bucket/emr/pipeline.zip "
                "--conf spark.archives=s3://your-bucket/emr/pyspark-env.tar.gz#environment "
                "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
            ),
        }
    },
    configuration_overrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://your-bucket/emr-logs/"}
        }
    },
)
```

All required AWS credentials (`AWS_S3_BUCKET`, `AWS_ACCESS_KEY_ID`, etc.) should move from `.env` into Airflow Connections, and be referenced via `aws_conn_id` in the operator — no credentials in files.

### 4. Switch the Gold layer to Redshift

The dbt models (`dim_currencies`, `dim_date`, `fact_rates`) require no changes. Only the adapter and connection change.

**Install `dbt-redshift`** instead of `dbt-postgres`:

```bash
pip install dbt-redshift
```

In `pyproject.toml`, replace `dbt-postgres` with `dbt-redshift`.

**Update `dbt/profiles.yml`:**

```yaml
currency_pipeline:
  target: prod
  outputs:
    prod:
      type: redshift
      host: <cluster-endpoint>.redshift.amazonaws.com
      port: 5439
      user: "{{ env_var('DB_USERNAME') }}"
      password: "{{ env_var('DB_PASSWORD') }}"
      dbname: "{{ env_var('DB_DATABASE') }}"
      schema: gold
      threads: 4
```

The staging load script (`scripts/load_silver_to_postgres.py`) must also target Redshift. The JDBC URL changes from `postgresql` to `redshift`:

```python
jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"
```

Add the Redshift JDBC driver to the Spark session configuration in `spark/session/builder.py`:

```python
.config("spark.jars", "/path/to/redshift-jdbc42.jar")
```

### 5. Migrate Airflow to MWAA (optional)

If you do not want to self-host Airflow, Amazon MWAA manages the scheduler, workers, and web server.

1. Create an MWAA environment in the AWS Console pointing at an S3 bucket for DAGs.
2. Upload the DAG file: `aws s3 cp airflow/dags/currency_pipeline.py s3://your-bucket/dags/`
3. MWAA workers run on AWS — the `DockerOperator` pattern (Docker-in-Docker) no longer applies. Use `EmrServerlessStartJobRunOperator` (as above) or `ECSOperator` (if using Fargate containers).
4. Required Python packages (e.g. `apache-airflow-providers-amazon`) are declared in a `requirements.txt` uploaded to the same S3 bucket.

MWAA does not replace the pipeline execution itself — it only replaces the local Airflow scheduler and worker. The Spark jobs still need to run somewhere (EMR Serverless or ECS).

### 6. IAM roles

In production, access keys in `.env` are replaced by IAM roles attached to the execution environment:

- **EMR Serverless** — attach a job execution role with `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on the pipeline bucket. No `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` needed.
- **MWAA** — attach an execution role with permissions to invoke EMR Serverless, read DAGs from S3, and write logs.
- **Lambda** — existing health-check function already uses an execution role; no change needed.

The pipeline code uses `boto3` / `s3a://` paths — both pick up credentials from the environment automatically, whether that is a role or explicit keys. No code changes are required when switching from key-based to role-based auth.

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

#### SCD Type 2 join consideration

`dim_currencies` uses SCD Type 2 via `dbt snapshot`. If a currency's attributes ever change, a new version row is added — meaning the same `currency_key` (`short_code`) can appear multiple times with different `valid_from`/`valid_to` ranges. A plain join on `currency_key` alone would then fan out and duplicate fact rows.

The correct pattern when querying is to filter by the valid time range:

```sql
SELECT f.rate, f.rate_date, c.name, c.symbol
FROM fact_rates f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_currencies c
    ON f.currency = c.currency_key
    AND d.date BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
```

The textbook fix is a **surrogate key** — an auto-generated integer that uniquely identifies each version row. `fact_rates` would store the surrogate instead of `short_code`, making the join unambiguous without any time-range filter:

```sql
-- dim_currencies with surrogate key
SELECT
    {{ dbt_utils.generate_surrogate_key(['short_code', 'dbt_valid_from']) }} AS currency_sk,
    short_code AS currency_key,
    ...
FROM {{ ref('currencies_snapshot') }}

-- fact_rates storing surrogate
SELECT
    c.currency_sk AS currency_sk,
    ...
FROM rates r
JOIN dim_currencies c
    ON r.currency = c.currency_key
    AND r.rate_date BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
```

Implementing surrogate keys would require adding `currency_sk` to `dim_currencies`, rewriting `fact_rates` to resolve and store it at load time, and updating all dbt relationship tests. In practice, ISO 4217 currency codes are among the most stable standards in existence — a code change is extraordinarily rare — so the fan-out risk here is theoretical rather than real. The current model uses `short_code` as the natural key and leaves the time-range join responsibility to the consuming query.

---

## Validation

Rules are defined in `conf/base/parameters.yml`. Key constraints:

- Currency codes must be exactly 3 characters (ISO 4217)
- Rates must be between 0.000001 and 100,000,000
- Rate dates must be ≥ 2019-01-01
- Required fields checked for null / empty

---

## Tests & CI

Unit tests for the validation logic live in `tests/test_validation.py`. They cover `build_error_column` (all rule types), `_append_errors` (accumulation and no separator artifacts on valid rows), and `validate_df`. Tests use a local PySpark session — no Delta Lake or S3 needed.

```bash
pytest tests/ -v
```

GitHub Actions runs three jobs on every push to `develop` (CI is intentionally not triggered on `main` — all development happens on `develop` and is merged only after CI passes):

| Job | What it does |
|-----|-------------|
| `lint` | Runs `ruff check .` — fails the build if lint errors are found (no auto-fix) |
| `dbt` | Runs `dbt parse` — validates all dbt model SQL and YAML without a database connection |
| `test` | Runs `pytest tests/` with PySpark 3.5.5 and Java 17 |
| ~~`build`~~ | Docker image build — disabled to conserve free-tier GitHub Actions minutes |

---

## Assumptions & Decisions

- **Delta Lake over plain Parquet** — ACID semantics are needed because rates are appended per-run while currencies are overwritten; Delta handles both cleanly.
- **AWS S3 as storage backend** — Bronze and Silver layers are stored as Delta Lake tables in S3. The Spark code is cloud-agnostic; the storage path is the only environment-specific setting.
- **Spark runs locally** — the pipeline uses `local[*]` mode, meaning Spark runs on the developer's machine while data is written to S3. This keeps the setup self-contained without requiring a managed Spark cluster.
- **PostgreSQL staging tables** — act as the interface between Spark and dbt so that dbt does not need Delta Lake support.
- **dbt incremental models** for `dim_date` and `fact_rates` — repeated runs do not reprocess existing data. `dim_date` grows over time as each daily run adds a new date entry; however, because `rates_stage` is always overwritten with the latest run only, a `--full-refresh` wipes historical dates that cannot be recovered without re-running the full pipeline for each past date. In a production setup `rates_stage` would retain history, making full refreshes safe.
- **Quarantine rather than drop** — invalid rows are preserved for debugging. Further quarantine processing pipelines are outside of the project scope right now.
- **Athena queries Bronze/Silver as Parquet, not native Delta** — Glue catalog tables for Bronze and Silver layers are registered as Parquet format. Athena reads the underlying Parquet files directly, bypassing the Delta transaction log. This means Athena does not benefit from Delta's time travel or snapshot isolation — it reads all Parquet files present in the folder. The root cause is that the Glue Crawler cannot parse the Delta Lake transaction log (`_delta_log/`) from files written by local Spark. On EMR Serverless this limitation does not apply: configuring the Spark session to use the Glue Data Catalog as the Delta metastore causes Delta Lake to register table metadata directly in Glue on write, so Athena can query the tables as native Delta Lake without any manual setup.
- **EMR Serverless not used** — Spark runs in `local[*]` mode on the developer's machine. EMR Serverless is not available on the AWS free tier; in a production setup it would be the natural managed execution layer for the Spark jobs.
- **Databricks not used** — Databricks has first-class Delta Lake support (it was created there) and would eliminate the Glue Parquet workaround entirely: Bronze and Silver tables are queryable via Databricks SQL without any catalog hacks. Databricks Community Edition (the free tier) was considered but has two blockers for this use case: it has no job scheduling (the Jobs/Workflows feature is paid-only), and clusters auto-terminate after inactivity, causing a 2–3 minute cold start on every pipeline run. Since the project already has an AWS setup and a working local execution path, adding Databricks as a third execution environment would add complexity without a clear benefit.
- **PostgreSQL stands in for Redshift** — in a production pipeline the Gold layer (star schema) would live in Amazon Redshift, a columnar data warehouse optimised for analytical queries at scale. The dimensional model (`dim_currencies`, `dim_date`, `fact_rates`) is exactly the structure Redshift is designed for. PostgreSQL is used here as a cost-free equivalent; the dbt models would transfer to Redshift with only a `profiles.yml` connection change. Redshift is a paid AWS service not available on the free tier.
- **Glue and Athena are managed via AWS Console only** — the PyCharm AWS Toolkit does not support Glue catalog or Athena. The Glue tables (Bronze and Silver) were created manually in the console and are not managed from the repository. As a result the repo is partially detached from the AWS catalog layer — the pipeline writes Delta Lake files to S3 correctly, but Glue table definitions and Athena queries exist outside the codebase. Infrastructure-as-code tooling (e.g. AWS CDK or Terraform) would be the proper solution to manage these as part of the project.
- **No alerting configured** — pipeline failures are visible in the Airflow UI but no notifications are sent. In production, Airflow's `on_failure_callback` would trigger email or Slack alerts on task failure. Manual monitoring via the Airflow UI is required in the current setup.
- **dbt version constrained to 1.8.7** — the base Docker image `apache/spark:3.5.5` ships with Python 3.8. dbt Core 1.9+ requires Python 3.10+, so upgrading is not possible without replacing the base image. As a result, the legacy `.sql`-based snapshot syntax is used for the `dim_currencies` SCD Type 2 snapshot rather than the newer YAML-based format introduced in dbt 1.9.
- The CurrencyBeacon free tier returns ~161 currencies. Ingesting each as a base produces ~25,760 rate pairs per run (161 × 160, self-pairs excluded).
