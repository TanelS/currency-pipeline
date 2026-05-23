# ETL Sequence

> Steps are orchestrated by the Airflow DAG (`currency_pipeline`). Each task runs in a fresh `pipeline-spark` container spawned via DockerOperator. The manual equivalent is shown in the README under Option A / Option B.

## Part 1 — Ingest & Transform

```mermaid
sequenceDiagram
    actor Orchestrator as Airflow DAG / User
    participant SEN as HttpSensor
    participant IB as ingest_bronze.py
    participant API as CurrencyBeacon API
    participant S3 as AWS S3
    participant TS as transform_silver.py

    rect rgb(230, 245, 255)
        Note over Orchestrator,API: SENSOR — API availability check
        Orchestrator->>SEN: task: check_api_availability
        SEN->>API: GET /v1/status (poke every 30s)
        API-->>SEN: HTTP 200 or 401
        SEN-->>Orchestrator: API reachable — proceed
    end

    rect rgb(255, 244, 225)
        Note over Orchestrator,S3: BRONZE — Ingest
        Orchestrator->>IB: task: ingest_bronze
        IB->>API: GET /currencies
        API-->>IB: ~161 currencies (JSON)
        IB->>S3: write bronze/currencies (overwrite)
        loop ~161 base currencies
            IB->>API: GET /latest?base={code}
            API-->>IB: rates JSON
        end
        IB->>S3: append bronze/rates (partitioned by curr_base)
    end

    rect rgb(240, 225, 255)
        Note over Orchestrator,TS: SILVER — Clean, Validate & Quarantine
        Orchestrator->>TS: task: transform_silver
        TS->>S3: read bronze/currencies
        TS->>S3: write silver/currencies + quarantine (overwrite)
        TS->>S3: read bronze/rates (latest _ingested_at)
        TS->>S3: read silver/currencies (ISO 4217 reference)
        TS->>S3: write silver/rates + quarantine (overwrite, partitioned)
    end
```

## Part 2 — Load & Gold

```mermaid
sequenceDiagram
    actor Orchestrator as Airflow DAG / User
    participant LP as load_silver_to_postgres.py
    participant S3 as AWS S3
    participant PG as PostgreSQL
    participant DBT as dbt run

    rect rgb(225, 245, 255)
        Note over Orchestrator,PG: STAGING — Load to PostgreSQL
        Orchestrator->>LP: task: load_to_postgres
        LP->>S3: read silver/currencies
        LP->>PG: JDBC write currencies_stage (truncate)
        LP->>S3: read silver/rates
        LP->>PG: JDBC write rates_stage (truncate, batch 10 000)
        PG-->>LP: PK + FK constraints applied
    end

    rect rgb(240, 255, 225)
        Note over Orchestrator,PG: SNAPSHOT — SCD Type 2
        Orchestrator->>DBT: task: run_dbt_snapshot
        DBT->>PG: compare currencies_stage → currencies_snapshot
        DBT->>PG: insert new version rows for changed currencies
        DBT-->>Orchestrator: Snapshot complete
    end

    rect rgb(225, 255, 225)
        Note over Orchestrator,PG: GOLD — dbt Dimensional Models
        Orchestrator->>DBT: task: run_dbt
        DBT->>PG: SELECT currencies_snapshot → gold.dim_currencies
        DBT->>PG: SELECT rates_stage → gold.dim_date (incremental)
        DBT->>PG: SELECT rates_stage + dim_date → gold.fact_rates (incremental)
        DBT-->>Orchestrator: Done
    end
```
