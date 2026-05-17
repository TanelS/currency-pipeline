# System Architecture

## Services & Networks

```mermaid
flowchart TB
    API["CurrencyBeacon API"]
    S3[("AWS S3\nbronze/ · silver/\nDelta Lake")]

    subgraph pipeline_net["pipeline-network  ·  docker-compose.yml"]
        SPARK["pipeline-spark\nPySpark 3.5.5 · Delta Lake 3.1.0 · dbt"]
        PG[("postgres:16\ncurrencies db\n:5443 on host")]
    end

    subgraph airflow_compose["docker-compose.airflow.yml"]
        subgraph airflow_net["airflow-network"]
            UI["airflow-apiserver\n:8080"]
            SCHED["scheduler · dag-processor · triggerer"]
            REDIS[("redis")]
            APGRES[("airflow-postgres\nAirflow metadata")]
        end
        WORKER["airflow-worker\nairflow-network + pipeline-network"]
    end

    HOST(["Host"])

    API -->|HTTPS| SPARK
    SPARK <-->|"s3a://"| S3
    SPARK -->|"JDBC :5432"| PG
    WORKER -->|"DockerOperator\n/var/run/docker.sock"| SPARK
    HOST -.->|":4040 Spark UI"| SPARK
    HOST -.->|":5443 SQL"| PG
    HOST -.->|":8080 Airflow UI"| UI
```

> The Airflow worker bridges both networks — it receives tasks via `airflow-network` and spawns fresh `pipeline-spark` containers that join `pipeline-network` (where `postgres` is reachable by hostname).

## Data Layer Overview

```mermaid
flowchart LR
    subgraph s3["AWS S3"]
        direction TB
        B1[bronze/currencies]
        B2["bronze/rates\n(partitioned by curr_base)"]
        S1[silver/currencies]
        S2[silver/currencies_quarantine]
        S3r["silver/rates\n(partitioned by curr_base)"]
        S4[silver/rates_quarantine]
    end

    subgraph pg["PostgreSQL  (currencies db)"]
        direction TB
        subgraph staging["public schema"]
            ST1[currencies_stage]
            ST2[rates_stage]
        end
        subgraph gold["gold schema"]
            G1[dim_currencies]
            G2[dim_date]
            G3[fact_rates]
        end
    end

    ST1 --> G1
    ST2 --> G2
    ST2 --> G3
    G1 -.->|FK| G3
    G2 -.->|FK| G3
```
