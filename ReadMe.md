
### Build Docker image
```bash
docker compose build
```

## Running the Pipeline

### Bronze — ingest raw currency codes into Delta
```bash
docker compose run spark python3 ingestion/ingest_bronze.py
```
or
```bash
docker compose run --remove-orphans spark python3 ingestion/ingest_bronze.py
```



To staging


```bash
docker compose run --remove-orphans spark python3 transformation/transform_silver.py
```



To PostgreSQL


```bash
docker compose run --remove-orphans spark python3 scripts/load_silver_to_postgres.py
```

Gold

Currencies

```bash
docker compose run --remove-orphans spark dbt run --project-dir dbt/ --profiles-dir dbt/
```