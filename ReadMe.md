


## Running the Pipeline

### Bronze — ingest raw currency codes into Delta
```bash
docker compose run spark python3 ingestion/ingest_bronze.py
```
or
```bash
docker compose run --remove-orphans spark python3 ingestion/ingest_bronze.py
```