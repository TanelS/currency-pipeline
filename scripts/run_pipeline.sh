#!/bin/bash
set -e

echo "=== Step 1: Bronze ingestion ==="
docker compose run --remove-orphans -p 4040:4040 spark python3 ingestion/ingest_bronze.py

echo "=== Step 2: Silver transformation ==="
docker compose run --remove-orphans -p 4040:4040 spark python3 transformation/transform_silver.py

echo "=== Step 3: Load Silver to PostgreSQL ==="
docker compose run --remove-orphans -p 4040:4040 spark python3 scripts/load_silver_to_postgres.py

echo "=== Pipeline complete ==="