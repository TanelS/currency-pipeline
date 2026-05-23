#!/bin/bash
set -e

echo "=== Running dbt snapshot ==="
docker compose run --remove-orphans spark dbt snapshot --project-dir dbt/ --profiles-dir dbt/

echo "=== Running dbt Gold models ==="
docker compose run --remove-orphans spark dbt run --project-dir dbt/ --profiles-dir dbt/

echo "=== dbt complete ==="