#!/bin/bash
set -e

echo "=== Running dbt Gold models ==="
docker compose run --remove-orphans spark dbt run --project-dir dbt/ --profiles-dir dbt/

echo "=== dbt complete ==="