FROM apache/spark:3.5.5

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.1.0 \
    pandas \
    pyarrow \
    dbt-core==1.8.7 \
    dbt-postgres==1.8.2 \
    psycopg[binary] \
    python-dotenv

WORKDIR /app