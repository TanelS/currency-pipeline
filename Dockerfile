FROM apache/spark:3.5.5

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.1.0 \
    pandas \
    pyarrow \
    dbt-core \
    dbt-postgres \
    psycopg[binary] \
    python-dotenv

WORKDIR /app

# No HADOOP_HOME needed — Linux container handles this natively
