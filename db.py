from config import (
    DATABASE_NAME,
    DATABASE_PASSWORD,
    DATABASE_USERNAME,
    DBT_POSTGRES_HOST,
    DBT_POSTGRES_PORT,
)

jdbc_props = {
    "user": DATABASE_USERNAME,
    "password": DATABASE_PASSWORD,
    "driver": "org.postgresql.Driver", # official PostgreSQL JDBC driver.
}

JDBC_URL = f"jdbc:postgresql://{DBT_POSTGRES_HOST}:{DBT_POSTGRES_PORT}/{DATABASE_NAME}"

# for psycopg:
conn_string = f"host={DBT_POSTGRES_HOST} port={DBT_POSTGRES_PORT} dbname={DATABASE_NAME} user={DATABASE_USERNAME} password={DATABASE_PASSWORD}"
