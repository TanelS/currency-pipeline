from config import (
    DATABASE_USERNAME,
    DATABASE_PASSWORD,
    DATABASE_NAME,
    DBT_POSTGRES_HOST,
    DBT_POSTGRES_PORT
)
jdbc_props = {
    'user': DATABASE_USERNAME,
    'password': DATABASE_PASSWORD,
    'driver': 'org.postgresql.Driver'
}

JDBC_URL = f'jdbc:postgresql://{DBT_POSTGRES_HOST}:{DBT_POSTGRES_PORT}/{DATABASE_NAME}'