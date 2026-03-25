from config import (
    DATABASE_USERNAME,
    DATABASE_PASSWORD,
    DATABASE_PORT,
    DATABASE_HOST,
    DATABASE_NAME
)
import psycopg
jdbc_props = {
    'user': DATABASE_USERNAME,
    'password': DATABASE_PASSWORD,
    'driver': 'org.postgresql.Driver'
}

JDBC_URL = f'jdbc:postgresql://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}'