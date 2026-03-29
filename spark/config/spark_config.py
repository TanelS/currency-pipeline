from config import RUNNING_LOCAL

if RUNNING_LOCAL:
    RAW_DATA_DIR = "/app/data/raw"
    BRONZE_OUT_DIR = "/app/data/bronze"
    SILVER_DIR = "/app/data/silver"
else:
    RAW_DATA_DIR = "dbfs:/FileStore/raw"
    BRONZE_OUT_DIR = "dbfs:/delta/bronze"
    SILVER_DIR = "dbfs:/delta/silver"
