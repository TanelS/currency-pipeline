from config import RUNNING_LOCAL


if RUNNING_LOCAL:
    RAW_DATA_DIR = "/app/data/raw"
    BRONZE_OUT_DIR = "/app/data/bronze"
else:
    RAW_DATA_DIR = "dbfs:/FileStore/raw"
