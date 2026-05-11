from config import RUNNING_LOCAL, RUNNING_AWS, AWS_S3_BUCKET

if RUNNING_AWS:
    RAW_DATA_DIR = f"s3a://{AWS_S3_BUCKET}/raw"
    BRONZE_OUT_DIR = f"s3a://{AWS_S3_BUCKET}/bronze"
    SILVER_DIR = f"s3a://{AWS_S3_BUCKET}/silver"

elif RUNNING_LOCAL:
    RAW_DATA_DIR = "/app/data/raw"
    BRONZE_OUT_DIR = "/app/data/bronze"
    SILVER_DIR = "/app/data/silver"
else:
    RAW_DATA_DIR = "dbfs:/FileStore/raw"
    BRONZE_OUT_DIR = "dbfs:/delta/bronze"
    SILVER_DIR = "dbfs:/delta/silver"
