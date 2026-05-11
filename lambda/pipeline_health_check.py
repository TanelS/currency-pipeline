import os
from datetime import timezone

import boto3

BUCKET = os.environ.get("AWS_S3_BUCKET")
PREFIXES = ["bronze/", "silver/"]

def lambda_handler(event, context):
    s3 = boto3.client("s3", region_name="eu-north-1")
    summary = {}

    for prefix in PREFIXES:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = response.get("Contents", [])
        if objects:
            last_modified = max(obj["LastModified"] for obj in objects)
            summary[prefix] = {
                "file_count": len(objects),
                "last_modified": last_modified.astimezone(timezone.utc).isoformat()
            }
        else:
            summary[prefix] = {"file_count": 0, "last_modified": None}

    return {"statusCode": 200, "body": summary}