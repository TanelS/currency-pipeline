import os
from datetime import timezone

import boto3

BUCKET = os.environ.get("AWS_S3_BUCKET")  # must be set manually in Lambda environment variables
PREFIXES = ["bronze/", "silver/"]

# a demo Lambda function to check the health of the data pipeline by inspecting S3 layer prefixes.
# Written by Clude AI.

def lambda_handler(event, context):
    """
    Check the health of the data pipeline by inspecting S3 layer prefixes.

    For each layer (bronze, silver) reports the number of objects and the
    UTC timestamp of the most recently modified object. An empty layer is
    reported with ``file_count: 0`` and ``last_modified: null``.

    :param event: Lambda invocation event (unused).
    :param context: Lambda runtime context (unused).
    :return: HTTP-style response with ``statusCode`` 200 and a ``body`` dict
        keyed by prefix, e.g.::

            {
                "statusCode": 200,
                "body": {
                    "bronze/": {"file_count": 42, "last_modified": "2026-05-11T11:29:37+00:00"},
                    "silver/": {"file_count": 10, "last_modified": "2026-05-11T11:30:48+00:00"}
                }
            }
    """
    s3 = boto3.client("s3", region_name="eu-north-1")
    summary = {}

    for prefix in PREFIXES:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = response.get("Contents", [])
        if objects:
            last_modified = max(obj["LastModified"] for obj in objects)
            summary[prefix] = {
                "file_count": len(objects),
                "last_modified": last_modified.astimezone(timezone.utc).isoformat(),
            }
        else:
            summary[prefix] = {"file_count": 0, "last_modified": None}

    return {"statusCode": 200, "body": summary}
