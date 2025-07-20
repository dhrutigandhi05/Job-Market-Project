import io
import json
import pandas as pd
from datetime import datetime
from config import get_s3_client, get_db_engine, S3_BUCKET_NAME

# list all files in the specified S3 bucket with the given prefix
def list_s3_files(prefix: str) -> list[str]:
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    return [o['Key'] for o in response.get('Contents', [])]

