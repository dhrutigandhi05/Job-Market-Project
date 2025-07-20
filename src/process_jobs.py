import io
import json
import pandas as pd
from datetime import datetime
from config import get_s3_client, get_db_engine, S3_BUCKET_NAME

# list all files in the specified S3 bucket with the given prefix
def list_s3_files(prefix: str) -> list[str]:
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix) # list all files in the bucket with the specific prefix (raw/(date))
    return [o['Key'] for o in response.get('Contents', [])] # return list of files to be processed

def load_page_to_df(key: str) -> pd.DataFrame:
    s3 = get_s3_client()
    object = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key) # get the object from S3
    data = json.loads(object["Body"].read()) # reads the object body and converts it to a JSON object
    return pd.json_normalize(data) # convert the JSON object to a pandas DataFrame

