import os
from dotenv import load_dotenv
import boto3
from sqlalchemy import create_engine
from functools import lru_cache
import json

# load_dotenv()

# # load environment variables
# RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
# RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")
# AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
# # AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# # AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT")
# DB_NAME = os.getenv("DB_NAME")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_USER = os.getenv("DB_USER")

# DATABASE_URL = (
#     f"postgresql://{DB_USER}:{DB_PASSWORD}"
#     f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# )

# # connect to s3
# def get_s3_client():
#     return boto3.client(
#         's3',
#         region_name=AWS_DEFAULT_REGION
#         # aws_access_key_id=AWS_ACCESS_KEY_ID,
#         # aws_secret_access_key=AWS_SECRET_ACCESS_KEY
#     )

# # postgres db connection
# def get_db_engine():
#     if DATABASE_URL:
#         return create_engine(DATABASE_URL)
#     else:
#         raise ValueError("DATABASE_URL is not set in the environment variables.")

SECRET_ARN = os.getenv("APP_SECRET_ARN")
EXPECTED_KEYS = {"RAPIDAPI_KEY", "RAPIDAPI_HOST", "S3_BUCKET_NAME", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "AWS_DEFAULT_REGION"}

# load secrets from aws secrets manager
@lru_cache(maxsize=1)
def _load_secret() -> dict:
    # if no secret arn provided, return env vars
    if not SECRET_ARN:
        return {
            k: os.getenv(k) 
            for k in EXPECTED_KEYS 
            if os.getenv(k) is not None
        }

    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ca-central-1"
    client = boto3.client('secretsmanager', region_name=region) # create client for secrets manager
    response = client.get_secret_value(SecretId=SECRET_ARN) # get secret value using the ARN
    payload = response.get("SecretString") or "{}"
    data = json.loads(payload) # parse the json string into a dictionary

    # merge secrets with env vars
    merged_data = {
        **data, # secrets from secrets manager
        **{
            k: os.getenv(k, data.get(k)) # env var takes precedence if exists
            for k in EXPECTED_KEYS
        }
    }

    return merged_data

# helper function to get config values
def cfg(key: str, default=None):
    return _load_secret().get(key, default)

# connect to s3
def get_s3_client():
    region = cfg("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "ca-central-1"
    return boto3.client('s3', region_name=region)

# postgres db connection
def get_db_engine():
    host = cfg("DB_HOST")
    port = cfg("DB_PORT", "5432")
    db = cfg("DB_NAME")
    user = cfg("DB_USER")
    pwd = cfg("DB_PASSWORD")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)