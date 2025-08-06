import os
from dotenv import load_dotenv
import boto3
from sqlalchemy import create_engine

load_dotenv()

# load environment variables
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# connect to s3
def get_s3_client():
    return boto3.client(
        's3',
        region_name=AWS_DEFAULT_REGION
        # aws_access_key_id=AWS_ACCESS_KEY_ID,
        # aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# postgres db connection
def get_db_engine():
    if DATABASE_URL:
        return create_engine(DATABASE_URL)
    else:
        raise ValueError("DATABASE_URL is not set in the environment variables.")