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
    resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key) # get the object from S3 bucket
    raw  = resp["Body"].read() # read the raw data from the object
    records = json.loads(raw) # load the raw data as a JSON object
    return pd.json_normalize(records) # convert the JSON object to a pandas DataFrame

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"job_min_salary": "min_salary", "job_max_salary": "max_salary"}).copy() # rename columns
    df = df.dropna(subset=["min_salary", "max_salary"], how="all") # drop rows with NaN in min_salary and max_salary
    df["min_salary"].fillna(df["max_salary"], inplace=True) # fill NaN in min_salary with max_salary
    df["max_salary"].fillna(df["min_salary"], inplace=True) # fill NaN in max_salary with min_salary
    df["avg_salary"] = (df.min_salary + df.max_salary) / 2 # calculate average salary
    df["date_posted"] = pd.to_datetime(df.job_posted_at_datetime_utc).dt.date # convert job_posted_at_datetime_utc to date

    # create a list of skills from job_highlights.Qualifications split by -
    df["skills_list"] = (
        df["job_highlights.Qualifications"].fillna("").apply(
            lambda x: [
                item.strip().lower()
                for item in x.split("-") if item.strip()
            ]
        )
    )

    df["title"] = df.job_title
    df["company"] = df.employer_name
    df["location"] = df.job_location

    return df

def write_to_db(df: pd.DataFrame) -> None:
    engine = get_db_engine()

    jobs_cols = [
        "job_id",
        "title",
        "company",
        "location",
        "salary_min",
        "salary_max",
        "avg_salary",
        "date_posted"
    ]

    df[jobs_cols].to_sql(
        "jobs",
        engine,
        if_exists="append",
        index=False
    )

    skills_df = (
        df[["job_id", "skills_list"]]
          .explode("skills_list")
          .rename(columns={"skills_list": "skill"})
    )

    skills_df.to_sql(
        "job_skills",
        engine,
        if_exists="append",
        index=False
    )

if __name__ == "__main__":
    # determine today's prefix
    today  = datetime.utcnow().date().isoformat()
    prefix = f"raw/{today}/"

    keys = list_s3_files(prefix)
    if not keys:
        print(f"No raw files found under prefix {prefix}")
        exit(0)

    for key in keys:
        print("Processing", key)
        page_df = load_page_to_df(key)
        clean    = clean_df(page_df)
        write_to_db(clean)

    print("pages loaded into db")