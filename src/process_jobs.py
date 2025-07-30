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
    df = df.rename(
        columns={
            'job_min_salary': 'salary_min',
            'job_max_salary': 'salary_max'
        }
    ).copy()

    df = df.dropna(subset=['salary_min', 'salary_max'], how='all')
    # fill missing salaries
    df.loc[:, 'salary_min'] = df['salary_min'].fillna(df['salary_max'])
    df.loc[:, 'salary_max'] = df['salary_max'].fillna(df['salary_min'])

    # avg salary and date fields
    df.loc[:, "avg_salary"]  = (df.salary_min + df.salary_max) / 2
    df.loc[:, "date_posted"] = pd.to_datetime(
        df.job_posted_at_datetime_utc
    ).dt.date

    # extract skills: handle both list and string
    def extract_skills(x):
        if isinstance(x, list):
            return [item.strip().lower() for item in x if isinstance(item, str) and item.strip()]
        if isinstance(x, str):
            return [itm.strip().lower() for itm in x.split("-") if itm.strip()]
        # covers x == None or NaN
        return []

    df.loc[:, "skills_list"] = df["job_highlights.Qualifications"].apply(extract_skills)

    # pull through a few other fields
    df.loc[:, "title"]    = df.job_title
    df.loc[:, "company"]  = df.employer_name
    df.loc[:, "location"] = df.job_location

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

def latest_prefix() -> str:
    keys = list_s3_files("raw/")
    # extract unique date segments
    dates = sorted({ key.split("/")[1] for key in keys if key.count("/")>=2 })
    return f"raw/{dates[-1]}/"  if dates else ""

if __name__ == "__main__":
    prefix = latest_prefix()
    
    if not prefix:
        print("No raw/<date>/ folders found")
        exit(0)

    for key in list_s3_files(prefix):
        print("Processing", key)
        page_df = load_page_to_df(key)
        clean = clean_df(page_df)
        write_to_db(clean)

    print("pages loaded into db")