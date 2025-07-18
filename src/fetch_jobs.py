import json
import time
import requests
from datetime import datetime, timezone
from config import get_s3_client, S3_BUCKET_NAME, RAPIDAPI_KEY, RAPIDAPI_HOST

API_URL = "https://jsearch.p.rapidapi.com/search"

# fetches a single page of job data from the api
def fetch_page(page=1, page_size=20, **kwargs): # kwargs handles extra params
    HEADERS = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }

    query = {"page": page, "page_size": page_size, **kwargs}
    response = requests.get(API_URL, headers=HEADERS, params=query)
    response.raise_for_status()  # raise error for bad responses
    return response.json().get("data", [])

def fetch_all_jobs(query, page_size=50, max_pages=10):
    all_jobs = []
    for page in range(1, max_pages + 1):
        print(f"Fetching page {page}")
        jobs = fetch_page(page=page, page_size=page_size, query=query)

        if not jobs:
            print("No more jobs found")
            break

        all_jobs.append(jobs)
        time.sleep(1)
    return all_jobs

def save_to_s3(query, page_size=50, max_pages=10):
    s3 = get_s3_client()
    all_jobs = fetch_all_jobs(query, page_size, max_pages)
    today = datetime.now(datetime.timezone.utc).date().isoformat()

    if not all_jobs:
        print("No jobs to save")
        return

    for idx, jobs in enumerate(all_jobs, start=1):
        key = f"raw/{today}/page_{idx}.json"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=json.dumps(jobs))

    print(f"Uploaded {len(all_jobs)} pages to s3://{S3_BUCKET_NAME}/raw/{today}/")

if __name__ == "__main__":
    save_to_s3(query="data science", page_size=50, max_pages=20)