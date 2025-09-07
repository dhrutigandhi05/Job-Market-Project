import json
import time
import requests
import random
from datetime import datetime, timezone
# from config import get_s3_client, S3_BUCKET_NAME, RAPIDAPI_KEY, RAPIDAPI_HOST
from config import get_s3_client, get_db_engine, cfg

# print("host:", RAPIDAPI_HOST, "key:", RAPIDAPI_KEY[:4]+"…") # verify config

API_URL = "https://jsearch.p.rapidapi.com/search"

# fetches a single page of job data from the api
def fetch_page(page=1, page_size=20, **kwargs): # kwargs handles extra params
    HEADERS = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }

    query = {"page": page, "page_size": page_size, **kwargs}
    max_retries = 6

    for attempt in range(1, max_retries + 1):
        resp = requests.get(API_URL, headers=HEADERS, params=query, timeout=15)

        # if its a successful response, return the data
        if 200 <= resp.status_code < 300:
            return resp.json().get("data", [])
        
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")

            if retry_after and retry_after.isdigit():
                wait = int(retry_after)
            else:
                wait = min(2 ** attempt, 60) + random.uniform(0, 0.5) # exponential backoff, max 60 seconds
            
            time.sleep(wait)
            continue

        resp.raise_for_status()

    raise RuntimeError("RapidAPI still rate limiting after retries")

    # response = requests.get(API_URL, headers=HEADERS, params=query)
    # response.raise_for_status()  # raise error for bad responses
    # return response.json().get("data", [])

def handler(event, context):
    query = "data science"
    page_size = 10
    max_pages = 1

    today = datetime.utcnow().date().isoformat()
    s3 = get_s3_client()

    for page in range(1, max_pages+1):
        data = fetch_page(page, page_size, query=query)

        if not data:
            break

        key = f"raw/{today}/page_{page}.json"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=json.dumps(data))
        time.sleep(1)

    return {"status": "done", "pages": page}

# fetches all pages of job data based on the query
def fetch_all_jobs(query, page_size=50, max_pages=10):
    all_jobs = []

    for page in range(1, max_pages + 1):
        print(f"Fetching page {page}")
        jobs = fetch_page(page=page, page_size=page_size, query=query) # calls the api for the current page

        if not jobs:
            print("No more jobs found")
            break

        all_jobs.append(jobs) # add all the jobs from the current page to the list
        time.sleep(1)

    return all_jobs

# returns the fetched jobs and saves them to s3
def save_to_s3(query, page_size=50, max_pages=10):
    s3 = get_s3_client() # create s3 client
    all_jobs = fetch_all_jobs(query, page_size, max_pages) # fetch all jobs from the api
    today = datetime.utcnow().date().isoformat() # use current date for s3 folder structure

    if not all_jobs:
        print("No jobs to save")
        return

    # save each page of jobs as a separate JSON file to s3
    for idx, jobs in enumerate(all_jobs, start=1):
        key = f"raw/{today}/page_{idx}.json"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=json.dumps(jobs))

    # confirm upload
    print(f"Uploaded {len(all_jobs)} pages to s3://{S3_BUCKET_NAME}/raw/{today}/")

if __name__ == "__main__":
    save_to_s3(query="data science", page_size=50, max_pages=20)