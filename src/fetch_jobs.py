import json
import time
import requests
from datetime import datetime
from config import get_s3_client, S3_BUCKET_NAME, RAPIDAPI_KEY, RAPIDAPI_HOST

API_URL = "https://jsearch.p.rapidapi.com/"

def fetch_page(page=1, page_size=20, **kwargs):
    HEADERS = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }

    query = {"page": page, "page_size": page_size, **kwargs}
    response = requests.get(API_URL, headers=HEADERS, params=query)
    response.raise_for_status()  # raise error for bad responses
    return response.json().get("data", [])