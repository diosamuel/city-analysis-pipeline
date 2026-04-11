import json
import os
import requests
from pathlib import Path
from typing import Any, Dict, List
from dotenv import load_dotenv
from utils import getAPCid

load_dotenv()

BASE_CCTV_CHART1_API = os.getenv("BASE_CCTV_CHART1_API")
BASE_CCTV_CHART2_API = os.getenv("BASE_CCTV_CHART2_API")
BASE_CCTV_CHART3_API = os.getenv("BASE_CCTV_CHART3_API")

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)

PERSISTENT_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "persistent_data"

with open(PERSISTENT_DATA_DIR / "cctv_list.json") as f:
    cctv_data = json.load(f)

CAMERA_LIST = cctv_data["cameraArray"]

def fetch_cctv(url: str, referrer: str, timeout: int = 30):
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": referrer,
        "x-requested-with": "XMLHttpRequest",
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return payload


def fetch_all_cctv():
    for row in CAMERA_LIST:
        camera_code = str(row[0]).strip()
        api_id = getAPCid(row)
        referrer = f"https://apace-ai.com/{api_id}/"

        c1 = fetch_cctv(f"{BASE_CCTV_CHART1_API}{api_id}", referrer)
        c2 = fetch_cctv(f"{BASE_CCTV_CHART2_API}{api_id}", referrer)
        c3 = fetch_cctv(f"{BASE_CCTV_CHART3_API}{api_id}", referrer)

        results.append({
            "camera_code": camera_code,
            "api_id": api_id,
            "chart1": c1,
            "chart2": c2,
            "chart3": c3,
        })

    return results
