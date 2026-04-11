import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Any

load_dotenv()

BASE_BMKG_API = os.getenv("BASE_BMKG_API")
BASE_NOMINATIM_API = os.getenv("BASE_NOMINATIM_API")

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

class BMKGSchema(BaseModel):
    adm1: str
    adm2: str
    adm3: str
    adm4: str
    provinsi: str
    kotkab: str
    kecamatan: str
    desa: str
    lon: float
    lat: float
    timezone: str
    weather_data: Any

def fetchBmkgWeather(adm4: str):
    """
    Fetch BMKG prakiraan-cuaca for a given adm4 code.
    Returns the full JSON response dict, or None on error.
    """
    r = requests.get(
        BMKG_URL,
        params={"adm4": adm4},
        headers={"User-Agent": CHROME_UA, "Accept": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        return None
    return data