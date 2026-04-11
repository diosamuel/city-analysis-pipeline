import os
from dotenv import load_dotenv
import requests
from pydantic import BaseModel
load_dotenv()

class AirQualitySchema(BaseModel):
    id_stasiun: str
    waktu: str
    lat: float
    lon: float
    a_pm10: float
    a_pm25: float
    a_so2: float
    a_co: float
    a_o3: float
    a_no2: float
    a_hc: float
    c_pm10: float
    c_pm25: float
    c_so2: float
    c_co: float
    c_o3: float
    c_no2: float
    c_hc: float
    t_pm10: float
    t_pm25: float
    t_so2: float
    t_co: float
    t_o3: float
    t_no2: float
    t_hc: float
    nama: str
    alamat: str
    kota: str
    provinsi: str
    param: str
    stasiun_uji: str
    stasiun_show: str
    stasiun_show_detail: str
    p3e: str
    time_offset: int | str
    time_z: str
    is_maintenance: int
    auto_validation: int
    tipe: str
    kelurahan: str
    kecamatan: str
    val: float
    cat: str
    kategori: str
    waktu_text: str
    tipe_text: str
   

BASE_KEMENLH_API = os.getenv("BASE_KEMENLH_API")
BASE_NOMINATIM_API = os.getenv("BASE_NOMINATIM_API")

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

def fetchStations():
    r = requests.get(
        BASE_KEMENLH_API,
        headers={
            "User-Agent": CHROME_UA,
            "Accept": "application/json",
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError("KemenLH returned non-object JSON")
    return data.get("rows", [])