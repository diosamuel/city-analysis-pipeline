from __future__ import annotations

import json
import os
import time

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()
RAW_DATA_DUCKDB = os.environ.get("INGEST_DUCKDB_PATH", "").strip()
BMKG_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca"
REQUEST_DELAY_S = 1.05
CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def fetchBmkgWeather(adm4: str):
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


def main():
    con = duckdb.connect(str(RAW_DATA_DUCKDB))
    rows = con.execute(
        """
        SELECT DISTINCT adm4 FROM cctv_list_final
        WHERE adm4 IS NOT NULL ORDER BY adm4
        """
    ).fetchall()
    if not rows:
        print("No adm4 codes found in cctv_list_final.")
        con.close()
        return
    inserted = 0
    errored = 0
    for i, (adm4,) in enumerate(rows):
        if i and REQUEST_DELAY_S > 0:
            time.sleep(REQUEST_DELAY_S)
        try:
            data = fetchBmkgWeather(adm4)
        except requests.RequestException as e:
            print(f"ERROR adm4={adm4!r}: {e}")
            errored += 1
            continue
        if not data:
            print(f"SKIP adm4={adm4!r}: empty response")
            errored += 1
            continue
        lokasi = data.get("lokasi", {})
        con.execute(
            """
            INSERT INTO bmkg_weather
                (adm1, adm2, adm3, adm4, provinsi, kotkab, kecamatan, desa,
                 lon, lat, timezone, weather_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                lokasi.get("adm1"), lokasi.get("adm2"), lokasi.get("adm3"), lokasi.get("adm4"),
                lokasi.get("provinsi"), lokasi.get("kotkab"), lokasi.get("kecamatan"), lokasi.get("desa"),
                lokasi.get("lon"), lokasi.get("lat"), lokasi.get("timezone"),
                json.dumps(data.get("data"), ensure_ascii=False),
            ],
        )
        inserted += 1
        print(
            f"INSERT adm4={adm4!r} {lokasi.get('provinsi')}, {lokasi.get('kotkab')}, "
            f"{lokasi.get('kecamatan')}, {lokasi.get('desa')}"
        )
    con.close()
    print(f"Done. {inserted} inserted, {errored} errors, {len(rows)} total adm4 codes.")


if __name__ == "__main__":
    main()
