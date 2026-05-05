import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
RAW_DATA_DUCKDB = os.environ.get("INGEST_DUCKDB_PATH", "").strip()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
from air_quality_index_insert import fetchStations, insertStation
from cctv_ingest_lifetime import crawlOnce, loadCameraRows
from cctv_to_bmkg import REQUEST_DELAY_S as BMKG_DELAY_S, fetchBmkgWeather
CCTV_POLL_SECONDS = 30
BMKG_SCHEDULE_HOURS = [0, 12]
AQI_HOUR = 6
db_lock = threading.Lock()


def log(tag: str, msg: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [{tag}] {msg}")


def cctvThread(con: duckdb.DuckDBPyConnection):
    with db_lock:
        camera_rows = loadCameraRows(con)
    log("CCTV", f"Started. {len(camera_rows)} cameras, interval={CCTV_POLL_SECONDS}s")
    while True:
        started = time.time()
        log("CCTV", "Crawling...")
        try:
            with db_lock:
                crawlOnce(con, camera_rows)
        except Exception as e:
            log("CCTV", f"ERROR: {e}")
        elapsed = time.time() - started
        sleep_for = max(0.0, CCTV_POLL_SECONDS - elapsed)
        log("CCTV", f"Done in {elapsed:.1f}s, sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)


def nextBmkgSlot():
    now = datetime.now()
    slots = [now.replace(hour=h, minute=0, second=0, microsecond=0) for h in BMKG_SCHEDULE_HOURS]
    for slot in slots:
        if now < slot:
            return slot
    return slots[0] + timedelta(days=1)


def bmkgThread(con: duckdb.DuckDBPyConnection):
    log("BMKG", f"Started. Runs at {BMKG_SCHEDULE_HOURS} daily.")
    while True:
        log("BMKG", "Fetching...")
        try:
            with db_lock:
                adm4_rows = con.execute(
                    "SELECT DISTINCT adm4 FROM cctv_list_final WHERE adm4 IS NOT NULL ORDER BY adm4"
                ).fetchall()
            ok, err = 0, 0
            for i, (adm4,) in enumerate(adm4_rows):
                if i and BMKG_DELAY_S > 0:
                    time.sleep(BMKG_DELAY_S)
                try:
                    data = fetchBmkgWeather(adm4)
                    if not data:
                        raise ValueError("empty response")
                    lok = data.get("lokasi", {})
                    with db_lock:
                        con.execute(
                            """INSERT INTO bmkg_weather (adm1,adm2,adm3,adm4,provinsi,kotkab,kecamatan,desa,lon,lat,timezone,weather_data)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                            [
                                lok.get("adm1"), lok.get("adm2"), lok.get("adm3"), lok.get("adm4"),
                                lok.get("provinsi"), lok.get("kotkab"), lok.get("kecamatan"), lok.get("desa"),
                                lok.get("lon"), lok.get("lat"), lok.get("timezone"),
                                json.dumps(data.get("data"), ensure_ascii=False),
                            ],
                        )
                    ok += 1
                except Exception as e:
                    err += 1
                    log("BMKG", f"ERROR adm4={adm4!r}: {e}")
            log("BMKG", f"{ok} inserted, {err} errors, {len(adm4_rows)} total.")
        except Exception as e:
            log("BMKG", f"ERROR: {e}")
        nxt = nextBmkgSlot()
        sleep_s = max(60, (nxt - datetime.now()).total_seconds())
        log("BMKG", f"Next run at {nxt:%Y-%m-%d %H:%M:%S} (sleeping {sleep_s:.0f}s)")
        time.sleep(sleep_s)


def nextAqiSlot():
    now = datetime.now()
    target = now.replace(hour=AQI_HOUR, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target


def aqiThread(con: duckdb.DuckDBPyConnection):
    log("AQI", "Started. Runs once/day at 06:00.")
    while True:
        log("AQI", "Fetching stations...")
        try:
            stations = fetchStations()
            ok, err = 0, 0
            for row in stations:
                try:
                    with db_lock:
                        insertStation(con, row)
                    ok += 1
                except Exception as e:
                    err += 1
                    log("AQI", f"ERROR {row.get('id_stasiun', '?')}: {e}")
            log("AQI", f"{ok} inserted, {err} errors, {len(stations)} total.")
        except Exception as e:
            log("AQI", f"ERROR: {e}")
        nxt = nextAqiSlot()
        sleep_s = max(60, (nxt - datetime.now()).total_seconds())
        log("AQI", f"Next run at {nxt:%Y-%m-%d %H:%M:%S} (sleeping {sleep_s:.0f}s)")
        time.sleep(sleep_s)


def main():
    print(f"Database: {RAW_DATA_DUCKDB}")
    con = duckdb.connect(str(RAW_DATA_DUCKDB))
    threads = [
        threading.Thread(target=cctvThread, args=(con,), name="cctv", daemon=True),
        threading.Thread(target=bmkgThread, args=(con,), name="bmkg", daemon=True),
        threading.Thread(target=aqiThread, args=(con,), name="aqi", daemon=True),
    ]
    for t in threads:
        t.start()
        log("MAIN", f"Started thread: {t.name}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("MAIN", "Shutting down...")
        con.close()


if __name__ == "__main__":
    main()
