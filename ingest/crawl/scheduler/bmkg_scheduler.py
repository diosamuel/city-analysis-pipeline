import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
from cctv_to_bmkg import REQUEST_DELAY_S, fetchBmkgWeather
SCHEDULE_HOURS = [0, 12]


def nextRunAt():
    now = datetime.now()
    today_slots = [now.replace(hour=h, minute=0, second=0, microsecond=0) for h in SCHEDULE_HOURS]
    for slot in today_slots:
        if now < slot:
            return slot
    return today_slots[0] + timedelta(days=1)


def runBmkgIngest(con: duckdb.DuckDBPyConnection):
    adm4_rows = con.execute(
        "SELECT DISTINCT adm4 FROM cctv_list_final WHERE adm4 IS NOT NULL ORDER BY adm4"
    ).fetchall()
    if not adm4_rows:
        print("  No adm4 codes in cctv_list_final.")
        return
    ok, err = 0, 0
    for i, (adm4,) in enumerate(adm4_rows):
        if i and REQUEST_DELAY_S > 0:
            time.sleep(REQUEST_DELAY_S)
        try:
            data = fetchBmkgWeather(adm4)
            if not data:
                raise ValueError("empty response")
            lok = data.get("lokasi", {})
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
            print(f"  INSERT adm4={adm4!r} {lok.get('kotkab')}, {lok.get('kecamatan')}")
        except Exception as e:
            err += 1
            print(f"  ERROR adm4={adm4!r}: {e}")
    print(f"  {ok} inserted, {err} errors, {len(adm4_rows)} total.")


def run(con: duckdb.DuckDBPyConnection):
    print(f"BMKG scheduler started. Runs at {SCHEDULE_HOURS} daily.")
    while True:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Running BMKG ingest...")
        try:
            runBmkgIngest(con)
        except Exception as e:
            print(f"ERROR in BMKG ingest: {e}")
        nxt = nextRunAt()
        sleep_s = (nxt - datetime.now()).total_seconds()
        if sleep_s <= 0:
            sleep_s = 60
        print(f"Next run at {nxt:%Y-%m-%d %H:%M:%S} (sleeping {sleep_s:.0f}s)")
        time.sleep(sleep_s)
