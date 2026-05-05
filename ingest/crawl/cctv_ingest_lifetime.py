import json
import os
from typing import Any, Dict, List

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()
RAW_DATA_DUCKDB = os.environ.get("INGEST_DUCKDB_PATH", "").strip()
API_CHART1 = "https://apace-ai.com/generateChart1Data/"
API_CHART2 = "https://apace-ai.com/generateChart2Data/"
API_CHART3 = "https://apace-ai.com/generateChart3Data/"
POLL_SECONDS = 30
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)


def fetchJson(url: str, referrer: str, timeout: int = 30):
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "referer": referrer,
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return payload


def loadCameraRows(con: duckdb.DuckDBPyConnection):
    return con.execute(
        """
        SELECT camera_code, replace(route_slug, '/', '') AS route_slug
        FROM cctv_list ORDER BY camera_code
        """
    ).fetchall()


def normalizeCameraApiId(camera_row: tuple):
    camera_code, route_slug = camera_row[0], camera_row[1]
    slug = str(route_slug).strip() if route_slug else ""
    if slug:
        return slug.upper()
    code = str(camera_code).strip() if camera_code else ""
    if code.upper().startswith("APC"):
        return code.upper()
    return f"APC{code}"


def ingestVehicleSpeed(con: duckdb.DuckDBPyConnection, camera_code: str, payload: Dict[str, Any]):
    sql = """
        INSERT INTO vehicle_speed (
            camera_code, last_update_5minutes, label5min_data, listchart1_normal, listchart1_opposite,
            speed_normal, speed_opposite, speed_gol_normal, speed_gol_opposite,
            speed_max_gol_normal_list, speed_max_gol_opposite_list
        )
        VALUES (?, ?, CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
                CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON))
    """
    con.execute(
        sql,
        [
            camera_code,
            payload.get("lastUpdate5minutes"),
            json.dumps(payload.get("label5MinData", [])),
            json.dumps(payload.get("listChart1_Normal", [])),
            json.dumps(payload.get("listChart1_Opposite", [])),
            json.dumps(payload.get("speed_Normal", [])),
            json.dumps(payload.get("speed_Opposite", [])),
            json.dumps(payload.get("speed_gol_Normal", [])),
            json.dumps(payload.get("speed_gol_Opposite", [])),
            json.dumps(payload.get("speed_max_gol_Normal_list", [])),
            json.dumps(payload.get("speed_max_gol_Opposite_list", [])),
        ],
    )


def ingestHourlyVehicleSpeed(con: duckdb.DuckDBPyConnection, camera_code: str, payload: Dict[str, Any]):
    sql = """
        INSERT INTO hourly_vehicle_speed (
            camera_code, hourly_label, listdata60_60_sm, listdata60_60_mp, listdata60_60_ks,
            listdata60_60_bb, listdata60_60_tb, totalvolume_sm, totalvolume_mp, totalvolume_ks,
            totalvolume_bb, totalvolume_tb
        )
        VALUES (?, CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
                CAST(? AS JSON), ?, ?, ?, ?, ?)
    """
    con.execute(
        sql,
        [
            camera_code,
            json.dumps(payload.get("hourlyLabel", [])),
            json.dumps(payload.get("listData60_60_SM", [])),
            json.dumps(payload.get("listData60_60_MP", [])),
            json.dumps(payload.get("listData60_60_KS", [])),
            json.dumps(payload.get("listData60_60_BB", [])),
            json.dumps(payload.get("listData60_60_TB", [])),
            payload.get("totalVolume_SM"),
            payload.get("totalVolume_MP"),
            payload.get("totalVolume_KS"),
            payload.get("totalVolume_BB"),
            payload.get("totalVolume_TB"),
        ],
    )


def ingestTimeseriesVehicleSpeed(con: duckdb.DuckDBPyConnection, camera_code: str, payload: Dict[str, Any]):
    sql = """
        INSERT INTO all_timeseries_vehicle_speed (
            camera_code, hourly_label,
            datanormal_sm, datanormal_mp, datanormal_ks, datanormal_bb, datanormal_tb, datanormal_all,
            datalastdatenormal_sm, datalastdatenormal_mp, datalastdatenormal_ks, datalastdatenormal_bb,
            datalastdatenormal_tb, datalastdatenormal_all,
            datalast7normal_sm, datalast7normal_mp, datalast7normal_ks, datalast7normal_bb,
            datalast7normal_tb, datalast7normal_all,
            dataopposite_sm, dataopposite_mp, dataopposite_ks, dataopposite_bb, dataopposite_tb, dataopposite_all,
            datalastdateopposite_sm, datalastdateopposite_mp, datalastdateopposite_ks, datalastdateopposite_bb,
            datalastdateopposite_tb, datalastdateopposite_all,
            datalast7opposite_sm, datalast7opposite_mp, datalast7opposite_ks, datalast7opposite_bb,
            datalast7opposite_tb, datalast7opposite_all,
            databoth_sm, databoth_mp, databoth_ks, databoth_bb, databoth_tb, databoth_all,
            datalastdateboth_sm, datalastdateboth_mp, datalastdateboth_ks, datalastdateboth_bb,
            datalastdateboth_tb, datalastdateboth_all,
            datalast7both_sm, datalast7both_mp, datalast7both_ks, datalast7both_bb, datalast7both_tb,
            datalast7both_all
        )
        VALUES (
            ?, CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON),
            CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON), CAST(? AS JSON)
        )
    """
    keys = [
        "hourlyLabel",
        "dataNormal_SM", "dataNormal_MP", "dataNormal_KS", "dataNormal_BB", "dataNormal_TB", "dataNormal_All",
        "dataLastDateNormal_SM", "dataLastDateNormal_MP", "dataLastDateNormal_KS", "dataLastDateNormal_BB",
        "dataLastDateNormal_TB", "dataLastDateNormal_All",
        "dataLast7Normal_SM", "dataLast7Normal_MP", "dataLast7Normal_KS", "dataLast7Normal_BB",
        "dataLast7Normal_TB", "dataLast7Normal_All",
        "dataOpposite_SM", "dataOpposite_MP", "dataOpposite_KS", "dataOpposite_BB", "dataOpposite_TB", "dataOpposite_All",
        "dataLastDateOpposite_SM", "dataLastDateOpposite_MP", "dataLastDateOpposite_KS", "dataLastDateOpposite_BB",
        "dataLastDateOpposite_TB", "dataLastDateOpposite_All",
        "dataLast7Opposite_SM", "dataLast7Opposite_MP", "dataLast7Opposite_KS", "dataLast7Opposite_BB",
        "dataLast7Opposite_TB", "dataLast7Opposite_All",
        "dataBoth_SM", "dataBoth_MP", "dataBoth_KS", "dataBoth_BB", "dataBoth_TB", "dataBoth_All",
        "dataLastDateBoth_SM", "dataLastDateBoth_MP", "dataLastDateBoth_KS", "dataLastDateBoth_BB",
        "dataLastDateBoth_TB", "dataLastDateBoth_All",
        "dataLast7Both_SM", "dataLast7Both_MP", "dataLast7Both_KS", "dataLast7Both_BB", "dataLast7Both_TB",
        "dataLast7Both_All",
    ]
    con.execute(sql, [camera_code] + [json.dumps(payload.get(k, [])) for k in keys])


def crawlOnce(con: duckdb.DuckDBPyConnection, camera_rows: List[tuple]):
    for row in camera_rows:
        camera_code = str(row[0]).strip()
        api_id = normalizeCameraApiId(row)
        referrer = f"https://apace-ai.com/{api_id}/"
        try:
            ingestVehicleSpeed(con, camera_code, fetchJson(f"{API_CHART1}{api_id}", referrer=referrer))
            ingestHourlyVehicleSpeed(con, camera_code, fetchJson(f"{API_CHART2}{api_id}", referrer=referrer))
            ingestTimeseriesVehicleSpeed(con, camera_code, fetchJson(f"{API_CHART3}{api_id}", referrer=referrer))
        except requests.exceptions.Timeout as e:
            print(f"[WARN] {camera_code} ({api_id}) timeout: {e}")
        except requests.exceptions.HTTPError as e:
            print(f"[WARN] {camera_code} ({api_id}) HTTP error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"[WARN] {camera_code} ({api_id}) request failed: {e}")
        except json.JSONDecodeError as e:
            print(f"[WARN] {camera_code} ({api_id}) invalid JSON: {e}")
        except ValueError as e:
            print(f"[WARN] {camera_code} ({api_id}) value error: {e}")
        except Exception as e:
            print(f"[ERROR] {camera_code} ({api_id}) unexpected error: {e}")


def main():
    con = duckdb.connect(str(RAW_DATA_DUCKDB))
    camera_rows = loadCameraRows(con)
    print(f"Started crawl. cameras={len(camera_rows)} interval={POLL_SECONDS}s db={RAW_DATA_DUCKDB}")
    crawlOnce(con, camera_rows)


if __name__ == "__main__":
    main()
