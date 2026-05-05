import duckdb
import json
import os
from typing import Any, List

from dotenv import load_dotenv

load_dotenv()
RAW_DATA_DUCKDB = os.environ.get("INGEST_DUCKDB_PATH", "").strip()
CCTV_LIST_PATH = os.environ.get("CCTV_LIST_PATH", "").strip()
INGEST_SCHEMA_SQL_PATH = os.environ.get("INGEST_SCHEMA_SQL_PATH", "").strip()


def loadCameraRows():
    with open(CCTV_LIST_PATH, encoding="utf-8") as f:
        payload = json.loads(f.read())
    camera_array = payload.get("cameraArray", [])
    if not isinstance(camera_array, list):
        raise ValueError("cameraArray is not a list")
    return camera_array


def insertCameraList(con: duckdb.DuckDBPyConnection, camera_rows: List[List[Any]]):
    insert_sql = """
        INSERT INTO cctv_list (
            camera_code, latitude, longitude, location_text, route_slug, adm2, adm4
        ) VALUES (?, ?, ?, ?, ?,?,?)
    """
    for row in camera_rows:
        camera_code = str(row[0]) if len(row) > 0 else None
        lat = float(row[1]) if len(row) > 1 and row[1] is not None else None
        lon = float(row[2]) if len(row) > 2 and row[2] is not None else None
        location_text = str(row[4]) if len(row) > 4 else None
        route_slug = str(row[5]) if len(row) > 5 else None
        con.execute(insert_sql, [camera_code, lat, lon, location_text, route_slug, "NULL", "NULL"])


def initDb(con: duckdb.DuckDBPyConnection):
    with open(INGEST_SCHEMA_SQL_PATH, encoding="utf-8") as f:
        con.execute(f.read())


if __name__ == "__main__":
    camera_rows = loadCameraRows()
    con = duckdb.connect(str(RAW_DATA_DUCKDB))
    initDb(con)
    insertCameraList(con, camera_rows)
    print("Success")
