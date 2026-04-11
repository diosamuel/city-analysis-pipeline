from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ValidationError
import json
"""
This is chart 1 response api
"""
class VehicleSpeedSchema(BaseModel):
    camera_code: str
    lastUpdate5minutes: str
    label5MinData: list
    listChart1_Normal: list
    listChart1_Opposite: list
    speed_Normal: list
    speed_Opposite: list
    speed_gol_Normal: list
    speed_gol_Opposite: list
    speed_max_gol_Normal_list: list
    speed_max_gol_Opposite_list: list

def ingest_vehicle_speed(con: 'duckdb.DuckDBPyConnection', camera_code: str, payload: Dict[str, Any]):
    try:
        # Compose the validated payload using the schema
        data = VehicleSpeedSchema(camera_code=camera_code, **payload)
    except ValidationError as ve:
        # Handle validation errors appropriately
        raise ValueError(f"Invalid vehicle speed payload: {ve}")

    sql = """
        INSERT INTO vehicle_speed (
            camera_code,
            last_update_5minutes,
            label5min_data,
            listchart1_normal,
            listchart1_opposite,
            speed_normal,
            speed_opposite,
            speed_gol_normal,
            speed_gol_opposite,
            speed_max_gol_normal_list,
            speed_max_gol_opposite_list
        )
        VALUES (
            ?,
            ?,
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON)
        )
    """
    con.execute(
        sql,
        [
            data.camera_code,
            data.lastUpdate5minutes,
            json.dumps(data.label5MinData),
            json.dumps(data.listChart1_Normal),
            json.dumps(data.listChart1_Opposite),
            json.dumps(data.speed_Normal),
            json.dumps(data.speed_Opposite),
            json.dumps(data.speed_gol_Normal),
            json.dumps(data.speed_gol_Opposite),
            json.dumps(data.speed_max_gol_Normal_list),
            json.dumps(data.speed_max_gol_Opposite_list),
        ],
    )
