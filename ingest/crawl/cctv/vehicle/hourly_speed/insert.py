
from typing import List, Optional
from pydantic import BaseModel,ValidationError
import json
"""
This is chart 2 response api
"""
class HourlyVehicleSchema(BaseModel):
    camera_code: str
    hourly_label: list
    listdata60_60_sm: list
    listdata60_60_mp: list
    listdata60_60_ks: list
    listdata60_60_bb: list
    listdata60_60_tb: list
    totalvolume_sm: int
    totalvolume_mp: int
    totalvolume_ks: int
    totalvolume_bb: int
    totalvolume_tb: int

def ingest_hourly_vehicle_speed(con: 'duckdb.DuckDBPyConnection', hourly_vehicle_data):
    try:
        # Validate data using the schema
        if isinstance(hourly_vehicle_data, HourlyVehicleSchema):
            hourly_vehicle = hourly_vehicle_data
        else:
            hourly_vehicle = HourlyVehicleSchema(**hourly_vehicle_data)
    except ValidationError as ve:
        raise ValueError(f"Invalid hourly vehicle speed schema: {ve}")

    sql = """
        INSERT INTO hourly_vehicle_speed (
            camera_code,
            hourly_label,
            listdata60_60_sm,
            listdata60_60_mp,
            listdata60_60_ks,
            listdata60_60_bb,
            listdata60_60_tb,
            totalvolume_sm,
            totalvolume_mp,
            totalvolume_ks,
            totalvolume_bb,
            totalvolume_tb
        )
        VALUES (
            ?,
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            CAST(? AS JSON),
            ?, ?, ?, ?, ?
        )
    """
    con.execute(
        sql,
        [
            hourly_vehicle.camera_code,
            json.dumps(hourly_vehicle.hourly_label),
            json.dumps(hourly_vehicle.listdata60_60_sm),
            json.dumps(hourly_vehicle.listdata60_60_mp),
            json.dumps(hourly_vehicle.listdata60_60_ks),
            json.dumps(hourly_vehicle.listdata60_60_bb),
            json.dumps(hourly_vehicle.listdata60_60_tb),
            hourly_vehicle.totalvolume_sm,
            hourly_vehicle.totalvolume_mp,
            hourly_vehicle.totalvolume_ks,
            hourly_vehicle.totalvolume_bb,
            hourly_vehicle.totalvolume_tb,
        ],
    )
