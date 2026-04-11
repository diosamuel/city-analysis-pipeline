from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ValidationError
import json

"""
This is chart 3 response api
"""
class TimeseriesVehicleSpeedSchema(BaseModel):
    hourlyLabel: list
    dataNormal_SM: list
    dataNormal_MP: list
    dataNormal_KS: list
    dataNormal_BB: list
    dataNormal_TB: list
    dataNormal_All: list
    dataLastDateNormal_SM: list
    dataLastDateNormal_MP: list
    dataLastDateNormal_KS: list
    dataLastDateNormal_BB: list
    dataLastDateNormal_TB: list
    dataLastDateNormal_All: list
    dataLast7Normal_SM: list
    dataLast7Normal_MP: list
    dataLast7Normal_KS: list
    dataLast7Normal_BB: list
    dataLast7Normal_TB: list
    dataLast7Normal_All: list
    dataOpposite_SM: list
    dataOpposite_MP: list
    dataOpposite_KS: list
    dataOpposite_BB: list
    dataOpposite_TB: list
    dataOpposite_All: list
    dataLastDateOpposite_SM: list
    dataLastDateOpposite_MP: list
    dataLastDateOpposite_KS: list
    dataLastDateOpposite_BB: list
    dataLastDateOpposite_TB: list
    dataLastDateOpposite_All: list
    dataLast7Opposite_SM: list
    dataLast7Opposite_MP: list
    dataLast7Opposite_KS: list
    dataLast7Opposite_BB: list
    dataLast7Opposite_TB: list
    dataLast7Opposite_All: list
    dataBoth_SM: list
    dataBoth_MP: list
    dataBoth_KS: list
    dataBoth_BB: list
    dataBoth_TB: list
    dataBoth_All: list
    dataLastDateBoth_SM: list
    dataLastDateBoth_MP: list
    dataLastDateBoth_KS: list
    dataLastDateBoth_BB: list
    dataLastDateBoth_TB: list
    dataLastDateBoth_All: list
    dataLast7Both_SM: list
    dataLast7Both_MP: list
    dataLast7Both_KS: list
    dataLast7Both_BB: list
    dataLast7Both_TB: list
    dataLast7Both_All: list

def ingest_timeseries_vehicle_speed(con: 'duckdb.DuckDBPyConnection', camera_code: str, payload: Dict[str, Any]):
    try:
        data = TimeseriesVehicleSpeedSchema(**payload)
    except ValidationError as ve:
        raise ValueError(f"Invalid timeseries vehicle speed payload: {ve}")

    sql = """
        INSERT INTO all_timeseries_vehicle_speed (
            camera_code,
            hourly_label,
            datanormal_sm, datanormal_mp, datanormal_ks, datanormal_bb, datanormal_tb, datanormal_all,
            datalastdatenormal_sm, datalastdatenormal_mp, datalastdatenormal_ks, datalastdatenormal_bb, datalastdatenormal_tb, datalastdatenormal_all,
            datalast7normal_sm, datalast7normal_mp, datalast7normal_ks, datalast7normal_bb, datalast7normal_tb, datalast7normal_all,
            dataopposite_sm, dataopposite_mp, dataopposite_ks, dataopposite_bb, dataopposite_tb, dataopposite_all,
            datalastdateopposite_sm, datalastdateopposite_mp, datalastdateopposite_ks, datalastdateopposite_bb, datalastdateopposite_tb, datalastdateopposite_all,
            datalast7opposite_sm, datalast7opposite_mp, datalast7opposite_ks, datalast7opposite_bb, datalast7opposite_tb, datalast7opposite_all,
            databoth_sm, databoth_mp, databoth_ks, databoth_bb, databoth_tb, databoth_all,
            datalastdateboth_sm, datalastdateboth_mp, datalastdateboth_ks, datalastdateboth_bb, datalastdateboth_tb, datalastdateboth_all,
            datalast7both_sm, datalast7both_mp, datalast7both_ks, datalast7both_bb, datalast7both_tb, datalast7both_all
        )
        VALUES (
            ?,
            CAST(? AS JSON),
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

    # List the attributes in the same order as columns in the SQL insert
    keys = [
        "hourlyLabel",
        "dataNormal_SM", "dataNormal_MP", "dataNormal_KS", "dataNormal_BB", "dataNormal_TB", "dataNormal_All",
        "dataLastDateNormal_SM", "dataLastDateNormal_MP", "dataLastDateNormal_KS", "dataLastDateNormal_BB", "dataLastDateNormal_TB", "dataLastDateNormal_All",
        "dataLast7Normal_SM", "dataLast7Normal_MP", "dataLast7Normal_KS", "dataLast7Normal_BB", "dataLast7Normal_TB", "dataLast7Normal_All",
        "dataOpposite_SM", "dataOpposite_MP", "dataOpposite_KS", "dataOpposite_BB", "dataOpposite_TB", "dataOpposite_All",
        "dataLastDateOpposite_SM", "dataLastDateOpposite_MP", "dataLastDateOpposite_KS", "dataLastDateOpposite_BB", "dataLastDateOpposite_TB", "dataLastDateOpposite_All",
        "dataLast7Opposite_SM", "dataLast7Opposite_MP", "dataLast7Opposite_KS", "dataLast7Opposite_BB", "dataLast7Opposite_TB", "dataLast7Opposite_All",
        "dataBoth_SM", "dataBoth_MP", "dataBoth_KS", "dataBoth_BB", "dataBoth_TB", "dataBoth_All",
        "dataLastDateBoth_SM", "dataLastDateBoth_MP", "dataLastDateBoth_KS", "dataLastDateBoth_BB", "dataLastDateBoth_TB", "dataLastDateBoth_All",
        "dataLast7Both_SM", "dataLast7Both_MP", "dataLast7Both_KS", "dataLast7Both_BB", "dataLast7Both_TB", "dataLast7Both_All",
    ]
    values = [camera_code] + [json.dumps(getattr(data, k, [])) for k in keys]
    con.execute(sql, values)