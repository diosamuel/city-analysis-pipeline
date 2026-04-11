from ingest.crawl.aqi.fetch import AirQualitySchema
import json
from pydantic import ValidationError

def insertStation(con: 'duckdb.DuckDBPyConnection', data) -> None:
    # Validate the input data using Pydantic
    if not isinstance(data, AirQualitySchema):
        try:
            # Attempt to parse and validate `data` as AirQualitySchema
            data = AirQualitySchema(**(data.dict() if hasattr(data, "dict") else dict(data)))
        except (ValidationError, Exception) as e:
            raise ValueError(f"Data does not conform to AirQualitySchema: {e}")

    # Now data is guaranteed to be an AirQualitySchema
    con.execute(
        """
        INSERT INTO air_quality (
            id_stasiun, waktu, lat, lon,
            a_pm10, a_pm25, a_so2, a_co, a_o3, a_no2, a_hc,
            c_pm10, c_pm25, c_so2, c_co, c_o3, c_no2, c_hc,
            t_pm10, t_pm25, t_so2, t_co, t_o3, t_no2, t_hc,
            nama, alamat, kota, provinsi, param,
            stasiun_uji, stasiun_show, stasiun_show_detail,
            p3e, time_offset, time_z,
            is_maintenance, auto_validation, tipe,
            kelurahan, kecamatan,
            val, cat, kategori, waktu_text, tipe_text
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, CAST(? AS JSON), ?, ?
        )
        """,
        [
            data.id_stasiun,
            data.waktu,
            data.lat,
            data.lon,
            data.a_pm10,
            data.a_pm25,
            data.a_so2,
            data.a_co,
            data.a_o3,
            data.a_no2,
            data.a_hc,
            data.c_pm10,
            data.c_pm25,
            data.c_so2,
            data.c_co,
            data.c_o3,
            data.c_no2,
            data.c_hc,
            data.t_pm10,
            data.t_pm25,
            data.t_so2,
            data.t_co,
            data.t_o3,
            data.t_no2,
            data.t_hc,
            data.nama,
            data.alamat,
            data.kota,
            data.provinsi,
            data.param,
            data.stasiun_uji,
            data.stasiun_show,
            data.stasiun_show_detail,
            data.p3e,
            data.time_offset,
            data.time_z,
            data.is_maintenance,
            data.auto_validation,
            data.tipe,
            data.kelurahan,
            data.kecamatan,
            data.val,
            data.cat,
            json.dumps(data.kategori, ensure_ascii=False) if data.kategori else None,
            data.waktu_text,
            data.tipe_text,
        ],
    )