import json
from pydantic import ValidationError
from ingest.crawl.weather.fetch import BMKGSchema

def insertData(con: 'duckdb.DuckDBPyConnection', data):
    # Validate the input data using Pydantic
    if not isinstance(data, BMKGSchema):
        try:
            data = BMKGSchema(**(data.dict() if hasattr(data, "dict") else dict(data)))
        except (ValidationError, Exception) as e:
            raise ValueError(f"Data does not conform to BMKGSchema: {e}")

    # Now data is guaranteed to be a BMKGSchema
    con.execute(
        """
        INSERT INTO bmkg_weather
            (adm1, adm2, adm3, adm4, provinsi, kotkab, kecamatan, desa,
             lon, lat, timezone, weather_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            data.adm1,
            data.adm2,
            data.adm3,
            data.adm4,
            data.provinsi,
            data.kotkab,
            data.kecamatan,
            data.desa,
            data.lon,
            data.lat,
            data.timezone,
            json.dumps(data.weather_data, ensure_ascii=False),
        ]
    )