from __future__ import annotations

import duckdb


def findKabupaten(con: duckdb.DuckDBPyConnection, lat: float, lon: float):
    row = con.execute(
        """
        SELECT kode, nama, sqrt(pow(lat - ?, 2) + pow(lng - ?, 2)) AS distance
        FROM wilayah_provinsi_kabupaten
        WHERE lat IS NOT NULL AND lng IS NOT NULL AND length(kode) > 2
        ORDER BY distance ASC
        LIMIT 1
        """,
        [lat, lon],
    ).fetchall()
    return row[0]
