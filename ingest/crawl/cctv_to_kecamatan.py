from __future__ import annotations

import duckdb
import requests

NOMINATIM = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_DELAY_S = 1.05
CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
KECAMATAN_KEYS = ("city_district", "village", "town", "suburb")


def getKecamatanName(lat: float, lon: float):
    r = requests.get(
        NOMINATIM,
        params={"lat": lat, "lon": lon, "format": "json", "addressdetails": 1},
        headers={"User-Agent": CHROME_UA, "Accept": "application/json", "Accept-Language": "id,en"},
        timeout=30,
    )
    r.raise_for_status()
    addr = r.json().get("address")
    if not isinstance(addr, dict):
        return None
    for key in KECAMATAN_KEYS:
        val = addr.get(key)
        if val and str(val).strip():
            return str(val).strip()
    return None


def findKecamatanKode(con: duckdb.DuckDBPyConnection, kecamatan_name: str, kode_kabupaten: str):
    if not kecamatan_name or not kecamatan_name.strip():
        return []
    nama_kecamatan = kecamatan_name.strip().lower()
    return con.execute(
        """
        SELECT nama, kode, levenshtein(?, lower(nama)) AS sim
        FROM wilayah_kecamatan_kelurahan
        WHERE sim < 2 AND kode LIKE ? || '%' AND length(kode) > 8
        ORDER BY sim ASC
        """,
        [nama_kecamatan, kode_kabupaten],
    ).fetchall()
