import os
import time

import duckdb
from dotenv import load_dotenv

from cctv_to_kecamatan import getKecamatanName, findKecamatanKode, NOMINATIM_DELAY_S
from cctv_to_kabupaten import findKabupaten

load_dotenv()
RAW_DATA_DUCKDB = os.environ.get("INGEST_DUCKDB_PATH", "").strip()


def runFinalize(con: duckdb.DuckDBPyConnection):
    rows = con.execute(
        """
        SELECT camera_code, latitude, longitude, location_text, route_slug
        FROM cctv_list
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY camera_code
        """
    ).fetchall()
    if not rows:
        print("No CCTV rows with coordinates.")
        return

    def recordQuarantine(camera_code, lat, lon, location_text, route_slug, kode_kabupaten, kode_kecamatan, reason):
        con.execute(
            """
            INSERT INTO cctv_list_quarantine
                (camera_code, latitude, longitude, location_text, route_slug, adm2, adm4)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [camera_code, float(lat), float(lon), location_text, route_slug, kode_kabupaten, kode_kecamatan],
        )
        print(f"QUARANTINE {camera_code} adm2={kode_kabupaten!r} adm4={kode_kecamatan!r} reason={reason}")

    for i, (camera_code, lat, lon, location_text, route_slug) in enumerate(rows):
        if i and NOMINATIM_DELAY_S > 0:
            time.sleep(NOMINATIM_DELAY_S)
        kecamatan_name = getKecamatanName(float(lat), float(lon))
        kabupaten_row = findKabupaten(con, float(lat), float(lon))
        kode_kabupaten = kabupaten_row[0] if kabupaten_row else None
        nama_kabupaten = kabupaten_row[1] if kabupaten_row else None
        wilayah_results = []
        if kecamatan_name and kode_kabupaten:
            wilayah_results = findKecamatanKode(con, kecamatan_name, kode_kabupaten)
        if not wilayah_results:
            recordQuarantine(camera_code, lat, lon, location_text, route_slug, kode_kabupaten, None, "no wilayah match")
            continue
        best_nama, kode_kecamatan, best_sim = wilayah_results[0]
        print(wilayah_results)
        if len(kode_kecamatan) <= 8:
            deeper = con.execute(
                """
                SELECT kode, nama FROM wilayah_kecamatan_kelurahan
                WHERE kode LIKE ? || '%' AND length(kode) > 8
                LIMIT 1
                """,
                [kode_kecamatan],
            ).fetchone()
            if deeper:
                kode_kecamatan, best_nama = deeper[0], deeper[1]
            else:
                recordQuarantine(
                    camera_code, lat, lon, location_text, route_slug,
                    kode_kabupaten, kode_kecamatan, "kode too short, no deeper kode found",
                )
                continue
        con.execute(
            """
            INSERT INTO cctv_list_final
                (camera_code, latitude, longitude, location_text, route_slug, adm2, adm4)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [camera_code, float(lat), float(lon), location_text, route_slug, kode_kabupaten, kode_kecamatan],
        )
        print(f"INSERT {camera_code} adm2={kode_kabupaten!r} ({nama_kabupaten!r}) adm4={kode_kecamatan!r} ({best_nama!r}, sim={best_sim})")
    print("Done finalize.")


if __name__ == "__main__":
    con = duckdb.connect(str(RAW_DATA_DUCKDB))
    runFinalize(con)
    con.close()
