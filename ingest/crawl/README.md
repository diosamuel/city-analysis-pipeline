# isolate/

Isolate scripts call **`load_dotenv()`** then read paths with **`os.environ.get(..., "").strip()`**: **`INGEST_DUCKDB_PATH`**, **`CCTV_LIST_PATH`** (camera list JSON), **`INGEST_SCHEMA_SQL_PATH`** (DDL for `initDb`). Relative paths use **cwd**.

## Prerequisites

```
pip install duckdb requests python-dotenv
```

## File overview

### SQL

| File | Description |
|---|---|
| `schema.sql` | DuckDB DDL for all tables: `wilayah_kecamatan_kelurahan`, `wilayah_provinsi_kabupaten`, `cctv_list`, `vehicle_speed`, `hourly_vehicle_speed`, `all_timeseries_vehicle_speed`, `bmkg_weather`, `air_quality` |
| `wilayah_provinsi_kabupaten.sql` | Insert data for kabupaten/provinsi reference table (kode, nama, lat, lng, etc.) |
| `wilayah_kecamatan_kelurahan.sql` | Insert data for kecamatan/kelurahan reference table (kode, nama) |

### Python — setup & transform

| File | Description |
|---|---|
| `cctv_transform.py` | **Step 1.** Reads **`INGEST_SCHEMA_SQL_PATH`** and **`CCTV_LIST_PATH`** from `.env`, initializes schema and loads `cctv_list`. Functions: `initDb(con)`, `loadCameraRows()`, `insertCameraList(con, rows)` |
| `cctv_to_kabupaten.py` | Finds the nearest kabupaten for a given lat/lon using Euclidean distance on `wilayah_provinsi_kabupaten`. Function: `findKabupaten(con, lat, lon)` |
| `cctv_to_kecamatan.py` | Reverse-geocodes lat/lon via Nominatim OSM to get a kecamatan name, then matches it against `wilayah_kecamatan_kelurahan` using Levenshtein distance. Functions: `getKecamatanName(lat, lon)`, `findKecamatanKode(con, name, kode_kabupaten)` |
| `cctv_quarantine.py` | **Step 2.** For each CCTV camera, calls `getKecamatanName` + `findKabupaten` + `findKecamatanKode` to resolve `adm2` (kabupaten kode) and `adm4` (kecamatan kode). Valid cameras go into `cctv_list_final`, others into `cctv_list_quarantine`. Function: `runFinalize(con)` |

### Python — data ingestion

| File | Description |
|---|---|
| `cctv_ingest_lifetime.py` | **Step 3.** Polls apace-ai chart APIs (chart1, chart2, chart3) for each camera and inserts into `vehicle_speed`, `hourly_vehicle_speed`, `all_timeseries_vehicle_speed`. Runs in an infinite loop with configurable interval. Functions: `crawlOnce(con, rows)`, `loadCameraRows(con)` |
| `cctv_to_bmkg.py` | **Step 4.** Fetches BMKG weather forecasts (`https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=...`) for each distinct `adm4` in `cctv_list_final` and inserts into `bmkg_weather`. Function: `fetchBmkgWeather(adm4)` |
| `air_quality_index_insert.py` | **Step 5.** Fetches all ISPU stations from KemenLH (`https://ispu.kemenlh.go.id/apimobile/v1/getStations`) and inserts into `air_quality`. Functions: `fetchStations()`, `insertStation(con, row)` |

### Data

| File | Description |
|---|---|
| `dataset/cctv-list.json` | Default camera list location; override with **`CCTV_LIST_PATH`** in `.env` |

## Pipeline order

```
1. cctv_transform.py: init schema + load cctv_list
2. cctv_quarantine.py: geocode, resolve adm2/adm4, cctv_list_final
3. cctv_ingest_lifetime.py: CCTV chart data
4. cctv_to_bmkg.py: BMKG weather
5. air_quality_index_insert.py: KemenLH AQI
```

## Running individually

```bash
cd isolate
python cctv_transform.py
python cctv_quarantine.py
python cctv_ingest_lifetime.py
python cctv_to_bmkg.py
python air_quality_index_insert.py
```

## API sources

| Source | URL |
|---|---|
| CCTV Charts (apace-ai) | `https://apace-ai.com/generateChart{1,2,3}Data/{cctvId}` |
| BMKG Weather | `https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={adm4}` |
| KemenLH ISPU | `https://ispu.kemenlh.go.id/apimobile/v1/getStations` |
| Nominatim OSM | `https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}` |
