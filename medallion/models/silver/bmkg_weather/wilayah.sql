{{ config(materialized='view') }}

WITH wilayah AS (
    SELECT unnest(weather_data::JSON[], recursive := TRUE) AS w
    FROM {{ source('bronze', 'bmkg_weather') }}
),

daily_forecasts AS (
    SELECT
        w.lokasi                       AS lokasi,
        unnest(w.cuaca::JSON[][])      AS day_forecasts
    FROM wilayah
),

forecasts AS (
    SELECT
        lokasi,
        unnest(day_forecasts::JSON[])  AS f
    FROM daily_forecasts
)

SELECT
    lokasi ->> 'adm1'                    AS adm1,
    lokasi ->> 'adm2'                    AS adm2,
    lokasi ->> 'adm3'                    AS adm3,
    lokasi ->> 'adm4'                    AS adm4,
    lokasi ->> 'provinsi'                AS provinsi,
    lokasi ->> 'kotkab'                  AS kotkab,
    lokasi ->> 'kecamatan'               AS kecamatan,
    lokasi ->> 'desa'                    AS desa,
    (lokasi ->> 'lon')::DOUBLE           AS lon,
    (lokasi ->> 'lat')::DOUBLE           AS lat,
    --lokasi ->> 'timezone'                AS timezone,
    --lokasi ->> 'type'                    AS type,

    (f ->> 'datetime')::TIMESTAMPTZ      AS forecast_datetime_utc,
    (f ->> 't')::INTEGER                 AS temperature_c,
    (f ->> 'tcc')::INTEGER               AS total_cloud_cover_pct,
    (f ->> 'tp')::DOUBLE                 AS total_precipitation_mm,
    (f ->> 'weather')::INTEGER           AS weather_code,
    f ->> 'weather_desc'                 AS weather_desc,
    f ->> 'weather_desc_en'              AS weather_desc_en,
    (f ->> 'wd_deg')::INTEGER            AS wind_direction_deg,
    f ->> 'wd'                           AS wind_direction,
    f ->> 'wd_to'                        AS wind_direction_to,
    (f ->> 'ws')::DOUBLE                 AS wind_speed_kmh,
    (f ->> 'hu')::INTEGER                AS humidity_pct,
    (f ->> 'vs')::INTEGER                AS visibility_m,
    f ->> 'vs_text'                      AS visibility_text,
    f ->> 'time_index'                   AS time_index,
    (f ->> 'analysis_date')::TIMESTAMP   AS analysis_date,
    --(f ->> 'utc_datetime')::TIMESTAMP    AS utc_datetime,
    --(f ->> 'local_datetime')::TIMESTAMP  AS local_datetime
FROM forecasts
