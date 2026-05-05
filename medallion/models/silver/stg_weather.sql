{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

WITH raw AS (
    SELECT PARSE_JSON(CAST(weather_data AS STRING)) AS wd
    FROM {{ source('sources', 'bmkg_weather') }}
),

wilayah AS (
    SELECT elem AS w
    FROM raw,
    UNNEST(JSON_QUERY_ARRAY(wd)) AS elem
),

daily_forecasts AS (
    SELECT
        JSON_QUERY(w, '$.lokasi') AS lokasi,
        day_bucket
    FROM wilayah,
    UNNEST(JSON_QUERY_ARRAY(JSON_QUERY(w, '$.cuaca'))) AS day_bucket
),

forecasts AS (
    SELECT
        lokasi,
        f
    FROM daily_forecasts,
    UNNEST(JSON_QUERY_ARRAY(day_bucket)) AS f
)

SELECT
    JSON_VALUE(lokasi, '$.adm1') AS adm1,
    JSON_VALUE(lokasi, '$.adm2') AS adm2,
    JSON_VALUE(lokasi, '$.adm3') AS adm3,
    JSON_VALUE(lokasi, '$.adm4') AS adm4,
    JSON_VALUE(lokasi, '$.provinsi') AS provinsi,
    JSON_VALUE(lokasi, '$.kotkab') AS kotkab,
    JSON_VALUE(lokasi, '$.kecamatan') AS kecamatan,
    JSON_VALUE(lokasi, '$.desa') AS desa,
    SAFE_CAST(JSON_VALUE(lokasi, '$.lon') AS FLOAT64) AS lon,
    SAFE_CAST(JSON_VALUE(lokasi, '$.lat') AS FLOAT64) AS lat,

    SAFE_CAST(JSON_VALUE(f, '$.datetime') AS TIMESTAMP) AS forecast_datetime_utc,
    SAFE_CAST(JSON_VALUE(f, '$.t') AS INT64) AS temperature_c,
    SAFE_CAST(JSON_VALUE(f, '$.tcc') AS INT64) AS total_cloud_cover_pct,
    SAFE_CAST(JSON_VALUE(f, '$.tp') AS FLOAT64) AS total_precipitation_mm,
    SAFE_CAST(JSON_VALUE(f, '$.weather') AS INT64) AS weather_code,
    JSON_VALUE(f, '$.weather_desc') AS weather_desc,
    JSON_VALUE(f, '$.weather_desc_en') AS weather_desc_en,
    SAFE_CAST(JSON_VALUE(f, '$.wd_deg') AS INT64) AS wind_direction_deg,
    JSON_VALUE(f, '$.wd') AS wind_direction,
    JSON_VALUE(f, '$.wd_to') AS wind_direction_to,
    SAFE_CAST(JSON_VALUE(f, '$.ws') AS FLOAT64) AS wind_speed_kmh,
    SAFE_CAST(JSON_VALUE(f, '$.hu') AS INT64) AS humidity_pct,
    SAFE_CAST(JSON_VALUE(f, '$.vs') AS INT64) AS visibility_m,
    JSON_VALUE(f, '$.vs_text') AS visibility_text,
    JSON_VALUE(f, '$.time_index') AS time_index,
    SAFE_CAST(JSON_VALUE(f, '$.analysis_date') AS TIMESTAMP) AS analysis_date
FROM forecasts

{% else %}

WITH wilayah AS (
    SELECT unnest(weather_data::JSON[], recursive := TRUE) AS w
    FROM {{ source('sources', 'bmkg_weather') }}
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
    (f ->> 'analysis_date')::TIMESTAMP   AS analysis_date
FROM forecasts

{% endif %}
