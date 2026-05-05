{{
  config(
    materialized='table'
  )
}}

/*
  Gold dimension dim_weather (schema/dwh.sql):
  grain: (weather_location_sk, forecast_datetime_utc).
  weather_location_sk matches dim_weather_location surrogate keys computed here from
  distinct BMKG lokasi attributes in staging (dense row_number over stable location columns).
*/

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_weather') }}
    WHERE forecast_datetime_utc IS NOT NULL
),

{% if target.type == 'bigquery' %}

location_keys AS (
    SELECT DISTINCT
        CAST(adm4 AS STRING) AS adm4_sk,
        CAST(provinsi AS STRING) AS provinsi,
        CAST(kotkab AS STRING) AS kotkab,
        CAST(kecamatan AS STRING) AS kecamatan,
        CAST(desa AS STRING) AS desa,
        CAST(lat AS FLOAT64) AS lat,
        CAST(lon AS FLOAT64) AS lon
    FROM stg
),

location_sk AS (
    SELECT
        adm4_sk,
        provinsi,
        kotkab,
        kecamatan,
        desa,
        lat,
        lon,
        CAST(
            ROW_NUMBER()
                OVER (
                    ORDER BY
                        adm4_sk NULLS LAST,
                        provinsi NULLS LAST,
                        kotkab NULLS LAST,
                        kecamatan NULLS LAST,
                        desa NULLS LAST,
                        lat NULLS LAST,
                        lon NULLS LAST
                ) AS INT64
        ) AS weather_location_sk
    FROM location_keys
),

enriched AS (
    SELECT
        ls.weather_location_sk,
        CAST(s.forecast_datetime_utc AS TIMESTAMP) AS forecast_datetime_utc,
        CAST(s.adm4 AS STRING) AS adm4_sk,
        CAST(s.lat AS FLOAT64) AS lat,
        CAST(s.lon AS FLOAT64) AS lon,
        CAST(s.provinsi AS STRING) AS provinsi,
        CAST(s.kotkab AS STRING) AS kotkab,
        CAST(s.kecamatan AS STRING) AS kecamatan,
        CAST(s.temperature_c AS INT64) AS temperature_c,
        CAST(s.weather_code AS INT64) AS weather_code,
        CAST(s.weather_desc_en AS STRING) AS weather_desc_en,
        CAST(s.wind_speed_kmh AS FLOAT64) AS wind_speed_kmh,
        CAST(s.humidity_pct AS INT64) AS humidity_pct
    FROM stg AS s
    INNER JOIN location_sk AS ls
        ON s.adm4 IS NOT DISTINCT FROM ls.adm4_sk
        AND s.provinsi IS NOT DISTINCT FROM ls.provinsi
        AND s.kotkab IS NOT DISTINCT FROM ls.kotkab
        AND s.kecamatan IS NOT DISTINCT FROM ls.kecamatan
        AND s.desa IS NOT DISTINCT FROM ls.desa
        AND s.lat IS NOT DISTINCT FROM ls.lat
        AND s.lon IS NOT DISTINCT FROM ls.lon
)

SELECT
    weather_location_sk,
    forecast_datetime_utc,
    adm4_sk,
    lat,
    lon,
    provinsi,
    kotkab,
    kecamatan,
    temperature_c,
    weather_code,
    weather_desc_en,
    wind_speed_kmh,
    humidity_pct
FROM enriched
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY weather_location_sk, forecast_datetime_utc
    ORDER BY temperature_c DESC NULLS LAST
) = 1

{% else %}

location_keys AS (
    SELECT DISTINCT
        adm4::VARCHAR(13) AS adm4_sk,
        provinsi::VARCHAR(128) AS provinsi,
        kotkab::VARCHAR(128) AS kotkab,
        kecamatan::VARCHAR(128) AS kecamatan,
        desa::VARCHAR(128) AS desa,
        lat::DOUBLE AS lat,
        lon::DOUBLE AS lon
    FROM stg
),

location_sk AS (
    SELECT
        adm4_sk,
        provinsi,
        kotkab,
        kecamatan,
        desa,
        lat,
        lon,
        ROW_NUMBER()
            OVER (
                ORDER BY
                    adm4_sk NULLS LAST,
                    provinsi NULLS LAST,
                    kotkab NULLS LAST,
                    kecamatan NULLS LAST,
                    desa NULLS LAST,
                    lat NULLS LAST,
                    lon NULLS LAST
            )::INTEGER AS weather_location_sk
    FROM location_keys
),

enriched AS (
    SELECT
        ls.weather_location_sk,
        CAST(s.forecast_datetime_utc AS TIMESTAMP) AS forecast_datetime_utc,
        s.adm4::VARCHAR(13) AS adm4_sk,
        s.lat::DOUBLE AS lat,
        s.lon::DOUBLE AS lon,
        s.provinsi::VARCHAR(128) AS provinsi,
        s.kotkab::VARCHAR(128) AS kotkab,
        s.kecamatan::VARCHAR(128) AS kecamatan,
        CAST(s.temperature_c AS INTEGER) AS temperature_c,
        s.weather_code::INTEGER AS weather_code,
        s.weather_desc_en::VARCHAR(128) AS weather_desc_en,
        s.wind_speed_kmh::DOUBLE AS wind_speed_kmh,
        CAST(s.humidity_pct AS INTEGER) AS humidity_pct
    FROM stg AS s
    INNER JOIN location_sk AS ls
        ON s.adm4 IS NOT DISTINCT FROM ls.adm4_sk
        AND s.provinsi IS NOT DISTINCT FROM ls.provinsi
        AND s.kotkab IS NOT DISTINCT FROM ls.kotkab
        AND s.kecamatan IS NOT DISTINCT FROM ls.kecamatan
        AND s.desa IS NOT DISTINCT FROM ls.desa
        AND s.lat IS NOT DISTINCT FROM ls.lat
        AND s.lon IS NOT DISTINCT FROM ls.lon
)

SELECT DISTINCT ON (
    weather_location_sk,
    forecast_datetime_utc
)
    weather_location_sk,
    forecast_datetime_utc,
    adm4_sk,
    lat,
    lon,
    provinsi,
    kotkab,
    kecamatan,
    temperature_c,
    weather_code,
    weather_desc_en,
    wind_speed_kmh,
    humidity_pct
FROM enriched
ORDER BY
    weather_location_sk,
    forecast_datetime_utc,
    temperature_c DESC NULLS LAST

{% endif %}
