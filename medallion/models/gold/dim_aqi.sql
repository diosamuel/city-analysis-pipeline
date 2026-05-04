{{
  config(
    materialized='table'
  )
}}

/*
  Gold dimension dim_air_quality (schema/dwh.sql):
  grain: one row per (station_id, observed_at).
  adm4 is reserved for joins to wilayah/CCTV once matched in staging or a bridge model.
*/

SELECT DISTINCT ON (station_id, observed_at)
    station_id,
    observed_at,
    CAST(NULL AS INTEGER) AS adm4,
    aqi_value,
    aqi_category,
    CAST(dominant_params_json AS VARCHAR) AS dominant_params_json
FROM {{ ref('stg_air_quality') }}
ORDER BY
    station_id,
    observed_at,
    aqi_value DESC NULLS LAST
