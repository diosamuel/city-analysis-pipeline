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

{% if target.type == 'bigquery' %}

SELECT
    station_id,
    observed_at,
    CAST(NULL AS INT64) AS adm4,
    aqi_value,
    aqi_category,
    CAST(dominant_params_json AS STRING) AS dominant_params_json
FROM {{ ref('stg_air_quality') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station_id, observed_at
    ORDER BY aqi_value DESC NULLS LAST
) = 1

{% else %}

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

{% endif %}
