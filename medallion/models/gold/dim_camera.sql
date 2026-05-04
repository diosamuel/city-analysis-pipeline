{{
  config(
    materialized='table'
  )
}}

/*
  Gold dimension dim_camera (schema/dwh.sql): one row per camera_code.
  Surrogate camera_sk is a stable dense id ordered by camera_code (rebuildable on full refresh).
*/

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_cctv') }}
),

typed AS (
    SELECT
        TRIM(camera_code)::VARCHAR AS camera_code,
        latitude::DOUBLE AS latitude,
        longitude::DOUBLE AS longitude,
        route_slug::VARCHAR AS route_slug,
        adm2::VARCHAR AS adm2,
        adm4::VARCHAR AS adm4_sk,
        kode::VARCHAR AS location_kode,
        jalan::VARCHAR AS jalan,
        kabupaten::VARCHAR AS kabupaten
    FROM stg
)

SELECT
    ROW_NUMBER() OVER (ORDER BY camera_code)::BIGINT AS camera_sk,
    camera_code,
    latitude,
    longitude,
    route_slug,
    adm2,
    adm4_sk,
    location_kode,
    jalan,
    kabupaten
FROM typed
