{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

WITH source AS (
    SELECT *
    FROM {{ source('sources', 'cctv_list_final') }}
),

cleaned AS (
    SELECT
        camera_code,
        latitude,
        longitude,
        location_text,
        route_slug,
        adm2,
        adm4,
        ARRAY(
            SELECT TRIM(t)
            FROM UNNEST(
                SPLIT(REGEXP_REPLACE(TRIM(COALESCE(location_text, '')), r'\s{2,}|\r|\n', '\x01'), '\x01')
            ) AS t
            WHERE TRIM(t) <> ''
        ) AS location_parts
    FROM source
)

SELECT
    TRIM(camera_code) AS camera_code,
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude,
    route_slug,
    adm2,
    adm4,
    location_parts[SAFE_OFFSET(0)] AS kode,
    location_parts[SAFE_OFFSET(1)] AS jalan,
    location_parts[SAFE_OFFSET(2)] AS kabupaten
FROM cleaned
WHERE camera_code IS NOT NULL
  AND TRIM(camera_code) <> ''
  AND latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(camera_code) ORDER BY camera_code) = 1

{% else %}

WITH source AS (
    SELECT *
    FROM {{ source('sources', 'cctv_list_final') }}
),

cleaned AS (
    SELECT
        camera_code,
        latitude,
        longitude,
        location_text,
        route_slug,
        adm2,
        adm4,
        list_filter(
            regexp_split_to_array(TRIM(location_text), '\r?\n|\s{2,}'),
            x -> TRIM(x) <> ''
        ) AS location_parts
    FROM source
)

SELECT
    TRIM(camera_code)::VARCHAR          AS camera_code,
    latitude::DOUBLE                    AS latitude,
    longitude::DOUBLE                   AS longitude,
    route_slug::VARCHAR                 AS route_slug,
    adm2::VARCHAR                       AS adm2,
    adm4::VARCHAR                       AS adm4,
    location_parts[1]::VARCHAR          AS kode,
    location_parts[2]::VARCHAR          AS jalan,
    location_parts[3]::VARCHAR          AS kabupaten
FROM cleaned
WHERE camera_code IS NOT NULL
  AND TRIM(camera_code) <> ''
  AND latitude  BETWEEN -90  AND 90
  AND longitude BETWEEN -180 AND 180
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(camera_code) ORDER BY camera_code) = 1

{% endif %}
