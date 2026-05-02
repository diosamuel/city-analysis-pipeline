{{ config(materialized='view') }}

WITH populate AS (
    SELECT
        camera_code,
        unnest(listdata60_60_sm::INTEGER[]) AS speed_data,
        ingested_at,
        unnest(hourly_label::STRING[]) AS gol,
        generate_subscripts(hourly_label::STRING[], 1) AS "order"
    FROM {{ source('bronze', 'hourly_vehicle_speed') }}
)

SELECT * FROM populate
