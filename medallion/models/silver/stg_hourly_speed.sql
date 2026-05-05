{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

WITH populate AS (
    SELECT
        camera_code,
        SAFE_CAST(speed_data AS INT64) AS speed_data,
        ingested_at,
        gol,
        hour_order
    FROM {{ source('sources', 'hourly_vehicle_speed') }},
    UNNEST(
        JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(listdata60_60_sm AS STRING)))
    ) AS speed_data WITH OFFSET AS hour_order
    INNER JOIN UNNEST(
        JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(hourly_label AS STRING)))
    ) AS gol WITH OFFSET AS hour_order2
        ON hour_order = hour_order2
)

SELECT * FROM populate

{% else %}

WITH populate AS (
    SELECT
        camera_code,
        unnest(listdata60_60_sm::INTEGER[]) AS speed_data,
        ingested_at,
        unnest(hourly_label::STRING[]) AS gol,
        generate_subscripts(hourly_label::STRING[], 1) AS "order"
    FROM {{ source('sources', 'hourly_vehicle_speed') }}
)

SELECT * FROM populate

{% endif %}
