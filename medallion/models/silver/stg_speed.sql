{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

WITH label_cte AS (
    SELECT DISTINCT
        offset AS gol_id,
        gol
    FROM {{ source('sources', 'vehicle_speed') }},
    UNNEST(
        JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(label5min_data AS STRING)))
    ) AS gol WITH OFFSET AS offset
),

speed_cte AS (
    SELECT
        last_update_5minutes,
        camera_code,
        offset AS gol_id,
        SAFE_CAST(speed AS FLOAT64) AS speed
    FROM {{ source('sources', 'vehicle_speed') }},
    UNNEST(
        JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(speed_normal AS STRING)))
    ) AS speed WITH OFFSET AS offset
)

SELECT
    l.gol,
    s.speed,
    s.camera_code,
    s.last_update_5minutes,
    EXTRACT(DAY FROM s.last_update_5minutes) AS day,
    EXTRACT(MONTH FROM s.last_update_5minutes) AS month,
    EXTRACT(YEAR FROM s.last_update_5minutes) AS year
FROM label_cte AS l
JOIN speed_cte AS s
    ON l.gol_id = s.gol_id

{% else %}

WITH label_cte AS (
    SELECT DISTINCT
        generate_subscripts(label5min_data::STRING[], 1) AS gol_id,
        unnest(label5min_data::STRING[]) AS gol
    FROM {{ source('sources', 'vehicle_speed') }}
),

speed_cte AS (
    SELECT
        last_update_5minutes,
        camera_code,
        generate_subscripts(speed_normal::DOUBLE[], 1) AS gol_id,
        unnest(speed_normal::DOUBLE[]) AS speed
    FROM {{ source('sources', 'vehicle_speed') }}
)

SELECT
    l.gol,
    s.speed,
    s.camera_code,
    s.last_update_5minutes,
    EXTRACT(DAY FROM s.last_update_5minutes) AS day,
    EXTRACT(MONTH FROM s.last_update_5minutes) AS month,
    EXTRACT(YEAR  FROM s.last_update_5minutes) AS year
FROM label_cte l
JOIN speed_cte s
    ON l.gol_id = s.gol_id

{% endif %}
