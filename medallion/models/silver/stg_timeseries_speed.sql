{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

WITH complete AS (
    SELECT *
    FROM {{ source('sources', 'all_timeseries_vehicle_speed') }}
    WHERE JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(hourly_label AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7normal_sm AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7normal_mp AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7normal_ks AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7normal_bb AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7normal_tb AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7normal_all AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7opposite_sm AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7opposite_mp AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7opposite_ks AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7opposite_bb AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7opposite_tb AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7opposite_all AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7both_sm AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7both_mp AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7both_ks AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7both_bb AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7both_tb AS STRING))) = 24
      AND JSON_ARRAY_LENGTH(PARSE_JSON(SAFE_CAST(datalast7both_all AS STRING))) = 24
)

SELECT
    c.camera_code,
    c.ingested_at,
    idx + 1 AS hour_id,
    hour_label,

    SAFE_CAST(last7_normal_sm AS FLOAT64) AS last7_normal_sm,
    SAFE_CAST(last7_normal_mp AS FLOAT64) AS last7_normal_mp,
    SAFE_CAST(last7_normal_ks AS FLOAT64) AS last7_normal_ks,
    SAFE_CAST(last7_normal_bb AS FLOAT64) AS last7_normal_bb,
    SAFE_CAST(last7_normal_tb AS FLOAT64) AS last7_normal_tb,
    SAFE_CAST(last7_normal_all AS FLOAT64) AS last7_normal_all,

    SAFE_CAST(last7_opposite_sm AS FLOAT64) AS last7_opposite_sm,
    SAFE_CAST(last7_opposite_mp AS FLOAT64) AS last7_opposite_mp,
    SAFE_CAST(last7_opposite_ks AS FLOAT64) AS last7_opposite_ks,
    SAFE_CAST(last7_opposite_bb AS FLOAT64) AS last7_opposite_bb,
    SAFE_CAST(last7_opposite_tb AS FLOAT64) AS last7_opposite_tb,
    SAFE_CAST(last7_opposite_all AS FLOAT64) AS last7_opposite_all,

    SAFE_CAST(last7_both_sm AS FLOAT64) AS last7_both_sm,
    SAFE_CAST(last7_both_mp AS FLOAT64) AS last7_both_mp,
    SAFE_CAST(last7_both_ks AS FLOAT64) AS last7_both_ks,
    SAFE_CAST(last7_both_bb AS FLOAT64) AS last7_both_bb,
    SAFE_CAST(last7_both_tb AS FLOAT64) AS last7_both_tb,
    SAFE_CAST(last7_both_all AS FLOAT64) AS last7_both_all,

    EXTRACT(DAY FROM c.ingested_at) AS day,
    EXTRACT(MONTH FROM c.ingested_at) AS month,
    EXTRACT(YEAR FROM c.ingested_at) AS year
FROM complete AS c
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.hourly_label AS STRING)))) AS hour_label WITH OFFSET AS idx
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7normal_sm AS STRING)))) AS last7_normal_sm WITH OFFSET AS i_nsm ON idx = i_nsm
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7normal_mp AS STRING)))) AS last7_normal_mp WITH OFFSET AS i_nmp ON idx = i_nmp
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7normal_ks AS STRING)))) AS last7_normal_ks WITH OFFSET AS i_nks ON idx = i_nks
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7normal_bb AS STRING)))) AS last7_normal_bb WITH OFFSET AS i_nbb ON idx = i_nbb
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7normal_tb AS STRING)))) AS last7_normal_tb WITH OFFSET AS i_ntb ON idx = i_ntb
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7normal_all AS STRING)))) AS last7_normal_all WITH OFFSET AS i_nall ON idx = i_nall
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7opposite_sm AS STRING)))) AS last7_opposite_sm WITH OFFSET AS i_osm ON idx = i_osm
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7opposite_mp AS STRING)))) AS last7_opposite_mp WITH OFFSET AS i_omp ON idx = i_omp
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7opposite_ks AS STRING)))) AS last7_opposite_ks WITH OFFSET AS i_oks ON idx = i_oks
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7opposite_bb AS STRING)))) AS last7_opposite_bb WITH OFFSET AS i_obb ON idx = i_obb
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7opposite_tb AS STRING)))) AS last7_opposite_tb WITH OFFSET AS i_otb ON idx = i_otb
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7opposite_all AS STRING)))) AS last7_opposite_all WITH OFFSET AS i_oall ON idx = i_oall
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7both_sm AS STRING)))) AS last7_both_sm WITH OFFSET AS i_bsm ON idx = i_bsm
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7both_mp AS STRING)))) AS last7_both_mp WITH OFFSET AS i_bmp ON idx = i_bmp
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7both_ks AS STRING)))) AS last7_both_ks WITH OFFSET AS i_bks ON idx = i_bks
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7both_bb AS STRING)))) AS last7_both_bb WITH OFFSET AS i_bbb ON idx = i_bbb
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7both_tb AS STRING)))) AS last7_both_tb WITH OFFSET AS i_btb ON idx = i_btb
INNER JOIN UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(SAFE_CAST(c.datalast7both_all AS STRING)))) AS last7_both_all WITH OFFSET AS i_ball ON idx = i_ball

{% else %}

WITH complete AS (
    SELECT *
    FROM {{ source('sources', 'all_timeseries_vehicle_speed') }}
    WHERE json_array_length(hourly_label)          = 24
      AND json_array_length(datalast7normal_sm)    = 24
      AND json_array_length(datalast7normal_mp)    = 24
      AND json_array_length(datalast7normal_ks)    = 24
      AND json_array_length(datalast7normal_bb)    = 24
      AND json_array_length(datalast7normal_tb)    = 24
      AND json_array_length(datalast7normal_all)   = 24
      AND json_array_length(datalast7opposite_sm)  = 24
      AND json_array_length(datalast7opposite_mp)  = 24
      AND json_array_length(datalast7opposite_ks)  = 24
      AND json_array_length(datalast7opposite_bb)  = 24
      AND json_array_length(datalast7opposite_tb)  = 24
      AND json_array_length(datalast7opposite_all) = 24
      AND json_array_length(datalast7both_sm)      = 24
      AND json_array_length(datalast7both_mp)      = 24
      AND json_array_length(datalast7both_ks)      = 24
      AND json_array_length(datalast7both_bb)      = 24
      AND json_array_length(datalast7both_tb)      = 24
      AND json_array_length(datalast7both_all)     = 24
)

SELECT
    camera_code,
    ingested_at,
    generate_subscripts(hourly_label::STRING[], 1) AS hour_id,
    unnest(hourly_label::STRING[])                 AS hour_label,

    unnest(datalast7normal_sm::DOUBLE[])           AS last7_normal_sm,
    unnest(datalast7normal_mp::DOUBLE[])           AS last7_normal_mp,
    unnest(datalast7normal_ks::DOUBLE[])           AS last7_normal_ks,
    unnest(datalast7normal_bb::DOUBLE[])           AS last7_normal_bb,
    unnest(datalast7normal_tb::DOUBLE[])           AS last7_normal_tb,
    unnest(datalast7normal_all::DOUBLE[])          AS last7_normal_all,

    unnest(datalast7opposite_sm::DOUBLE[])         AS last7_opposite_sm,
    unnest(datalast7opposite_mp::DOUBLE[])         AS last7_opposite_mp,
    unnest(datalast7opposite_ks::DOUBLE[])         AS last7_opposite_ks,
    unnest(datalast7opposite_bb::DOUBLE[])         AS last7_opposite_bb,
    unnest(datalast7opposite_tb::DOUBLE[])         AS last7_opposite_tb,
    unnest(datalast7opposite_all::DOUBLE[])        AS last7_opposite_all,

    unnest(datalast7both_sm::DOUBLE[])             AS last7_both_sm,
    unnest(datalast7both_mp::DOUBLE[])             AS last7_both_mp,
    unnest(datalast7both_ks::DOUBLE[])             AS last7_both_ks,
    unnest(datalast7both_bb::DOUBLE[])             AS last7_both_bb,
    unnest(datalast7both_tb::DOUBLE[])             AS last7_both_tb,
    unnest(datalast7both_all::DOUBLE[])            AS last7_both_all,

    EXTRACT(DAY   FROM ingested_at) AS day,
    EXTRACT(MONTH FROM ingested_at) AS month,
    EXTRACT(YEAR  FROM ingested_at) AS year
FROM complete

{% endif %}
