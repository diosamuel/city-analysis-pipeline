{{ config(materialized='table') }}

-- Vehicle-class codes (from KemenPUPR feed):
--   sm  = sepeda motor        (motorcycle)
--   mp  = mobil penumpang     (passenger car)
--   ks  = kendaraan sedang    (medium vehicle)
--   bb  = bus besar           (large bus)
--   tb  = truk besar          (large truck)
--   all = aggregate of all classes
-- Direction: normal / opposite / both.

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
