{{
  config(
    materialized='table'
  )
}}

/*
  Gold fact fact_cctv_daily_snapshot (schema/dwh.sql):
  grain: one row per (camera_sk, date_sk).
  Keys: gold dim_camera, dim_date.
  Measures: rollups from silver speed + hourly total volumes.
*/

{% if target.type == 'bigquery' %}

WITH speed_daily AS (
    SELECT
        TRIM(camera_code) AS camera_code,
        CAST(last_update_5minutes AS DATE) AS calendar_date,
        AVG(CAST(speed AS FLOAT64)) AS avg_speed_kmh,
        MAX(CAST(speed AS FLOAT64)) AS max_speed_kmh,
        MIN(CAST(speed AS FLOAT64)) AS min_speed_kmh,
        COUNT(*) AS obs_speed_sample_count,
        MAX(last_update_5minutes) AS ingest_speed_ts
    FROM {{ ref('stg_speed') }}
    WHERE camera_code IS NOT NULL
      AND TRIM(camera_code) <> ''
      AND last_update_5minutes IS NOT NULL
    GROUP BY TRIM(camera_code), CAST(last_update_5minutes AS DATE)
),

vol_daily AS (
    SELECT
        TRIM(camera_code) AS camera_code,
        CAST(ingested_at AS DATE) AS calendar_date,
        SUM(CAST(totalvolume_sm AS FLOAT64)) AS total_volume_sm,
        SUM(CAST(totalvolume_mp AS FLOAT64)) AS total_volume_mp,
        SUM(CAST(totalvolume_ks AS FLOAT64)) AS total_volume_ks,
        SUM(CAST(totalvolume_bb AS FLOAT64)) AS total_volume_bb,
        SUM(CAST(totalvolume_tb AS FLOAT64)) AS total_volume_tb,
        MAX(ingested_at) AS ingest_vol_ts
    FROM {{ ref('stg_total_hourly_speed') }}
    WHERE camera_code IS NOT NULL
      AND TRIM(camera_code) <> ''
      AND ingested_at IS NOT NULL
    GROUP BY TRIM(camera_code), CAST(ingested_at AS DATE)
),

daily_grain AS (
    SELECT camera_code, calendar_date FROM speed_daily
    UNION DISTINCT
    SELECT camera_code, calendar_date FROM vol_daily
),

base AS (
    SELECT
        g.camera_code,
        g.calendar_date,
        sd.avg_speed_kmh,
        sd.max_speed_kmh,
        sd.min_speed_kmh,
        sd.obs_speed_sample_count,
        vd.total_volume_sm,
        vd.total_volume_mp,
        vd.total_volume_ks,
        vd.total_volume_bb,
        vd.total_volume_tb,
        CASE
            WHEN sd.ingest_speed_ts IS NOT NULL AND vd.ingest_vol_ts IS NOT NULL
                THEN GREATEST(sd.ingest_speed_ts, vd.ingest_vol_ts)
            ELSE COALESCE(sd.ingest_speed_ts, vd.ingest_vol_ts)
        END AS ingest_batch_ts
    FROM daily_grain AS g
    LEFT JOIN speed_daily AS sd
        ON g.camera_code = sd.camera_code AND g.calendar_date = sd.calendar_date
    LEFT JOIN vol_daily AS vd
        ON g.camera_code = vd.camera_code AND g.calendar_date = vd.calendar_date
)

SELECT
    ROW_NUMBER() OVER (ORDER BY dc.camera_sk, dd.date_sk) AS fact_sk,
    dc.camera_sk,
    dc.adm4_sk,
    dd.date_sk,

    b.avg_speed_kmh,
    b.max_speed_kmh,
    b.min_speed_kmh,
    b.obs_speed_sample_count,

    b.total_volume_sm,
    b.total_volume_mp,
    b.total_volume_ks,
    b.total_volume_bb,
    b.total_volume_tb,

    CAST(NULL AS STRING) AS speed_timeseries_note,

    CAST(b.ingest_batch_ts AS TIMESTAMP) AS ingest_batch_ts,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM base AS b
INNER JOIN {{ ref('dim_camera') }} AS dc ON b.camera_code = dc.camera_code
INNER JOIN {{ ref('dim_date') }} AS dd ON b.calendar_date = dd.calendar_date

{% else %}

WITH speed_daily AS (
    SELECT
        TRIM(camera_code)::VARCHAR AS camera_code,
        CAST(last_update_5minutes AS DATE) AS calendar_date,
        AVG(speed)::DOUBLE AS avg_speed_kmh,
        MAX(speed)::DOUBLE AS max_speed_kmh,
        MIN(speed)::DOUBLE AS min_speed_kmh,
        COUNT(*)::BIGINT AS obs_speed_sample_count,
        MAX(last_update_5minutes)::TIMESTAMP AS ingest_speed_ts
    FROM {{ ref('stg_speed') }}
    WHERE camera_code IS NOT NULL
      AND TRIM(camera_code) <> ''
      AND last_update_5minutes IS NOT NULL
    GROUP BY TRIM(camera_code), CAST(last_update_5minutes AS DATE)
),

vol_daily AS (
    SELECT
        TRIM(camera_code)::VARCHAR AS camera_code,
        CAST(ingested_at AS DATE) AS calendar_date,
        SUM(totalvolume_sm::DOUBLE) AS total_volume_sm,
        SUM(totalvolume_mp::DOUBLE) AS total_volume_mp,
        SUM(totalvolume_ks::DOUBLE) AS total_volume_ks,
        SUM(totalvolume_bb::DOUBLE) AS total_volume_bb,
        SUM(totalvolume_tb::DOUBLE) AS total_volume_tb,
        MAX(ingested_at)::TIMESTAMP AS ingest_vol_ts
    FROM {{ ref('stg_total_hourly_speed') }}
    WHERE camera_code IS NOT NULL
      AND TRIM(camera_code) <> ''
      AND ingested_at IS NOT NULL
    GROUP BY TRIM(camera_code), CAST(ingested_at AS DATE)
),

daily_grain AS (
    SELECT camera_code, calendar_date FROM speed_daily
    UNION DISTINCT
    SELECT camera_code, calendar_date FROM vol_daily
),

base AS (
    SELECT
        g.camera_code,
        g.calendar_date,
        sd.avg_speed_kmh,
        sd.max_speed_kmh,
        sd.min_speed_kmh,
        sd.obs_speed_sample_count,
        vd.total_volume_sm,
        vd.total_volume_mp,
        vd.total_volume_ks,
        vd.total_volume_bb,
        vd.total_volume_tb,
        CASE
            WHEN sd.ingest_speed_ts IS NOT NULL AND vd.ingest_vol_ts IS NOT NULL
                THEN greatest(sd.ingest_speed_ts, vd.ingest_vol_ts)
            ELSE coalesce(sd.ingest_speed_ts, vd.ingest_vol_ts)
        END AS ingest_batch_ts
    FROM daily_grain AS g
    LEFT JOIN speed_daily AS sd
        ON g.camera_code = sd.camera_code AND g.calendar_date = sd.calendar_date
    LEFT JOIN vol_daily AS vd
        ON g.camera_code = vd.camera_code AND g.calendar_date = vd.calendar_date
)

SELECT
    ROW_NUMBER() OVER (ORDER BY dc.camera_sk, dd.date_sk)::BIGINT AS fact_sk,
    dc.camera_sk,
    dc.adm4_sk,
    dd.date_sk,

    b.avg_speed_kmh,
    b.max_speed_kmh,
    b.min_speed_kmh,
    b.obs_speed_sample_count,

    b.total_volume_sm,
    b.total_volume_mp,
    b.total_volume_ks,
    b.total_volume_bb,
    b.total_volume_tb,

    CAST(NULL AS VARCHAR) AS speed_timeseries_note,

    b.ingest_batch_ts::TIMESTAMP AS ingest_batch_ts,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM base AS b
INNER JOIN {{ ref('dim_camera') }} AS dc ON b.camera_code = dc.camera_code
INNER JOIN {{ ref('dim_date') }} AS dd ON b.calendar_date = dd.calendar_date

{% endif %}
