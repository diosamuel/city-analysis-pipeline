{{
  config(
    materialized='table'
  )
}}

/*
  Gold dimension dim_date (schema/dwh.sql):
  date_sk = YYYYMMDD integer; one row per distinct calendar_date seen in silver staging.
*/

WITH staging_dates AS (
    SELECT CAST(observed_at AS DATE) AS calendar_date
    FROM {{ ref('stg_air_quality') }}
    WHERE observed_at IS NOT NULL

    UNION

    SELECT CAST(last_update_5minutes AS DATE) AS calendar_date
    FROM {{ ref('stg_speed') }}
    WHERE last_update_5minutes IS NOT NULL

    UNION

    SELECT CAST(forecast_datetime_utc AS DATE) AS calendar_date
    FROM {{ ref('stg_weather') }}
    WHERE forecast_datetime_utc IS NOT NULL

    UNION

    SELECT CAST(ingested_at AS DATE) AS calendar_date
    FROM {{ ref('stg_hourly_speed') }}
    WHERE ingested_at IS NOT NULL

    UNION

    SELECT CAST(ingested_at AS DATE) AS calendar_date
    FROM {{ ref('stg_total_hourly_speed') }}
    WHERE ingested_at IS NOT NULL

    UNION

    SELECT CAST(ingested_at AS DATE) AS calendar_date
    FROM {{ ref('stg_timeseries_speed') }}
    WHERE ingested_at IS NOT NULL
),

distinct_dates AS (
    SELECT DISTINCT calendar_date
    FROM staging_dates
    WHERE calendar_date IS NOT NULL
)

{% if target.type == 'bigquery' %}

SELECT
    CAST(
        EXTRACT(YEAR FROM calendar_date) * 10000
        + EXTRACT(MONTH FROM calendar_date) * 100
        + EXTRACT(DAY FROM calendar_date) AS INT64
    ) AS date_sk,
    calendar_date,
    CAST(EXTRACT(DAY FROM calendar_date) AS INT64) AS day,
    CAST(EXTRACT(MONTH FROM calendar_date) AS INT64) AS month,
    CAST(EXTRACT(YEAR FROM calendar_date) AS INT64) AS year
FROM distinct_dates

{% else %}

SELECT
    CAST(
        year(calendar_date) * 10000
        + month(calendar_date) * 100
        + day(calendar_date) AS INTEGER
    ) AS date_sk,
    calendar_date,
    CAST(day(calendar_date) AS INTEGER) AS day,
    CAST(month(calendar_date) AS INTEGER) AS month,
    CAST(year(calendar_date) AS INTEGER) AS year
FROM distinct_dates
ORDER BY calendar_date

{% endif %}
