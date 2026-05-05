{{ config(materialized='table') }}

{% if target.type == 'bigquery' %}

SELECT
    camera_code,
    ingested_at,
    totalvolume_sm,
    totalvolume_mp,
    totalvolume_ks,
    totalvolume_bb,
    totalvolume_tb
FROM {{ source('sources', 'hourly_vehicle_speed') }}

{% else %}

SELECT camera_code,
    ingested_at,
    COLUMNS('^total.*')
FROM {{ source('sources', 'hourly_vehicle_speed') }}

{% endif %}
