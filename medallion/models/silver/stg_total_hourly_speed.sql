{{ config(materialized='table') }}

select camera_code,
    ingested_at,
    COLUMNS('^total.*')
from {{ source('sources', 'hourly_vehicle_speed') }}
