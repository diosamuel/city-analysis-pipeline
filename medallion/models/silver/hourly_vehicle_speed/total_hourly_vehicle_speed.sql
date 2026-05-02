{{ config(materialized='view') }}

select camera_code,
    ingested_at,
    COLUMNS('^total.*')
from {{ source('bronze', 'hourly_vehicle_speed') }}
