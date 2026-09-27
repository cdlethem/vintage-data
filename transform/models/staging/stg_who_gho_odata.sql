{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly', 'hourly']
) }}

select *
from {{ ref('base_who_gho_odata') }}
