{{ config(
    materialized='table',
    on_schema_change='fail'
) }}

select *
from {{ ref('base_who_gho_odata') }}
