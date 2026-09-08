{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='station_status_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with selected as (
    select *
    from {{ ref('stg_gbfs_station_status') }}
    {% if is_incremental() %}
    where source_loaded_at >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select *
    from selected
    qualify row_number() over (
        partition by station_status_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    station_status_key,
    network,
    station_id,
    observed_at,
    station_reported_at,
    num_bikes_available,
    num_docks_available,
    num_ebikes_available,
    num_bikes_disabled,
    num_docks_disabled,
    is_installed,
    is_renting,
    is_returning,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
