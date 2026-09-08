{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='station_status_daily_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with observations as (
    select
        *,
        cast(timezone('UTC', observed_at) as date) as observation_date
    from {{ ref('fct_bike_station_status') }}
    {% if is_incremental() %}
    where cast(timezone('UTC', observed_at) as date) >= (
        select max(cast(timezone('UTC', observed_at) as date)) - interval 1 day
        from {{ ref('fct_bike_station_status') }}
    )
    {% endif %}
),

aggregated as (
    select
        network,
        station_id,
        observation_date,
        cast(count(*) as bigint) as observation_count,
        min(observed_at) as first_observed_at,
        max(observed_at) as last_observed_at,
        min(num_bikes_available) as min_bikes_available,
        max(num_bikes_available) as max_bikes_available,
        avg(num_bikes_available) as avg_bikes_available,
        min(num_docks_available) as min_docks_available,
        max(num_docks_available) as max_docks_available,
        avg(num_docks_available) as avg_docks_available,
        max(source_loaded_at) as source_loaded_at
    from observations
    group by network, station_id, observation_date
)

select
    md5(to_json(struct_pack(
        network := network,
        station_id := station_id,
        observation_date := observation_date
    ))) as station_status_daily_key,
    network,
    station_id,
    observation_date,
    observation_count,
    first_observed_at,
    last_observed_at,
    min_bikes_available,
    max_bikes_available,
    avg_bikes_available,
    min_docks_available,
    max_docks_available,
    avg_docks_available,
    source_loaded_at
from aggregated
