{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='opensky_flight_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(icao24 as varchar) as aircraft_id,
        cast(id as varchar) as source_observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(snapshot_time as bigint) as snapshot_time,
        cast(callsign as varchar) as callsign,
        cast(origin_country as varchar) as origin_country,
        cast(longitude as double) as longitude,
        cast(latitude as double) as latitude,
        cast(baro_altitude_m as double) as baro_altitude_m,
        cast(on_ground as boolean) as on_ground,
        cast(velocity_m_s as double) as velocity_m_s,
        cast(true_track_deg as double) as true_track_deg,
        cast(vertical_rate_m_s as double) as vertical_rate_m_s,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_opensky') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            aircraft_id := aircraft_id,
            observed_at := observed_at
        ))) as varchar) as opensky_flight_observation_key,
        *
    from normalized
),
ranked as (
    select
        *,
        row_number() over (
            partition by opensky_flight_observation_key
            order by source_loaded_at desc, source_file desc,
                     source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from keyed
)

select
    cast(opensky_flight_observation_key as varchar) as opensky_flight_observation_key,
    cast(source as varchar) as source,
    cast(aircraft_id as varchar) as aircraft_id,
    cast(source_observation_id as varchar) as source_observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(snapshot_time as bigint) as snapshot_time,
    cast(callsign as varchar) as callsign,
    cast(origin_country as varchar) as origin_country,
    cast(longitude as double) as longitude,
    cast(latitude as double) as latitude,
    cast(baro_altitude_m as double) as baro_altitude_m,
    cast(on_ground as boolean) as on_ground,
    cast(velocity_m_s as double) as velocity_m_s,
    cast(true_track_deg as double) as true_track_deg,
    cast(vertical_rate_m_s as double) as vertical_rate_m_s,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from ranked
where _dedupe_rank = 1
