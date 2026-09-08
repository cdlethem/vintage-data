{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='wspr_live_spot_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as spot_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(ts as timestamp with time zone) as signal_at,
        cast(band as bigint) as band,
        cast(frequency_hz as bigint) as frequency_hz,
        cast(tx_call as varchar) as transmit_call,
        cast(tx_grid as varchar) as transmit_grid,
        cast(tx_lat as double) as transmit_latitude,
        cast(tx_lon as double) as transmit_longitude,
        cast(rx_call as varchar) as receive_call,
        cast(rx_grid as varchar) as receive_grid,
        cast(rx_lat as double) as receive_latitude,
        cast(rx_lon as double) as receive_longitude,
        cast(distance_km as bigint) as distance_km,
        cast(azimuth_deg as bigint) as azimuth_degrees,
        cast(snr_db as bigint) as snr_db,
        cast(tx_power_dbm as bigint) as transmit_power_dbm,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_wspr_live') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            spot_id := spot_id,
            observed_at := observed_at
        ))) as varchar) as wspr_live_spot_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by wspr_live_spot_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    wspr_live_spot_key,
    source,
    spot_id,
    observed_at,
    signal_at,
    band,
    frequency_hz,
    transmit_call,
    transmit_grid,
    transmit_latitude,
    transmit_longitude,
    receive_call,
    receive_grid,
    receive_latitude,
    receive_longitude,
    distance_km,
    azimuth_degrees,
    snr_db,
    transmit_power_dbm,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
