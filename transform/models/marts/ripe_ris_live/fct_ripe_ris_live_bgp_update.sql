{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ripe_ris_live_bgp_update_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as update_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(type as varchar) as update_type,
        case
            when bgp_timestamp > 0
            then cast(to_timestamp(bgp_timestamp) as timestamp with time zone)
        end as bgp_observed_at,
        cast(host as varchar) as collector_host,
        cast(peer as varchar) as peer_address,
        cast(peer_asn as varchar) as peer_asn,
        cast(path as json) as as_path,
        cast(origin as varchar) as origin,
        cast(announcements as json) as announcements,
        cast(withdrawals as json) as withdrawals,
        cast(json_array_length(path) as bigint) as path_hop_count,
        cast(json_array_length(announcements) as bigint) as announcement_count,
        cast(json_array_length(withdrawals) as bigint) as withdrawal_count,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_ripe_ris_live') }}
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
            update_id := update_id,
            observed_at := observed_at
        ))) as varchar) as ripe_ris_live_bgp_update_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by ripe_ris_live_bgp_update_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    ripe_ris_live_bgp_update_key,
    source,
    update_id,
    observed_at,
    update_type,
    bgp_observed_at,
    collector_host,
    peer_address,
    peer_asn,
    as_path,
    origin,
    announcements,
    withdrawals,
    path_hop_count,
    announcement_count,
    withdrawal_count,
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
