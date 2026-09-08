{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='openactive_feed_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(md5(to_json(struct_pack(
            source := cast(source as varchar),
            record_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone),
            source_row_id := cast(_row_id as varchar)
        ))) as varchar) as openactive_feed_key,
        cast(_source as varchar) as raw_source,
        cast(source as varchar) as source,
        cast(id as varchar) as record_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(state as varchar) as state,
        cast(kind as varchar) as kind,
        try_cast(modified as bigint) as modified_sequence,
        cast(item_id as varchar) as item_id,
        cast(provider as varchar) as provider,
        cast(feed_type as varchar) as feed_type,
        cast(feed_url as varchar) as feed_url,
        cast(page_url as varchar) as page_url,
        cast(next as varchar) as next_url,
        json_extract_string(data, '$.@id') as slot_url,
        json_extract_string(data, '$.@type') as slot_type,
        json_extract_string(data, '$.identifier') as slot_identifier,
        json_extract_string(data, '$.facilityUse') as facility_use_url,
        json_extract_string(data, '$.duration') as duration,
        try_cast(json_extract_string(data, '$.startDate') as timestamp with time zone) as starts_at,
        try_cast(json_extract_string(data, '$.endDate') as timestamp with time zone) as ends_at,
        try_cast(json_extract_string(data, '$.maximumUses') as bigint) as maximum_uses,
        try_cast(json_extract_string(data, '$.remainingUses') as bigint) as remaining_uses,
        try_cast(json_extract_string(data, '$.offers[0].price') as decimal(18, 2)) as offer_price,
        json_extract_string(data, '$.offers[0].priceCurrency') as offer_currency,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_openactive_feeds') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
)
select
    cast(openactive_feed_key as varchar) as openactive_feed_key,
    cast(raw_source as varchar) as raw_source,
    cast(source as varchar) as source,
    cast(record_id as varchar) as record_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(state as varchar) as state,
    cast(kind as varchar) as kind,
    cast(modified_sequence as bigint) as modified_sequence,
    cast(item_id as varchar) as item_id,
    cast(provider as varchar) as provider,
    cast(feed_type as varchar) as feed_type,
    cast(feed_url as varchar) as feed_url,
    cast(page_url as varchar) as page_url,
    cast(next_url as varchar) as next_url,
    cast(slot_url as varchar) as slot_url,
    cast(slot_type as varchar) as slot_type,
    cast(slot_identifier as varchar) as slot_identifier,
    cast(facility_use_url as varchar) as facility_use_url,
    cast(duration as varchar) as duration,
    cast(starts_at as timestamp with time zone) as starts_at,
    cast(ends_at as timestamp with time zone) as ends_at,
    cast(maximum_uses as bigint) as maximum_uses,
    cast(remaining_uses as bigint) as remaining_uses,
    cast(offer_price as decimal(18, 2)) as offer_price,
    cast(offer_currency as varchar) as offer_currency,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_load_id as varchar) as _load_id,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from source_rows
