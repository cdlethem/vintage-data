{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source,
        cast(coalesce(localid, id) as varchar) as procedural_item_id,
        try_cast(fetched_at as timestamp with time zone) as observed_at,
        try_cast(layingdate as timestamp with time zone) as laid_at,
        cast(businessitemdate as json) as business_item_dates,
        cast(json_array_length(businessitemdate) as bigint) as business_item_date_count,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_uk_parliament_procedural_activity') }}
),

scheduled_dates as (
    select
        source_rows.*,
        (
            select min(coalesce(
                try_cast(json_extract_string(item.value, '$.BusinessItemDate') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$.Date') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$.BusinessItemDateTime') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$.DateTime') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$') as timestamp with time zone)
            ))
            from json_each(source_rows.business_item_dates) as item
        ) as first_scheduled_at,
        (
            select max(coalesce(
                try_cast(json_extract_string(item.value, '$.BusinessItemDate') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$.Date') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$.BusinessItemDateTime') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$.DateTime') as timestamp with time zone),
                try_cast(json_extract_string(item.value, '$') as timestamp with time zone)
            ))
            from json_each(source_rows.business_item_dates) as item
        ) as last_scheduled_at
    from source_rows
),

deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_relation := source_relation,
            procedural_item_id := procedural_item_id,
            observed_at := observed_at
        ))) as varchar) as uk_parliament_procedural_activity_key,
        *,
        row_number() over (
            partition by source_relation, procedural_item_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from scheduled_dates
)

select
    cast(uk_parliament_procedural_activity_key as varchar) as uk_parliament_procedural_activity_key,
    cast(source_relation as varchar) as source_relation,
    cast(source as varchar) as source,
    cast(procedural_item_id as varchar) as procedural_item_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(laid_at as timestamp with time zone) as laid_at,
    cast(business_item_dates as json) as business_item_dates,
    cast(business_item_date_count as bigint) as business_item_date_count,
    cast(first_scheduled_at as timestamp with time zone) as first_scheduled_at,
    cast(last_scheduled_at as timestamp with time zone) as last_scheduled_at,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
where _dedupe_rank = 1
