{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='wikimedia_change_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(id as varchar) as change_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(ts as timestamp with time zone) as event_at,
        cast(type as varchar) as change_type,
        cast(title as varchar) as title,
        cast("user" as varchar) as editor,
        cast(bot as boolean) as is_bot,
        cast(minor as boolean) as is_minor,
        cast(comment as varchar) as comment,
        cast(bytes_delta as bigint) as bytes_delta,
        cast(tags as json) as tags,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_wikimedia_changes') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
ranked_rows as (
    select
        cast(md5(to_json(struct_pack(
            source_name := source_name,
            change_id := change_id,
            observed_at := observed_at
        ))) as varchar) as wikimedia_change_observation_key,
        source_rows.*,
        row_number() over (
            partition by source_name, change_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(wikimedia_change_observation_key as varchar) as wikimedia_change_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source_name as varchar) as source_name,
    cast(change_id as varchar) as change_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(event_at as timestamp with time zone) as event_at,
    cast(change_type as varchar) as change_type,
    cast(title as varchar) as title,
    cast(editor as varchar) as editor,
    cast(is_bot as boolean) as is_bot,
    cast(is_minor as boolean) as is_minor,
    cast(comment as varchar) as comment,
    cast(bytes_delta as bigint) as bytes_delta,
    cast(tags as json) as tags,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from ranked_rows
where _dedupe_rank = 1
