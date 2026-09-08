{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='open_library_change_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            change_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as open_library_change_key,
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(id as varchar) as change_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(kind as varchar) as change_kind,
        cast(timestamp as timestamp with time zone) as event_timestamp,
        cast(comment as varchar) as comment,
        cast(author as varchar) as author,
        cast(n_changes as bigint) as change_count,
        cast(changed_keys as json) as changed_keys,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_open_library') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
ranked_rows as (
    select
        source_rows.*,
        row_number() over (
            partition by source_name, change_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    open_library_change_key,
    source_relation,
    source_name,
    change_id,
    observed_at,
    change_kind,
    event_timestamp,
    comment,
    author,
    change_count,
    changed_keys,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from ranked_rows
where _dedupe_rank = 1
