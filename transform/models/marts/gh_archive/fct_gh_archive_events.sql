{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            archive_source := cast(source as varchar),
            event_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as gh_archive_event_key,
        cast(source as varchar) as archive_source,
        cast(id as varchar) as event_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(type as varchar) as event_type,
        cast(created_at as timestamp with time zone) as created_at,
        cast(actor_login as varchar) as actor_login,
        cast(repo_name as varchar) as repository_name,
        cast(public as boolean) as is_public,
        cast(payload as json) as event_payload,
        cast(json_extract_string(payload, '$.action') as varchar) as payload_action,
        cast(json_extract_string(payload, '$.ref') as varchar) as payload_ref,
        cast(json_extract_string(payload, '$.ref_type') as varchar) as payload_ref_type,
        try_cast(json_extract_string(payload, '$.number') as bigint) as payload_number,
        try_cast(json_extract_string(payload, '$.push_id') as bigint) as payload_push_id,
        try_cast(json_extract_string(payload, '$.repository_id') as bigint) as payload_repository_id,
        cast(json_extract_string(payload, '$.head') as varchar) as payload_head_sha,
        cast(json_extract_string(payload, '$.before') as varchar) as payload_before_sha,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_gh_archive') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by archive_source, event_id, fetched_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    gh_archive_event_key,
    archive_source,
    event_id,
    fetched_at,
    event_type,
    created_at,
    actor_login,
    repository_name,
    is_public,
    event_payload,
    payload_action,
    payload_ref,
    payload_ref_type,
    payload_number,
    payload_push_id,
    payload_repository_id,
    payload_head_sha,
    payload_before_sha,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
