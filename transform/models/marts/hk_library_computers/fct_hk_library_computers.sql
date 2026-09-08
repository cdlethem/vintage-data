{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(md5(to_json(struct_pack(
            source := cast(source as varchar),
            computer_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as computer_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as computer_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(record_type as varchar) as record_type,
        cast(library_code as varchar) as library_code,
        cast(library_name as varchar) as library_name,
        cast(district as varchar) as district,
        cast(is_open as boolean) as is_open,
        cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
        cast(session_start as timestamp with time zone) as session_start,
        cast(session_end as timestamp with time zone) as session_end,
        cast(group_id as varchar) as group_id,
        cast(group_name as varchar) as group_name,
        cast(available_workstations as bigint) as available_workstations,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_hk_library_computers') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by computer_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    computer_observation_key,
    source,
    computer_id,
    observed_at,
    record_type,
    library_code,
    library_name,
    district,
    is_open,
    publisher_updated_at,
    session_start,
    session_end,
    group_id,
    group_name,
    available_workstations,
    source_date,
    extract_started_at,
    source_batch_id,
    load_id,
    source_file,
    source_file_row_number,
    source_loaded_at,
    content_hash
from deduplicated
