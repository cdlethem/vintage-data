{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as pledge_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(date as date) as pledge_date,
        cast(state as varchar) as state,
        cast(pledges as bigint) as pledge_count,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_malaysia_organ_pledges') }}
),

deduplicated as (
    select
        md5(to_json(struct_pack(
            source_name := source_name,
            pledge_id := pledge_id,
            observed_at := observed_at
        ))) as malaysia_organ_pledge_observation_key,
        source_name,
        pledge_id,
        observed_at,
        pledge_date,
        state,
        pledge_count,
        publisher_updated_at,
        source_row_id,
        source_batch_id,
        source_file,
        source_file_row_number,
        source_date,
        extract_started_at,
        load_id,
        source_loaded_at,
        content_hash
    from source_rows
    qualify row_number() over (
        partition by source_name, pledge_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    cast(malaysia_organ_pledge_observation_key as varchar) as malaysia_organ_pledge_observation_key,
    cast(source_name as varchar) as source_name,
    cast(pledge_id as varchar) as pledge_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(pledge_date as date) as pledge_date,
    cast(state as varchar) as state,
    cast(pledge_count as bigint) as pledge_count,
    cast(publisher_updated_at as varchar) as publisher_updated_at,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
