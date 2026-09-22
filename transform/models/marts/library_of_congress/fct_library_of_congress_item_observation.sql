{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            item_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as library_of_congress_item_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as item_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as title,
        cast(date as varchar) as item_date,
        cast(dates as json) as dates,
        cast(contributors as json) as contributors,
        cast(subjects as json) as subjects,
        cast(digitized as boolean) as digitized,
        cast(online_formats as json) as online_formats,
        cast(mime_types as json) as mime_types,
        cast(canonical_url as varchar) as canonical_url,
        cast(resources as json) as resources,
        cast(resource_links as json) as resource_links,
        cast(raw as json) as raw_item,
        cast(_payload as json) as source_payload,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_library_of_congress') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by library_of_congress_item_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    library_of_congress_item_observation_key,
    source,
    item_id,
    observed_at,
    title,
    item_date,
    dates,
    contributors,
    subjects,
    digitized,
    online_formats,
    mime_types,
    canonical_url,
    resources,
    resource_links,
    raw_item,
    source_payload,
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
