{{ config(
    materialized='table',
    tags=['hourly']
) }}

with normalized as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            processing_type := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as processing_time_key,
        cast(source as varchar) as source,
        cast(id as varchar) as processing_type,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(data_type as varchar) as data_type,
        cast(sla as varchar) as sla,
        cast(oldest_item as varchar) as oldest_item,
        try_strptime(oldest_item, '%d %B %Y')::date as oldest_item_date,
        cast(unprocessed_items as bigint) as unprocessed_items,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        try_strptime(publisher_updated_at, '%d %B %Y')::date as publisher_updated_date,
        date_diff(
            'day',
            try_strptime(oldest_item, '%d %B %Y')::date,
            cast(fetched_at as date)
        ) as queue_age_days,
        cast(_row_id as varchar) as source_row_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as file_row_num,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_batch_id as varchar) as batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_imdb_processing_times') }}
),

deduplicated as (
    select *
    from normalized
    qualify row_number() over (
        partition by processing_time_key
        order by source_loaded_at desc, source_file desc, file_row_num desc, content_hash desc
    ) = 1
)

select
    processing_time_key,
    source,
    processing_type,
    observed_at,
    data_type,
    sla,
    oldest_item,
    oldest_item_date,
    unprocessed_items,
    publisher_updated_at,
    publisher_updated_date,
    queue_age_days,
    source_row_id,
    source_file,
    file_row_num,
    source_date,
    extract_started_at,
    batch_id,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
