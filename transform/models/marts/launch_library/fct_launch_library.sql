{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with normalized as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            launch_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as launch_event_key,
        cast(source as varchar) as source,
        cast(id as varchar) as launch_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(name as varchar) as name,
        cast(status as varchar) as status,
        cast(status_name as varchar) as status_name,
        cast(net as timestamp with time zone) as net,
        cast(net_precision as varchar) as net_precision,
        cast(window_start as timestamp with time zone) as window_start,
        cast(window_end as timestamp with time zone) as window_end,
        cast(last_updated as timestamp with time zone) as last_updated,
        cast(_dt as date) as source_date,
        cast(_batch_id as varchar) as batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_launch_library') }}
),

deduplicated as (
    select *
    from normalized
    qualify row_number() over (
        partition by source, launch_id, fetched_at
        order by source_loaded_at desc, source_file desc, file_row_num desc, content_hash desc
    ) = 1
)

select
    launch_event_key,
    source,
    launch_id,
    fetched_at,
    name,
    status,
    status_name,
    net,
    net_precision,
    window_start,
    window_end,
    last_updated,
    source_date,
    batch_id,
    load_id,
    source_file,
    file_row_num,
    source_loaded_at,
    content_hash
from deduplicated
