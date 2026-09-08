{{ config(
    materialized='table',
    tags=['daily']
) }}

with normalized as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            turnaround_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as turnaround_key,
        cast(source as varchar) as source,
        cast(id as varchar) as turnaround_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(lab as varchar) as lab,
        cast(report_type as varchar) as report_type,
        cast(processing_time as varchar) as processing_time,
        cast(processing_days as bigint) as processing_days,
        cast(_dt as date) as source_date,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as file_row_num,
        cast(_batch_id as varchar) as batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_nc_lab_turnaround') }}
),

deduplicated as (
    select *
    from normalized
    qualify row_number() over (
        partition by turnaround_key
        order by source_loaded_at desc, source_file desc, file_row_num desc, content_hash desc
    ) = 1
)

select
    turnaround_key,
    source,
    turnaround_id,
    fetched_at,
    lab,
    report_type,
    processing_time,
    processing_days,
    source_date,
    source_file,
    file_row_num,
    batch_id,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
