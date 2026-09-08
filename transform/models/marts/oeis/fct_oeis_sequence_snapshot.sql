{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            sequence_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as oeis_sequence_snapshot_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as sequence_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(number as bigint) as sequence_number,
        cast(name as varchar) as sequence_name,
        cast(data as varchar) as sequence_data,
        cast(keywords as json) as keywords,
        cast(author as varchar) as author,
        cast(created as timestamp with time zone) as created_at,
        cast("offset" as varchar) as sequence_offset,
        cast(url as varchar) as sequence_url,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_oeis') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by oeis_sequence_snapshot_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    oeis_sequence_snapshot_key,
    source_name,
    sequence_id,
    observed_at,
    sequence_number,
    sequence_name,
    sequence_data,
    keywords,
    author,
    created_at,
    sequence_offset,
    sequence_url,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
