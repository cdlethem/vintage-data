{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            structure_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as structure_snapshot_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as structure_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(initial_release_date as date) as initial_release_date,
        cast(deposit_date as date) as deposit_date,
        cast(revision_date as date) as revision_date,
        cast(method as varchar) as experimental_method,
        cast(title as varchar) as title,
        cast(keywords as varchar) as keywords,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_rcsb_pdb') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by structure_snapshot_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    structure_snapshot_key,
    source_name,
    structure_id,
    observed_at,
    initial_release_date,
    deposit_date,
    revision_date,
    experimental_method,
    title,
    keywords,
    _batch_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
