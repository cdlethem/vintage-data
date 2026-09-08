{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            paper_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as paper_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as paper_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(published as timestamp with time zone) as published_at,
        cast(updated as timestamp with time zone) as updated_at,
        cast(title as varchar) as title,
        cast(abstract as varchar) as abstract,
        cast(authors as json) as authors,
        cast(primary_category as varchar) as primary_category,
        cast(categories as json) as categories,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_arxiv_new') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by paper_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    paper_observation_key,
    source_name,
    paper_id,
    observed_at,
    published_at,
    updated_at,
    title,
    abstract,
    authors,
    primary_category,
    categories,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
