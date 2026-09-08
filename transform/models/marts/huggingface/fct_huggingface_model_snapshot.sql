{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with typed_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as model_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(author as varchar) as author,
        cast(created_at as timestamp with time zone) as created_at,
        cast(likes as bigint) as like_count,
        cast(downloads as bigint) as download_count,
        cast(private as boolean) as is_private,
        cast(tags as json) as tags,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_huggingface') }}
),
keyed_rows as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        model_id := model_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as huggingface_model_snapshot_key,
        *,
        row_number() over (
            partition by source_name, model_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from typed_rows
)

select
    huggingface_model_snapshot_key,
    source_name,
    model_id,
    observed_at,
    author,
    created_at,
    like_count,
    download_count,
    is_private,
    tags,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from keyed_rows
where _dedupe_rank = 1
