{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as registration_observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(published_at as timestamp with time zone) as published_at,
        cast(applications as bigint) as application_count,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_register_to_vote') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        registration_observation_id := registration_observation_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as register_to_vote_key,
        source,
        registration_observation_id,
        observed_at,
        published_at,
        application_count,
        _batch_id,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by register_to_vote_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(register_to_vote_key as varchar) as register_to_vote_key,
    cast(source as varchar) as source,
    cast(registration_observation_id as varchar) as registration_observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(published_at as timestamp with time zone) as published_at,
    cast(application_count as bigint) as application_count,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
