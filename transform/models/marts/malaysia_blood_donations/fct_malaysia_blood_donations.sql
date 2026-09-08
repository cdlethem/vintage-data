{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(date as date) as donation_date,
        cast(state as varchar) as state,
        cast(blood_type as varchar) as blood_type,
        cast(donations as bigint) as donation_count,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_malaysia_blood_donations') }}
),

ranked as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            observation_id := observation_id,
            fetched_at := fetched_at
        ))) as varchar) as malaysia_blood_donation_observation_key,
        source,
        observation_id,
        fetched_at,
        donation_date,
        state,
        blood_type,
        donation_count,
        publisher_updated_at,
        _row_id,
        _batch_id,
        _source_file,
        _file_row_num,
        _dt,
        _extract_started_at,
        _load_id,
        source_loaded_at,
        _content_hash,
        row_number() over (
            partition by source, observation_id, fetched_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from source_rows
)

select
    malaysia_blood_donation_observation_key,
    source,
    observation_id,
    fetched_at,
    donation_date,
    state,
    blood_type,
    donation_count,
    publisher_updated_at,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from ranked
where observation_rank = 1
