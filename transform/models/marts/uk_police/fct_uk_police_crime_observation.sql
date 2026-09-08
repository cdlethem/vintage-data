{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='uk_police_crime_observation_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as crime_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        try_cast(nullif(trim(month), '') || '-01' as date) as crime_month,
        cast(category as varchar) as category,
        try_cast(nullif(trim(lat), '') as double) as latitude,
        try_cast(nullif(trim(lng), '') as double) as longitude,
        cast(street as varchar) as street,
        cast(location_type as varchar) as location_type,
        cast(outcome as varchar) as outcome,
        try_cast(nullif(trim(outcome_date), '') || '-01' as date) as outcome_month,
        cast(_row_id as varchar) as _row_id,
        cast(_source as varchar) as _source,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_uk_police') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
),

keyed_rows as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            crime_id := crime_id,
            observed_at := observed_at
        ))) as varchar) as uk_police_crime_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed_rows
    qualify row_number() over (
        partition by uk_police_crime_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(uk_police_crime_observation_key as varchar) as uk_police_crime_observation_key,
    cast(source as varchar) as source,
    cast(crime_id as varchar) as crime_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(crime_month as date) as crime_month,
    cast(category as varchar) as category,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(street as varchar) as street,
    cast(location_type as varchar) as location_type,
    cast(outcome as varchar) as outcome,
    cast(outcome_month as date) as outcome_month,
    cast(_row_id as varchar) as _row_id,
    cast(_source as varchar) as _source,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
