{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='carbon_intensity_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as interval_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(forecast as bigint) as forecast_intensity,
        cast(actual as bigint) as actual_intensity,
        cast("index" as varchar) as intensity_index,
        cast(error as bigint) as error_code,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_carbon_intensity') }}
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

keyed as (
    select
        md5(to_json(struct_pack(
            source := source,
            interval_id := interval_id,
            observed_at := observed_at
        ))) as carbon_intensity_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by carbon_intensity_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    carbon_intensity_key,
    source,
    interval_id,
    observed_at,
    forecast_intensity,
    actual_intensity,
    intensity_index,
    error_code,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
