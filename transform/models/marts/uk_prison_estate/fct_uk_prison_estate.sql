{{ config(
    materialized='table',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(report_date as date) as report_date,
        cast(period as varchar) as period,
        cast(period_date as date) as period_date,
        cast(metric as varchar) as metric,
        cast(population_group as varchar) as population_group,
        cast(value as varchar) as measure_value_text,
        try_cast(nullif(trim(value), '') as bigint) as measure_value,
        cast(attachment_title as varchar) as attachment_title,
        cast(attachment_url as varchar) as attachment_url,
        cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_uk_prison_estate') }}
),

deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            observation_id := observation_id,
            observed_at := observed_at
        ))) as varchar) as prison_estate_observation_key,
        source,
        observation_id,
        observed_at,
        report_date,
        period,
        period_date,
        metric,
        population_group,
        measure_value,
        measure_value_text,
        attachment_title,
        attachment_url,
        publisher_updated_at,
        _row_id,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from source_rows
    qualify row_number() over (
        partition by source, observation_id, observed_at
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(prison_estate_observation_key as varchar) as prison_estate_observation_key,
    cast(source as varchar) as source,
    cast(observation_id as varchar) as observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(report_date as date) as report_date,
    cast(period as varchar) as period,
    cast(period_date as date) as period_date,
    cast(metric as varchar) as metric,
    cast(population_group as varchar) as population_group,
    cast(measure_value as bigint) as measure_value,
    cast(measure_value_text as varchar) as measure_value_text,
    cast(attachment_title as varchar) as attachment_title,
    cast(attachment_url as varchar) as attachment_url,
    cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
    cast(_row_id as varchar) as _row_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
