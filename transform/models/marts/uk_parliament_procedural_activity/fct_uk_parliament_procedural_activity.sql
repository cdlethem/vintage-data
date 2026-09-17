{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(id as varchar) as procedural_item_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(layingdate as timestamp with time zone) as laying_at,
        cast(businessitemdate as json) as business_item_dates,
        cast(json_array_length(businessitemdate) as bigint) as business_item_date_count,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_uk_parliament_procedural_activity') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source_relation := source_relation,
            procedural_item_id := procedural_item_id,
            observed_at := observed_at
        ))) as varchar) as uk_parliament_procedural_activity_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by source_relation, procedural_item_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
),

scheduled_dates as (
    select
        uk_parliament_procedural_activity_observation_key,
        coalesce(
            try_cast(json_extract_string(schedule_date.value, '$.BusinessDate') as timestamp with time zone),
            try_cast(json_extract_string(schedule_date.value, '$.Date') as timestamp with time zone),
            try_cast(json_extract_string(schedule_date.value, '$.BusinessItemDate') as timestamp with time zone),
            try_cast(json_extract_string(schedule_date.value, '$') as timestamp with time zone)
        ) as business_item_at
    from deduplicated
    cross join lateral json_each(business_item_dates) as schedule_date
),

scheduled_date_summary as (
    select
        uk_parliament_procedural_activity_observation_key,
        min(business_item_at) as first_business_item_at,
        max(business_item_at) as last_business_item_at
    from scheduled_dates
    group by 1
)

select
    deduplicated.uk_parliament_procedural_activity_observation_key,
    deduplicated.source_relation,
    deduplicated.source_name,
    deduplicated.procedural_item_id,
    deduplicated.observed_at,
    deduplicated.laying_at,
    deduplicated.business_item_dates,
    deduplicated.business_item_date_count,
    scheduled_date_summary.first_business_item_at,
    scheduled_date_summary.last_business_item_at,
    deduplicated.source_row_id,
    deduplicated.source_batch_id,
    deduplicated.source_file,
    deduplicated.source_file_row_number,
    deduplicated.source_date,
    deduplicated.extract_started_at,
    deduplicated.load_id,
    deduplicated.source_loaded_at,
    deduplicated.content_hash
from deduplicated
left join scheduled_date_summary using (uk_parliament_procedural_activity_observation_key)
