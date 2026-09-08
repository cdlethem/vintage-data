{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_feed,
        cast(id as varchar) as train_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(commuterlineid as varchar) as commuter_line_id,
        cast(runningcurrently as boolean) as is_running_currently,
        cast(cancelled as boolean) as is_cancelled,
        cast(version as bigint) as record_version,
        cast(timetabletype as varchar) as timetable_type,
        cast(timetableacceptancedate as timestamp with time zone) as timetable_acceptance_at,
        cast(departuredate as date) as departure_date,
        cast(trainnumber as bigint) as train_number,
        cast(operatoruiccode as bigint) as operator_uic_code,
        cast(operatorshortcode as varchar) as operator_short_code,
        cast(timetablerows as json) as timetable_rows,
        cast(traincategory as varchar) as train_category,
        cast(traintype as varchar) as train_type,
        cast(requested_station as varchar) as requested_station,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_digitraffic_rail') }}
),

deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_feed := source_feed,
            train_id := train_id,
            observed_at := observed_at
        ))) as varchar) as digitraffic_rail_train_observation_key,
        *
    from source_rows
    qualify row_number() over (
        partition by source_feed, train_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(digitraffic_rail_train_observation_key as varchar) as digitraffic_rail_train_observation_key,
    cast(source_feed as varchar) as source_feed,
    cast(train_id as varchar) as train_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(commuter_line_id as varchar) as commuter_line_id,
    cast(is_running_currently as boolean) as is_running_currently,
    cast(is_cancelled as boolean) as is_cancelled,
    cast(record_version as bigint) as record_version,
    cast(timetable_type as varchar) as timetable_type,
    cast(timetable_acceptance_at as timestamp with time zone) as timetable_acceptance_at,
    cast(departure_date as date) as departure_date,
    cast(train_number as bigint) as train_number,
    cast(operator_uic_code as bigint) as operator_uic_code,
    cast(operator_short_code as varchar) as operator_short_code,
    cast(timetable_rows as json) as timetable_rows,
    cast(train_category as varchar) as train_category,
    cast(train_type as varchar) as train_type,
    cast(requested_station as varchar) as requested_station,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
