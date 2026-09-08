{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            observation_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as sveriges_radio_program_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(episodeid as bigint) as episode_id,
        cast(title as varchar) as title,
        cast(description as varchar) as description,
        case
            when try_cast(regexp_extract(starttimeutc, '/Date\\((-?[0-9]+)\\)/', 1) as bigint) is not null
            then to_timestamp(
                try_cast(regexp_extract(starttimeutc, '/Date\\((-?[0-9]+)\\)/', 1) as bigint) / 1000.0
            )
            else null
        end as scheduled_start_at,
        case
            when try_cast(regexp_extract(endtimeutc, '/Date\\((-?[0-9]+)\\)/', 1) as bigint) is not null
            then to_timestamp(
                try_cast(regexp_extract(endtimeutc, '/Date\\((-?[0-9]+)\\)/', 1) as bigint) / 1000.0
            )
            else null
        end as scheduled_end_at,
        cast(json_extract_string(program, '$.id') as varchar) as program_id,
        cast(json_extract_string(program, '$.name') as varchar) as program_name,
        cast(program as json) as program,
        cast(socialimage as varchar) as social_image_url,
        cast(channel_id as bigint) as channel_id,
        cast(channel_name as varchar) as channel_name,
        cast(relation as varchar) as schedule_relation,
        cast(subtitle as varchar) as subtitle,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_sveriges_radio_now') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by sveriges_radio_program_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(sveriges_radio_program_observation_key as varchar) as sveriges_radio_program_observation_key,
    cast(source_name as varchar) as source_name,
    cast(observation_id as varchar) as observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(episode_id as bigint) as episode_id,
    cast(title as varchar) as title,
    cast(description as varchar) as description,
    cast(scheduled_start_at as timestamp with time zone) as scheduled_start_at,
    cast(scheduled_end_at as timestamp with time zone) as scheduled_end_at,
    cast(program_id as varchar) as program_id,
    cast(program_name as varchar) as program_name,
    cast(program as json) as program,
    cast(social_image_url as varchar) as social_image_url,
    cast(channel_id as bigint) as channel_id,
    cast(channel_name as varchar) as channel_name,
    cast(schedule_relation as varchar) as schedule_relation,
    cast(subtitle as varchar) as subtitle,
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
