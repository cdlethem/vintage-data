{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as pedestrian_id,
        cast(location_id as bigint) as location_id,
        cast(sensing_datetime as timestamp with time zone) as sensing_datetime,
        cast(sensing_date as date) as sensing_date,
        cast(sensing_time as varchar) as sensing_time,
        cast(direction_1 as bigint) as direction_1,
        cast(direction_2 as bigint) as direction_2,
        cast(total_of_directions as bigint) as total_of_directions,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_melbourne_pedestrians') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            location_id := location_id,
            sensing_datetime := sensing_datetime,
            observed_at := observed_at
        ))) as varchar) as pedestrian_observation_key,
        *
    from normalized
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by pedestrian_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(pedestrian_observation_key as varchar) as pedestrian_observation_key,
    cast(source as varchar) as source,
    cast(pedestrian_id as varchar) as pedestrian_id,
    cast(location_id as bigint) as location_id,
    cast(sensing_datetime as timestamp with time zone) as sensing_datetime,
    cast(sensing_date as date) as sensing_date,
    cast(sensing_time as varchar) as sensing_time,
    cast(direction_1 as bigint) as direction_1,
    cast(direction_2 as bigint) as direction_2,
    cast(total_of_directions as bigint) as total_of_directions,
    cast(observed_at as timestamp with time zone) as observed_at,
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
