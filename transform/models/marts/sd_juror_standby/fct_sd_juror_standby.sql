{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

select
    md5(to_json(struct_pack(
        standby_id := cast(id as varchar),
        observed_at := cast(fetched_at as timestamp with time zone)
    ))) as standby_observation_key,
    cast(source as varchar) as source_name,
    cast(id as varchar) as standby_id,
    cast(courthouse as varchar) as courthouse,
    try_strptime(cast(publisher_updated_at as varchar), '%B %d, %Y')::date as service_start_date,
    cast(fetched_at as timestamp with time zone) as observed_at,
    cast(instructions as varchar) as instructions,
    cast(group_ranges as json) as group_ranges,
    cast(length(instructions) as bigint) as instruction_length,
    cast(json_array_length(group_ranges) as bigint) as group_range_count,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from {{ ref('base_sd_juror_standby') }}
