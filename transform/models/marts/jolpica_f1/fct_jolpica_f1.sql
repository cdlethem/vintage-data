{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with ranked as (
    select
        md5(to_json(struct_pack(
            season := cast(season as varchar),
            round := cast(round as varchar),
            driver_id := cast(driver as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as jolpica_f1_key,
        try_cast(season as bigint) as season,
        try_cast(round as bigint) as round,
        cast(driver as varchar) as driver_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(id as varchar) as source_record_id,
        cast(driver_name as varchar) as driver_name,
        cast(nationality as varchar) as nationality,
        cast(constructor as varchar) as constructor,
        try_cast(position as bigint) as championship_position,
        try_cast(points as double) as championship_points,
        try_cast(wins as bigint) as race_wins,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        row_number() over (
            partition by season, round, driver, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as row_number
    from {{ ref('base_jolpica_f1') }}
)

select
    jolpica_f1_key,
    season,
    round,
    driver_id,
    observed_at,
    source_record_id,
    driver_name,
    nationality,
    constructor,
    championship_position,
    championship_points,
    race_wins,
    source_file,
    source_file_row_number,
    source_loaded_at,
    content_hash
from ranked
where row_number = 1
