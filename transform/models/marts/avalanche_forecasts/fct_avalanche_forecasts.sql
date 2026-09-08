{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := cast(source as varchar),
                        forecast_id := cast(id as varchar),
                        observed_at := cast(fetched_at as timestamp with time zone)
                    )
                )
            ) as varchar
        ) as avalanche_forecast_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as forecast_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(network as varchar) as network,
        cast(center_id as varchar) as center_id,
        cast(center as varchar) as center,
        cast(zone_id as bigint) as zone_id,
        cast(state as varchar) as state,
        cast(timezone as varchar) as timezone,
        cast(danger_text as varchar) as danger_text,
        cast(off_season as boolean) as off_season,
        cast(start_date as timestamp with time zone) as start_date,
        cast(end_date as timestamp with time zone) as end_date,
        cast(travel_advice as varchar) as travel_advice,
        cast(warning as boolean) as warning,
        cast(link as varchar) as link,
        cast(provider as varchar) as provider,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_avalanche_forecasts') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by avalanche_forecast_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    avalanche_forecast_key,
    source_name,
    forecast_id,
    observed_at,
    network,
    center_id,
    center,
    zone_id,
    state,
    timezone,
    danger_text,
    off_season,
    start_date,
    end_date,
    travel_advice,
    warning,
    link,
    provider,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
