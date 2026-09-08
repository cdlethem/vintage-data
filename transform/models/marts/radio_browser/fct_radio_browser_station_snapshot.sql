{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as station_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(name as varchar) as station_name,
        cast(country_code as varchar) as country_code,
        cast(state as varchar) as state,
        cast(language as varchar) as language,
        cast(language_codes as varchar) as language_codes,
        cast(tags as json) as tags,
        cast(codec as varchar) as codec,
        cast(bitrate as bigint) as bitrate_kbps,
        cast(homepage as varchar) as homepage,
        cast(clickcount as bigint) as click_count,
        cast(clicktrend as bigint) as click_trend,
        cast(votes as bigint) as vote_count,
        cast(click_timestamp as timestamp with time zone) as click_observed_at,
        cast(last_check_ok as boolean) as last_check_ok,
        cast(last_check_time as timestamp with time zone) as last_checked_at,
        cast(last_change_time as timestamp with time zone) as last_changed_at,
        cast(geo_lat as double) as latitude,
        cast(geo_lon as double) as longitude,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_radio_browser') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        station_id := station_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as radio_browser_station_snapshot_key,
        source,
        station_id,
        observed_at,
        station_name,
        country_code,
        state,
        language,
        language_codes,
        tags,
        codec,
        bitrate_kbps,
        homepage,
        click_count,
        click_trend,
        vote_count,
        click_observed_at,
        last_check_ok,
        last_checked_at,
        last_changed_at,
        latitude,
        longitude,
        _batch_id,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by radio_browser_station_snapshot_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(radio_browser_station_snapshot_key as varchar) as radio_browser_station_snapshot_key,
    cast(source as varchar) as source,
    cast(station_id as varchar) as station_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(station_name as varchar) as station_name,
    cast(country_code as varchar) as country_code,
    cast(state as varchar) as state,
    cast(language as varchar) as language,
    cast(language_codes as varchar) as language_codes,
    cast(tags as json) as tags,
    cast(codec as varchar) as codec,
    cast(bitrate_kbps as bigint) as bitrate_kbps,
    cast(homepage as varchar) as homepage,
    cast(click_count as bigint) as click_count,
    cast(click_trend as bigint) as click_trend,
    cast(vote_count as bigint) as vote_count,
    cast(click_observed_at as timestamp with time zone) as click_observed_at,
    cast(last_check_ok as boolean) as last_check_ok,
    cast(last_checked_at as timestamp with time zone) as last_checked_at,
    cast(last_changed_at as timestamp with time zone) as last_changed_at,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
