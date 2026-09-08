{{ config(
    materialized='table',
    tags=['daily']
) }}

with ranked_observations as (
    select
        cast(md5(to_json(struct_pack(
            source := cast(source as varchar),
            quota_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as catch_quota_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as quota_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(system as varchar) as management_system,
        cast(share_category as varchar) as share_category,
        cast(start_of_year_quota as varchar) as start_of_year_quota,
        cast(quota_increase as varchar) as quota_increase,
        cast(quota_increase_date as varchar) as quota_increase_date,
        cast(end_of_year_quota as varchar) as end_of_year_quota,
        cast(landings as varchar) as landings,
        cast(quota_landed as varchar) as quota_landed,
        cast(quota_remaining as varchar) as quota_remaining,
        cast(start_of_year_quota_value as bigint) as start_of_year_quota_value,
        cast(quota_increase_value as varchar) as quota_increase_value,
        cast(end_of_year_quota_value as bigint) as end_of_year_quota_value,
        cast(landings_value as bigint) as landings_value,
        cast(quota_landed_value as double) as quota_landed_value,
        cast(quota_remaining_value as double) as quota_remaining_value,
        cast(fishery_year as bigint) as fishery_year,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_noaa_catch_quotas') }}
)

select
    catch_quota_observation_key,
    source,
    quota_id,
    observed_at,
    management_system,
    share_category,
    start_of_year_quota,
    quota_increase,
    quota_increase_date,
    end_of_year_quota,
    landings,
    quota_landed,
    quota_remaining,
    start_of_year_quota_value,
    quota_increase_value,
    end_of_year_quota_value,
    landings_value,
    quota_landed_value,
    quota_remaining_value,
    fishery_year,
    _row_id,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked_observations
where observation_rank = 1
