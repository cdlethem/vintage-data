{{ config(materialized='table', tags=['daily']) }}

with ranked as (
    select
        *,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _load_id desc, _file_row_num desc, _row_id desc
        ) as replay_rank
    from {{ ref('base_nager_date') }}
)
select
    cast(md5(to_json(list_value(source, id, cast(fetched_at as varchar)))) as varchar) as holiday_observation_key,
    cast(source as varchar) as source,
    cast(id as varchar) as holiday_id,
    cast(fetched_at as timestamp with time zone) as observed_at,
    cast(country_code as varchar) as country_code,
    cast(date as date) as holiday_date,
    cast(country_code || ':' || cast(date as varchar) as varchar) as country_date_key,
    cast(name as varchar) as holiday_name,
    cast(local_name as varchar) as local_name,
    cast(types as varchar) as holiday_types,
    cast("global" as boolean) as is_nationwide,
    cast(case when "global" then 'Nationwide' when "global" = false then 'Regional' else 'Unspecified' end as varchar) as observance_scope,
    cast(counties as varchar) as subdivision_codes,
    cast(_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_source_file as varchar) as source_file,
    cast(_load_id as varchar) as load_id,
    cast(_row_id as varchar) as raw_row_id
from ranked
where replay_rank = 1
