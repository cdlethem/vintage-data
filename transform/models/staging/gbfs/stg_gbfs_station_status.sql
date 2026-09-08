with station_status as (
    select * from {{ ref('base_gbfs_citibike') }}
    union all
    select * from {{ ref('base_gbfs_divvy') }}
)

select
    md5(to_json(struct_pack(
        network := cast(_source as varchar),
        station_id := cast(station_id as varchar),
        observed_at := cast(fetched_at as timestamptz)
    ))) as station_status_key,
    cast(_source as varchar) as network,
    cast(station_id as varchar) as station_id,
    cast(fetched_at as timestamptz) as observed_at,
    case
        when last_reported > 0 then to_timestamp(last_reported)
        else cast(null as timestamptz)
    end as station_reported_at,
    cast(num_bikes_available as bigint) as num_bikes_available,
    cast(num_docks_available as bigint) as num_docks_available,
    cast(num_ebikes_available as bigint) as num_ebikes_available,
    cast(num_bikes_disabled as bigint) as num_bikes_disabled,
    cast(num_docks_disabled as bigint) as num_docks_disabled,
    cast(is_installed as boolean) as is_installed,
    cast(is_renting as boolean) as is_renting,
    cast(is_returning as boolean) as is_returning,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_loaded_at as timestamptz) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from station_status
