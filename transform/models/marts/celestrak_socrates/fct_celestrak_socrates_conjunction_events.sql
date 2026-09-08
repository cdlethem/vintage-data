{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            conjunction_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as conjunction_event_key,
        cast(source as varchar) as source,
        cast(id as varchar) as conjunction_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(norad_id_1 as varchar) as norad_id_1,
        cast(object_name_1 as varchar) as object_name_1,
        cast(norad_id_2 as varchar) as norad_id_2,
        cast(object_name_2 as varchar) as object_name_2,
        cast(tca as timestamp with time zone) as tca,
        cast(tca_range_km as double) as tca_range_km,
        cast(tca_relative_speed_km_s as double) as tca_relative_speed_km_s,
        cast(max_prob as double) as max_probability,
        cast(dilution_km as double) as dilution_km,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_celestrak_socrates') }}
)

select
    conjunction_event_key,
    source,
    conjunction_id,
    observed_at,
    norad_id_1,
    object_name_1,
    norad_id_2,
    object_name_2,
    tca,
    tca_range_km,
    tca_relative_speed_km_s,
    max_probability,
    dilution_km,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from source_rows
where _dedupe_rank = 1
