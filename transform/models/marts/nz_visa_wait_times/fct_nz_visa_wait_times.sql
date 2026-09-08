{{ config(
    materialized='table',
    tags=['daily']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(coalesce(nullif(trim(visa_type), ''), nullif(trim(id), '')) as varchar) as visa_type,
        cast(
            regexp_replace(
                regexp_replace(
                    trim(lower(coalesce(nullif(visa_type, ''), id))),
                    '[^a-z0-9]+',
                    '_',
                    'g'
                ),
                '^_+|_+$',
                '',
                'g'
            ) as varchar
        ) as visa_type_key,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(average_wait_time as varchar) as average_wait_time_text,
        cast(
            case
                when regexp_matches(
                    lower(trim(average_wait_time)),
                    '^[0-9]+(?:\\.[0-9]+)?\\s+(day|days|week|weeks)$'
                ) then
                    try_cast(
                        regexp_extract(lower(trim(average_wait_time)), '([0-9]+(?:\\.[0-9]+)?)', 1)
                        as double
                    )
                    * case
                        when lower(trim(average_wait_time)) like '%week%' then 7
                        when lower(trim(average_wait_time)) like '%day%' then 1
                      end
                else null
            end as double
        ) as average_wait_days,
        cast(most_completed_within as varchar) as most_completed_within_text,
        cast(
            case
                when regexp_matches(
                    lower(trim(most_completed_within)),
                    '^[0-9]+(?:\\.[0-9]+)?\\s+(day|days|week|weeks)$'
                ) then
                    try_cast(
                        regexp_extract(lower(trim(most_completed_within)), '([0-9]+(?:\\.[0-9]+)?)', 1)
                        as double
                    )
                    * case
                        when lower(trim(most_completed_within)) like '%week%' then 7
                        when lower(trim(most_completed_within)) like '%day%' then 1
                      end
                else null
            end as double
        ) as most_completed_within_days,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_nz_visa_wait_times') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        visa_type_key := visa_type_key,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as visa_wait_time_key,
        source,
        visa_type,
        visa_type_key,
        observed_at,
        average_wait_time_text,
        average_wait_days,
        most_completed_within_text,
        most_completed_within_days,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from normalized
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by visa_wait_time_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(visa_wait_time_key as varchar) as visa_wait_time_key,
    cast(source as varchar) as source,
    cast(visa_type as varchar) as visa_type,
    cast(visa_type_key as varchar) as visa_type_key,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(average_wait_time_text as varchar) as average_wait_time_text,
    cast(average_wait_days as double) as average_wait_days,
    cast(most_completed_within_text as varchar) as most_completed_within_text,
    cast(most_completed_within_days as double) as most_completed_within_days,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
