{{ config(
    materialized='table',
    tags=['daily']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(
            case
                when work_type is not null then 'work_type'
                when ecct_work_type is not null then 'ecct_work_type'
                when ecf_work_type is not null then 'ecf_work_type'
                else 'unknown'
            end as varchar
        ) as processing_stream,
        cast(id as varchar) as processing_item,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(try_strptime(trim(current_processing_date), '%d %B %Y') as date) as current_processing_date,
        cast(try_strptime(trim(publisher_updated_at), '%d %B %Y') as date) as publisher_updated_date,
        cast(backlog_older_than_current_processing_date as varchar) as backlog_older_than_current_processing_date_text,
        cast(
            try_cast(
                regexp_replace(trim(backlog_older_than_current_processing_date), '%', '')
                as double
            ) / 100.0 as double
        ) as backlog_older_than_current_processing_date_pct,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_uk_legal_aid_processing') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        processing_stream := processing_stream,
                        processing_item := processing_item,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as legal_aid_processing_key,
        source,
        processing_stream,
        processing_item,
        observed_at,
        current_processing_date,
        publisher_updated_date,
        backlog_older_than_current_processing_date_text,
        backlog_older_than_current_processing_date_pct,
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
        partition by legal_aid_processing_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(legal_aid_processing_key as varchar) as legal_aid_processing_key,
    cast(source as varchar) as source,
    cast(processing_stream as varchar) as processing_stream,
    cast(processing_item as varchar) as processing_item,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(current_processing_date as date) as current_processing_date,
    cast(publisher_updated_date as date) as publisher_updated_date,
    cast(backlog_older_than_current_processing_date_text as varchar) as backlog_older_than_current_processing_date_text,
    cast(backlog_older_than_current_processing_date_pct as double) as backlog_older_than_current_processing_date_pct,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
