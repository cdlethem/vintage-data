{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with ranked_warnings as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            warning_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as niord_warning_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as warning_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(to_timestamp(created / 1000.0) as timestamp with time zone) as created_at,
        cast(to_timestamp(updated / 1000.0) as timestamp with time zone) as updated_at,
        cast(number as bigint) as warning_number,
        cast(shortid as varchar) as short_id,
        cast(maintype as varchar) as main_type,
        cast(type as varchar) as warning_type,
        cast(status as varchar) as status,
        cast(messageseries as json) as message_series,
        cast(areas as json) as areas,
        cast(to_timestamp(publishdatefrom / 1000.0) as timestamp with time zone) as publish_date_from,
        cast(to_timestamp(followupdate / 1000.0) as timestamp with time zone) as follow_up_date,
        cast(originalinformation as boolean) as original_information,
        cast(parts as json) as parts,
        cast(descs as json) as descriptions,
        cast("references" as json) as references_json,
        cast(charts as json) as charts,
        cast(attachments as json) as attachments,
        cast(categories as json) as categories,
        cast(to_timestamp(publishdateto / 1000.0) as timestamp with time zone) as publish_date_to,
        cast(horizontaldatum as varchar) as horizontal_datum,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as warning_rank
    from {{ ref('base_niord_warnings') }}
)

select
    niord_warning_key,
    source_name,
    warning_id,
    observed_at,
    created_at,
    updated_at,
    warning_number,
    short_id,
    main_type,
    warning_type,
    status,
    message_series,
    areas,
    publish_date_from,
    follow_up_date,
    original_information,
    parts,
    descriptions,
    references_json,
    charts,
    attachments,
    categories,
    publish_date_to,
    horizontal_datum,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from ranked_warnings
where warning_rank = 1
