{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source,
        cast(id as varchar) as petition_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(type as varchar) as record_type,
        cast(attributes->>'action' as varchar) as action,
        cast(attributes->>'background' as varchar) as background,
        cast(attributes->>'additional_details' as varchar) as additional_details,
        cast(attributes->>'committee_note' as varchar) as committee_note,
        cast(attributes->>'state' as varchar) as state,
        try_cast(attributes->>'signature_count' as bigint) as signature_count,
        try_cast(attributes->>'closing_date' as date) as closing_date,
        try_cast(attributes->>'created_at' as timestamp with time zone) as created_at,
        try_cast(attributes->>'updated_at' as timestamp with time zone) as updated_at,
        try_cast(attributes->>'rejected_at' as timestamp with time zone) as rejected_at,
        try_cast(attributes->>'opened_at' as timestamp with time zone) as opened_at,
        try_cast(attributes->>'closed_at' as timestamp with time zone) as closed_at,
        try_cast(attributes->>'moderation_threshold_reached_at' as timestamp with time zone) as moderation_threshold_reached_at,
        try_cast(attributes->>'response_threshold_reached_at' as timestamp with time zone) as response_threshold_reached_at,
        try_cast(attributes->>'government_response_at' as timestamp with time zone) as government_response_at,
        try_cast(attributes->>'debate_threshold_reached_at' as timestamp with time zone) as debate_threshold_reached_at,
        try_cast(attributes->>'debate_scheduled_on' as date) as debate_scheduled_on,
        try_cast(attributes->>'scheduled_debate_date' as date) as scheduled_debate_date,
        try_cast(attributes->>'debate_outcome_at' as timestamp with time zone) as debate_outcome_at,
        cast(attributes->>'creator_name' as varchar) as creator_name,
        cast(attributes->'rejection' as json) as rejection,
        cast(attributes->'government_response' as json) as government_response,
        cast(attributes->'debate' as json) as debate,
        cast(attributes->'departments' as json) as departments,
        cast(attributes->'topics' as json) as topics,
        cast(links->>'self' as varchar) as self_link,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_uk_petitions') }}
),

deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_relation := source_relation,
            petition_id := petition_id,
            observed_at := observed_at
        ))) as varchar) as uk_petition_observation_key,
        *,
        row_number() over (
            partition by source_relation, petition_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(uk_petition_observation_key as varchar) as uk_petition_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source as varchar) as source,
    cast(petition_id as varchar) as petition_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(record_type as varchar) as record_type,
    cast(action as varchar) as action,
    cast(background as varchar) as background,
    cast(additional_details as varchar) as additional_details,
    cast(committee_note as varchar) as committee_note,
    cast(state as varchar) as state,
    cast(signature_count as bigint) as signature_count,
    cast(closing_date as date) as closing_date,
    cast(created_at as timestamp with time zone) as created_at,
    cast(updated_at as timestamp with time zone) as updated_at,
    cast(rejected_at as timestamp with time zone) as rejected_at,
    cast(opened_at as timestamp with time zone) as opened_at,
    cast(closed_at as timestamp with time zone) as closed_at,
    cast(moderation_threshold_reached_at as timestamp with time zone) as moderation_threshold_reached_at,
    cast(response_threshold_reached_at as timestamp with time zone) as response_threshold_reached_at,
    cast(government_response_at as timestamp with time zone) as government_response_at,
    cast(debate_threshold_reached_at as timestamp with time zone) as debate_threshold_reached_at,
    cast(debate_scheduled_on as date) as debate_scheduled_on,
    cast(scheduled_debate_date as date) as scheduled_debate_date,
    cast(debate_outcome_at as timestamp with time zone) as debate_outcome_at,
    cast(creator_name as varchar) as creator_name,
    cast(rejection as json) as rejection,
    cast(government_response as json) as government_response,
    cast(debate as json) as debate,
    cast(departments as json) as departments,
    cast(topics as json) as topics,
    cast(self_link as varchar) as self_link,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_load_id as varchar) as _load_id,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
where _dedupe_rank = 1
