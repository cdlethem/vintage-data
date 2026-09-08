{{ config(
    materialized='table',
    tags=['daily']
) }}

with ranked_observations as (
    select
        md5(
            to_json(
                struct_pack(
                    award_id := award_id,
                    observed_at := fetched_at
                )
            )
        )::varchar as usaspending_award_observation_key,
        source::varchar as source_name,
        id::varchar as award_record_id,
        award_id::varchar as award_id,
        fetched_at::timestamp with time zone as observed_at,
        recipient::varchar as recipient_name,
        start_date::date as performance_start_date,
        last_modified_date::timestamp with time zone as last_modified_at,
        cast(award_amount as decimal(20, 2)) as award_amount,
        awarding_agency::varchar as awarding_agency,
        _batch_id::varchar as _batch_id,
        _load_id::varchar as _load_id,
        _source_file::varchar as _source_file,
        _file_row_num::bigint as _file_row_num,
        _loaded_at::timestamp with time zone as source_loaded_at,
        _content_hash::varchar as _content_hash,
        row_number() over (
            partition by award_id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_usaspending') }}
)

select
    usaspending_award_observation_key,
    source_name,
    award_record_id,
    award_id,
    observed_at,
    recipient_name,
    performance_start_date,
    last_modified_at,
    award_amount,
    awarding_agency,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked_observations
where observation_rank = 1
