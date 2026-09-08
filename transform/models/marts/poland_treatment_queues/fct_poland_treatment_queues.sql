{{ config(
    materialized='table',
    tags=['daily']
) }}

select
    md5(to_json(struct_pack(
        source := source,
        queue_id := id,
        observed_at := fetched_at
    )))::varchar as queue_observation_key,
    source::varchar as source,
    id::varchar as queue_id,
    type::varchar as queue_type,
    fetched_at::timestamp with time zone as observed_at,
    publisher_updated_at::timestamp with time zone as publisher_updated_at,
    try_cast(attributes->>'case' as bigint) as case_number,
    attributes->>'benefit' as benefit_name,
    attributes->>'provider' as provider_name,
    attributes->>'provider-code' as provider_code,
    attributes->>'regon-provider' as provider_regon,
    attributes->>'nip-provider' as provider_nip,
    attributes->>'teryt-provider' as provider_teryt_code,
    attributes->>'place' as treatment_place,
    attributes->>'address' as provider_address,
    attributes->>'locality' as locality,
    attributes->>'phone' as provider_phone,
    attributes->>'teryt-place' as place_teryt_code,
    attributes->>'registry-number' as registry_number,
    attributes->>'id-resort-part-VII' as resort_part_vii_code,
    attributes->>'id-resort-part-VIII' as resort_part_viii_code,
    attributes->>'benefits-for-children' as benefits_for_children,
    attributes->>'age-range' as age_range,
    attributes->>'covid-19' as covid_19,
    attributes->>'queue-in-cer' as queue_in_cer,
    try_cast((attributes->'dates')->>'applicable' as boolean) as situation_date_applicable,
    (attributes->'dates')->>'pcus' as pcus_text,
    try_cast((attributes->'dates')->>'date-situation-as-at' as date) as situation_date,
    try_cast((attributes->'statistics'->'provider-data')->>'awaiting' as bigint) as patients_awaiting,
    try_cast((attributes->'statistics'->'provider-data')->>'removed' as bigint) as patients_removed,
    try_cast((attributes->'statistics'->'provider-data')->>'average-period' as bigint) as average_wait_days,
    (attributes->'statistics'->'provider-data')->>'update' as statistics_update_month,
    _row_id::varchar as _row_id,
    _source_file::varchar as _source_file,
    _file_row_num::bigint as _file_row_num,
    _loaded_at::timestamp with time zone as source_loaded_at,
    _content_hash::varchar as _content_hash
from {{ ref('base_poland_treatment_queues') }}
