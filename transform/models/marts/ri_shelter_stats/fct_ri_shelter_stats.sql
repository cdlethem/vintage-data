{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ri_shelter_stats_observation_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as record_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(entity_name as varchar) as entity_name,
        cast(status as varchar) as status,
        cast(species as varchar) as species,
        cast(abandoned as bigint) as abandoned_count,
        cast(born_in_facility as bigint) as born_in_facility_count,
        cast(pickup_stray as bigint) as pickup_stray_count,
        cast(owner_surrendered as bigint) as owner_surrendered_count,
        cast(known_entity as bigint) as known_entity_count,
        cast(transfer_in as bigint) as transfer_in_count,
        cast(other_in as bigint) as other_in_count,
        cast(returned_to_owner as bigint) as returned_to_owner_count,
        cast(euthanized as bigint) as euthanized_count,
        cast(adopted as bigint) as adopted_count,
        cast(escaped as bigint) as escaped_count,
        cast(stolen as bigint) as stolen_count,
        cast(dead_on_arrival as bigint) as dead_on_arrival_count,
        cast(died as bigint) as died_count,
        cast(transfer_out as bigint) as transfer_out_count,
        cast(other_out as bigint) as other_out_count,
        cast("in" as bigint) as intake_count,
        cast("out" as bigint) as outcome_count,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_ri_shelter_stats') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
),

keyed_rows as (
    select
        cast(md5(to_json(struct_pack(
            source_name := source_name,
            record_id := record_id,
            observed_at := observed_at
        ))) as varchar) as ri_shelter_stats_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed_rows
    qualify row_number() over (
        partition by ri_shelter_stats_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(ri_shelter_stats_observation_key as varchar) as ri_shelter_stats_observation_key,
    cast(source_name as varchar) as source_name,
    cast(record_id as varchar) as record_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(entity_name as varchar) as entity_name,
    cast(status as varchar) as status,
    cast(species as varchar) as species,
    cast(abandoned_count as bigint) as abandoned_count,
    cast(born_in_facility_count as bigint) as born_in_facility_count,
    cast(pickup_stray_count as bigint) as pickup_stray_count,
    cast(owner_surrendered_count as bigint) as owner_surrendered_count,
    cast(known_entity_count as bigint) as known_entity_count,
    cast(transfer_in_count as bigint) as transfer_in_count,
    cast(other_in_count as bigint) as other_in_count,
    cast(returned_to_owner_count as bigint) as returned_to_owner_count,
    cast(euthanized_count as bigint) as euthanized_count,
    cast(adopted_count as bigint) as adopted_count,
    cast(escaped_count as bigint) as escaped_count,
    cast(stolen_count as bigint) as stolen_count,
    cast(dead_on_arrival_count as bigint) as dead_on_arrival_count,
    cast(died_count as bigint) as died_count,
    cast(transfer_out_count as bigint) as transfer_out_count,
    cast(other_out_count as bigint) as other_out_count,
    cast(intake_count as bigint) as intake_count,
    cast(outcome_count as bigint) as outcome_count,
    cast(publisher_updated_at as varchar) as publisher_updated_at,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
