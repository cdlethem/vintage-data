{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='manifold_market_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as market_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(question as varchar) as question,
        cast(creator as varchar) as creator,
        cast(created as timestamp with time zone) as created_at,
        cast(close_time as timestamp with time zone) as close_time,
        cast(outcome_type as varchar) as outcome_type,
        cast(probability as double) as probability,
        cast(volume as double) as volume,
        cast(volume_24h as double) as volume_24h,
        cast(n_bettors as bigint) as bettor_count,
        cast(resolved as boolean) as is_resolved,
        cast(resolution as varchar) as resolution,
        cast(resolution_prob as double) as resolution_probability,
        cast(last_bet as timestamp with time zone) as last_bet_at,
        cast(url as varchar) as market_url,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_manifold_markets') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        market_id := market_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as manifold_market_observation_key,
        selected_source.*
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by manifold_market_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    manifold_market_observation_key,
    source_name,
    market_id,
    observed_at,
    question,
    creator,
    created_at,
    close_time,
    outcome_type,
    probability,
    volume,
    volume_24h,
    bettor_count,
    is_resolved,
    resolution,
    resolution_probability,
    last_bet_at,
    market_url,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_load_id,
    source_loaded_at,
    content_hash
from deduplicated
