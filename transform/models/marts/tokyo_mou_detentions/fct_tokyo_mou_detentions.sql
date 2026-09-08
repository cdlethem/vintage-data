{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with ranked_observations as (
    select
        md5(to_json(struct_pack(
            detention_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        )))::varchar as tokyo_mou_detention_observation_key,
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(_row_id as varchar) as source_row_id,
        cast(id as varchar) as detention_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(imo_no as varchar) as imo_number,
        cast(ship_name as varchar) as ship_name,
        cast(ship_flag as varchar) as flag_country,
        cast(year_of_build as date) as build_date,
        try_cast(nullif(trim(gross_tonnage), '') as bigint) as gross_tonnage,
        cast(ship_type as varchar) as ship_type,
        cast(classification_society as varchar) as classification_society,
        cast(related_ros as varchar) as related_recognized_organizations,
        cast(company as varchar) as company_name,
        cast(place_of_detention as varchar) as detention_location,
        try_strptime(nullif(trim(date_of_detention), ''), '%d.%m.%Y')::date as detention_date,
        try_strptime(nullif(trim(date_of_release), ''), '%d.%m.%Y')::date as release_date,
        cast(nature_of_deficiencies as varchar) as deficiency_summary,
        cast(query_year as bigint) as query_year,
        cast(query_month as bigint) as query_month,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_tokyo_mou_detentions') }}
)

select
    tokyo_mou_detention_observation_key,
    source_relation,
    source_name,
    source_row_id,
    detention_id,
    observed_at,
    imo_number,
    ship_name,
    flag_country,
    build_date,
    gross_tonnage,
    ship_type,
    classification_society,
    related_recognized_organizations,
    company_name,
    detention_location,
    detention_date,
    release_date,
    deficiency_summary,
    query_year,
    query_month,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked_observations
where observation_rank = 1
