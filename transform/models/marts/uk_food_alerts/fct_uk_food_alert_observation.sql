{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as alert_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(id_2 as varchar) as alert_identifier_url,
        cast(title as varchar) as title,
        cast(notation as varchar) as notation,
        cast(created as date) as created_date,
        cast(modified as timestamp with time zone) as modified_at,
        cast(type as json) as alert_types,
        cast(status as json) as status,
        cast(json_extract_string(status, '$."@id"') as varchar) as status_id,
        cast(json_extract_string(status, '$.label') as varchar) as status_label,
        cast(alerturl as varchar) as alert_url,
        cast(json_extract_string(reportingbusiness, '$.commonName') as varchar) as reporting_business,
        cast(problem as json) as problems,
        cast(json_extract_string(problem[0], '$.riskStatement') as varchar) as primary_risk_statement,
        cast(json_extract(problem[0], '$.allergen') as json) as primary_allergens,
        cast(json_extract(problem[0], '$.pathogenRisk') as json) as primary_pathogen_risk,
        cast(productdetails as json) as product_details,
        cast(country as json) as countries,
        cast(shorttitle as varchar) as short_title,
        cast(_row_id as varchar) as _row_id,
        cast(_source as varchar) as _source,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_uk_food_alerts') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            alert_id := alert_id,
            fetched_at := fetched_at
        ))) as varchar) as uk_food_alert_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by uk_food_alert_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(uk_food_alert_key as varchar) as uk_food_alert_key,
    cast(source as varchar) as source,
    cast(alert_id as varchar) as alert_id,
    cast(fetched_at as timestamp with time zone) as fetched_at,
    cast(alert_identifier_url as varchar) as alert_identifier_url,
    cast(title as varchar) as title,
    cast(notation as varchar) as notation,
    cast(created_date as date) as created_date,
    cast(modified_at as timestamp with time zone) as modified_at,
    cast(alert_types as json) as alert_types,
    cast(status as json) as status,
    cast(status_id as varchar) as status_id,
    cast(status_label as varchar) as status_label,
    cast(alert_url as varchar) as alert_url,
    cast(reporting_business as varchar) as reporting_business,
    cast(problems as json) as problems,
    cast(primary_risk_statement as varchar) as primary_risk_statement,
    cast(primary_allergens as json) as primary_allergens,
    cast(primary_pathogen_risk as json) as primary_pathogen_risk,
    cast(product_details as json) as product_details,
    cast(countries as json) as countries,
    cast(short_title as varchar) as short_title,
    cast(_row_id as varchar) as _row_id,
    cast(_source as varchar) as _source,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
