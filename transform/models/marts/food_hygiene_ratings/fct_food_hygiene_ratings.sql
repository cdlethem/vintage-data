{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='food_hygiene_rating_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as rating_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(addressline1 as varchar) as address_line_1,
        cast(addressline2 as varchar) as address_line_2,
        cast(addressline3 as varchar) as address_line_3,
        cast(addressline4 as varchar) as address_line_4,
        cast(businessname as varchar) as business_name,
        cast(businesstype as varchar) as business_type,
        cast(businesstypeid as bigint) as business_type_id,
        cast(changesbyserverid as bigint) as changed_by_server_id,
        cast(fhrsid as bigint) as food_hygiene_rating_id,
        cast(localauthoritybusinessid as varchar) as local_authority_business_id,
        cast(localauthoritycode as varchar) as local_authority_code,
        cast(localauthorityemailaddress as varchar) as local_authority_email,
        cast(localauthorityname as varchar) as local_authority_name,
        cast(localauthoritywebsite as varchar) as local_authority_website,
        cast(newratingpending as boolean) as new_rating_pending,
        cast(phone as varchar) as phone,
        cast(postcode as varchar) as postcode,
        cast(ratingdate as timestamp with time zone) as rating_at,
        cast(ratingkey as varchar) as rating_key,
        cast(ratingvalue as varchar) as rating_value,
        try_cast(nullif(trim(ratingvalue), '') as bigint) as rating_score,
        cast(righttoreply as varchar) as right_to_reply,
        cast(schemetype as varchar) as scheme_type,
        cast(geocode as varchar) as geocode,
        try_cast(json_extract_string(geocode, '$.latitude') as double) as latitude,
        try_cast(json_extract_string(geocode, '$.longitude') as double) as longitude,
        cast(scores as varchar) as scores,
        try_cast(json_extract_string(scores, '$.Hygiene') as bigint) as hygiene_score,
        try_cast(json_extract_string(scores, '$.Structural') as bigint) as structural_score,
        try_cast(json_extract_string(scores, '$.ConfidenceInManagement') as bigint) as confidence_in_management_score,
        cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_food_hygiene_ratings') }}
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

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            rating_id := rating_id,
            observed_at := observed_at
        ))) as varchar) as food_hygiene_rating_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by food_hygiene_rating_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(food_hygiene_rating_key as varchar) as food_hygiene_rating_key,
    cast(source as varchar) as source,
    cast(rating_id as varchar) as rating_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(address_line_1 as varchar) as address_line_1,
    cast(address_line_2 as varchar) as address_line_2,
    cast(address_line_3 as varchar) as address_line_3,
    cast(address_line_4 as varchar) as address_line_4,
    cast(business_name as varchar) as business_name,
    cast(business_type as varchar) as business_type,
    cast(business_type_id as bigint) as business_type_id,
    cast(changed_by_server_id as bigint) as changed_by_server_id,
    cast(food_hygiene_rating_id as bigint) as food_hygiene_rating_id,
    cast(local_authority_business_id as varchar) as local_authority_business_id,
    cast(local_authority_code as varchar) as local_authority_code,
    cast(local_authority_email as varchar) as local_authority_email,
    cast(local_authority_name as varchar) as local_authority_name,
    cast(local_authority_website as varchar) as local_authority_website,
    cast(new_rating_pending as boolean) as new_rating_pending,
    cast(phone as varchar) as phone,
    cast(postcode as varchar) as postcode,
    cast(rating_at as timestamp with time zone) as rating_at,
    cast(rating_key as varchar) as rating_key,
    cast(rating_value as varchar) as rating_value,
    cast(rating_score as bigint) as rating_score,
    cast(right_to_reply as varchar) as right_to_reply,
    cast(scheme_type as varchar) as scheme_type,
    cast(geocode as varchar) as geocode,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(scores as varchar) as scores,
    cast(hygiene_score as bigint) as hygiene_score,
    cast(structural_score as bigint) as structural_score,
    cast(confidence_in_management_score as bigint) as confidence_in_management_score,
    cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
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
