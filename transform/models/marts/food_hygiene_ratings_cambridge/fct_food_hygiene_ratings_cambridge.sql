{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as establishment_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(ratingdate as timestamp with time zone) as rating_date,
        cast(ratingkey as varchar) as rating_key,
        cast(ratingvalue as varchar) as rating_value,
        cast(businessname as varchar) as business_name,
        cast(businesstype as varchar) as business_type,
        cast(businesstypeid as bigint) as business_type_id,
        cast(addressline1 as varchar) as address_line_1,
        cast(addressline2 as varchar) as address_line_2,
        cast(addressline3 as varchar) as address_line_3,
        cast(addressline4 as varchar) as address_line_4,
        cast(postcode as varchar) as postcode,
        cast(phone as varchar) as phone,
        cast(localauthoritybusinessid as varchar) as local_authority_business_id,
        cast(localauthoritycode as varchar) as local_authority_code,
        cast(localauthorityname as varchar) as local_authority_name,
        cast(localauthorityemailaddress as varchar) as local_authority_email,
        cast(localauthoritywebsite as varchar) as local_authority_website,
        cast(newratingpending as boolean) as new_rating_pending,
        cast(righttoreply as varchar) as right_to_reply,
        cast(schemetype as varchar) as scheme_type,
        try_cast(json_extract_string(geocode, '$.latitude') as double) as latitude,
        try_cast(json_extract_string(geocode, '$.longitude') as double) as longitude,
        try_cast(json_extract_string(scores, '$.Hygiene') as bigint) as hygiene_score,
        try_cast(json_extract_string(scores, '$.Structural') as bigint) as structural_score,
        try_cast(json_extract_string(scores, '$.ConfidenceInManagement') as bigint) as confidence_in_management_score,
        cast(geocode as json) as geocode,
        cast(scores as json) as scores,
        cast(fhrsid as bigint) as fhrs_id,
        cast(changesbyserverid as bigint) as changes_by_server_id,
        cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_food_hygiene_ratings_cambridge') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        establishment_id := establishment_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as food_hygiene_ratings_cambridge_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by food_hygiene_ratings_cambridge_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(food_hygiene_ratings_cambridge_key as varchar) as food_hygiene_ratings_cambridge_key,
    cast(source as varchar) as source,
    cast(establishment_id as varchar) as establishment_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(rating_date as timestamp with time zone) as rating_date,
    cast(rating_key as varchar) as rating_key,
    cast(rating_value as varchar) as rating_value,
    cast(business_name as varchar) as business_name,
    cast(business_type as varchar) as business_type,
    cast(business_type_id as bigint) as business_type_id,
    cast(address_line_1 as varchar) as address_line_1,
    cast(address_line_2 as varchar) as address_line_2,
    cast(address_line_3 as varchar) as address_line_3,
    cast(address_line_4 as varchar) as address_line_4,
    cast(postcode as varchar) as postcode,
    cast(phone as varchar) as phone,
    cast(local_authority_business_id as varchar) as local_authority_business_id,
    cast(local_authority_code as varchar) as local_authority_code,
    cast(local_authority_name as varchar) as local_authority_name,
    cast(local_authority_email as varchar) as local_authority_email,
    cast(local_authority_website as varchar) as local_authority_website,
    cast(new_rating_pending as boolean) as new_rating_pending,
    cast(right_to_reply as varchar) as right_to_reply,
    cast(scheme_type as varchar) as scheme_type,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(hygiene_score as bigint) as hygiene_score,
    cast(structural_score as bigint) as structural_score,
    cast(confidence_in_management_score as bigint) as confidence_in_management_score,
    cast(geocode as json) as geocode,
    cast(scores as json) as scores,
    cast(fhrs_id as bigint) as fhrs_id,
    cast(changes_by_server_id as bigint) as changes_by_server_id,
    cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
