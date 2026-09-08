{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as feature_id,
        cast(layer_url as varchar) as layer_url,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(type as varchar) as feature_type,
        cast(json_extract_string(geometry, '$.type') as varchar) as geometry_type,
        cast(geometry as json) as geometry,
        try_cast(json_extract(geometry, '$.coordinates[0]') as double) as longitude,
        try_cast(json_extract(geometry, '$.coordinates[1]') as double) as latitude,
        coalesce(
            json_extract_string(properties, '$.ART_ID'),
            json_extract_string(properties, '$.Accession_Number'),
            json_extract_string(properties, '$.GLOBALID'),
            json_extract_string(properties, '$.GlobalID'),
            cast(id as varchar)
        ) as artwork_identifier,
        coalesce(
            json_extract_string(properties, '$.Artwork_Title'),
            json_extract_string(properties, '$.TITLE')
        ) as title,
        coalesce(
            json_extract_string(properties, '$.Category'),
            json_extract_string(properties, '$.TAB_NAME')
        ) as category,
        json_extract_string(properties, '$.Material') as material,
        json_extract_string(properties, '$.Discipline') as discipline,
        coalesce(
            json_extract_string(properties, '$.Current_Status'),
            json_extract_string(properties, '$.MODIFIED_ACTION')
        ) as status,
        json_extract_string(properties, '$.Artist_s_') as artist,
        coalesce(
            json_extract_string(properties, '$.Artwork_Description_1'),
            json_extract_string(properties, '$.SHORT_DESC')
        ) as description,
        json_extract_string(properties, '$.Artwork_Location') as location_name,
        coalesce(
            json_extract_string(properties, '$.Location_Street_Address'),
            json_extract_string(properties, '$.ADDRESS')
        ) as address,
        json_extract_string(properties, '$.Location_City') as city,
        json_extract_string(properties, '$.Location_State') as state,
        json_extract_string(properties, '$.Location_Zip_Code') as postal_code,
        json_extract_string(properties, '$.Neighborhood') as neighborhood,
        json_extract_string(properties, '$.WEBSITE') as website_url,
        coalesce(
            json_extract_string(properties, '$.Photo_Thumbnail_URL'),
            json_extract_string(properties, '$.PIC_URL')
        ) as image_url,
        case
            when try_cast(json_extract_string(properties, '$.MODIFIED_DT') as bigint) > 0
            then cast(to_timestamp(try_cast(json_extract_string(properties, '$.MODIFIED_DT') as bigint) / 1000.0) as timestamp with time zone)
            else cast(null as timestamp with time zone)
        end as external_modified_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_public_art_arcgis') }}
),

keyed_rows as (
    select
        cast(md5(to_json(struct_pack(
            source_name := source_name,
            feature_id := feature_id,
            observed_at := observed_at
        ))) as varchar) as public_art_arcgis_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed_rows
    qualify row_number() over (
        partition by public_art_arcgis_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    public_art_arcgis_key,
    source_name,
    feature_id,
    layer_url,
    observed_at,
    feature_type,
    geometry_type,
    geometry,
    longitude,
    latitude,
    artwork_identifier,
    title,
    category,
    material,
    discipline,
    status,
    artist,
    description,
    location_name,
    address,
    city,
    state,
    postal_code,
    neighborhood,
    website_url,
    image_url,
    external_modified_at,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
