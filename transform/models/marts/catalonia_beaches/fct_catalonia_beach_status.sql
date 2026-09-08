{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := cast(source as varchar),
                        beach_id := cast(id as varchar),
                        status_updated_at := (
                            try_strptime(estat_data, '%d/%m/%YT%H:%M:%S.%fZ')
                            at time zone 'UTC'
                        )
                )
                )
            ) as varchar
        ) as beach_status_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as beach_id,
        cast(codiplatja as varchar) as beach_code,
        cast(platja as varchar) as beach_name,
        cast(municipi_codimunicipi as varchar) as municipality_code,
        cast(municipi_municipi as varchar) as municipality_name,
        cast(costa_id as varchar) as coast_id,
        cast(municipi_costa as varchar) as coast_name,
        try_cast(coordenada_x as double) as coordinate_x,
        try_cast(coordenada_y as double) as coordinate_y,
        (
            try_strptime(estat_data, '%d/%m/%YT%H:%M:%S.%fZ')
            at time zone 'UTC'
        ) as status_updated_at,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(temps_data as timestamp with time zone) as weather_date,
        cast(temps_celmati as varchar) as morning_weather,
        cast(temps_celtarda as varchar) as afternoon_weather,
        cast(temps_ventmati_d as varchar) as morning_wind_direction,
        try_cast(temps_ventmati_v as double) as morning_wind_speed,
        cast(temps_venttarda_d as varchar) as afternoon_wind_direction,
        try_cast(temps_vent_tarda_v as double) as afternoon_wind_speed,
        try_cast(temps_temperaturamax as double) as max_temperature,
        try_cast(temps_temperaturamin as double) as min_temperature,
        try_cast(temps_sensmax as double) as max_feels_like,
        try_cast(temps_sensmin as double) as min_feels_like,
        try_cast(temps_uv as bigint) as uv_index,
        try_cast(temps_maraigua as double) as water_temperature,
        cast(temps_mardireccio as varchar) as sea_direction,
        nullif(try_cast(temps_maralcada as double), -9999) as wave_height,
        cast(estat_bandera as varchar) as beach_flag,
        cast(estat_motiubandera as varchar) as beach_flag_reason,
        cast(estat_meteorologia as varchar) as meteorology,
        cast(estat_transparenciaaigua as varchar) as water_transparency,
        cast(estat_estatmar as varchar) as sea_state,
        try_cast(estat_temperatura as double) as beach_temperature,
        cast(estat_meduses as varchar) as jellyfish_status,
        cast(codi_boia as varchar) as buoy_code,
        try_cast(temps_precipitaciomati as double) as morning_precipitation,
        try_cast(temps_precipitaciotarda as double) as afternoon_precipitation,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_catalonia_beaches') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by beach_status_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    beach_status_key,
    source_name,
    beach_id,
    beach_code,
    beach_name,
    municipality_code,
    municipality_name,
    coast_id,
    coast_name,
    coordinate_x,
    coordinate_y,
    status_updated_at,
    observed_at,
    weather_date,
    morning_weather,
    afternoon_weather,
    morning_wind_direction,
    morning_wind_speed,
    afternoon_wind_direction,
    afternoon_wind_speed,
    max_temperature,
    min_temperature,
    max_feels_like,
    min_feels_like,
    uv_index,
    water_temperature,
    sea_direction,
    wave_height,
    beach_flag,
    beach_flag_reason,
    meteorology,
    water_transparency,
    sea_state,
    beach_temperature,
    jellyfish_status,
    buoy_code,
    morning_precipitation,
    afternoon_precipitation,
    _row_id,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
