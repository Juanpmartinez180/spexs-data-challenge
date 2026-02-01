{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

WITH app_usage AS(
    SELECT 
        *,
        'app_usage' as data_source_type
        FROM {{ ref('app_usage_trips') }}
        WHERE is_deprecated = false
),
car_navigation AS(
    SELECT 
        *,
        'car_navigation' as data_source_type
        FROM {{ ref('car_navigation_trips') }}
        WHERE is_deprecated = false
),
cell_phone AS(
    SELECT 
        *,
        'cell_phone' as data_source_type
        FROM {{ ref('cell_phone_trips') }}
        WHERE is_deprecated = false
),

unioned AS(
    SELECT * FROM app_usage
    UNION ALL
    SELECT* FROM car_navigation
    UNION ALL
    SELECT * FROM cell_phone
)

SELECT
    md5(region || origin_coord || destination_coord || data_source_type) as trip_id,
    region as region_name,
    origin_coord,
    destination_coord,
    datetime::timestamp,
    data_source_type,
    inserted_at,
    {{ current_timestamp() }} as processed_at
FROM unioned
{% if is_incremental() %}
    -- Filtro para optimizar la carga incremental
    WHERE inserted_at > (SELECT max(inserted_at) FROM {{ this }})
{% endif %}