{{ config(
    materialized='incremental',
    pre_hook=[
        "{% if is_incremental() %}
        -- antes de insertar lo nuevo, buscamos los hashes que estan por ser agregados
        -- y marcamos las versiones anteriores como deprecadas
        UPDATE {{this}}
        SET is_deprecated = true
        WHERE trip_id_hash IN (
            SELECT DISTINCT md5(region || origin_coord || destination_coord || datetime)
            FROM {{ source('bronze', 'app_usage_data') }}
        )
        AND is_deprecated = false;
        {% endif %}"
    ]
) }}

WITH new_data AS (
    SELECT 
        md5(region || origin_coord || destination_coord || datetime) as trip_id_hash,
        region,
        origin_coord,
        destination_coord,
        datetime,
        datasource,
        {{ current_timestamp() }} as inserted_at,
        false as is_deprecated 
    FROM {{ source('bronze', 'app_usage_data') }}
)

SELECT * FROM new_data