{{ config(materialized='table') }}

SELECT 
    region_name,
    EXTRACT(WEEK FROM datetime) as week_number,
    EXTRACT(YEAR FROM datetime) as year,
    COUNT(*) / 7.0 as avg_trips_daily,
    {{ current_timestamp() }} as processed_at
FROM {{ ref('trips') }}
GROUP BY 1, 2, 3