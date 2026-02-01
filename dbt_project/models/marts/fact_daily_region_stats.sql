{{ config(materialized='table') }}

SELECT 
    region_name,
    datetime::date as trip_date,
    COUNT(trip_id) as total_trips,
    AVG(COUNT(trip_id)) OVER(PARTITION BY region_name) as avg_trips_per_region,
    {{ current_timestamp() }} as processed_at
FROM {{ ref('trips') }}
GROUP BY 1, 2