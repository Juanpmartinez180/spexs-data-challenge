SELECT
    *
FROM
    {{ ref('trips') }}
WHERE datetime > current_timestamp