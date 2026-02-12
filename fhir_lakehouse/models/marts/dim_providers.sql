WITH dim_providers AS (
    SELECT DISTINCT
        provider_id,
        provider_name,
    FROM {{ ref('int_claims') }}
)

SELECT 
    *
FROM dim_providers