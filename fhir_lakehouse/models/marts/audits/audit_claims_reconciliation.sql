WITH bronze_stats AS (
    SELECT 
        'Claims' AS resource_type,
        COUNT(*) AS raw_record_count,
        SUM((RAW_JSON:total:value)::FLOAT) AS total_dollars_raw
    FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE = 'Claim'
),

gold_stats AS (
    SELECT 
        'Claims' AS resource_type,
        COUNT(DISTINCT claim_id) AS unique_claim_count,
        SUM(total_amount) AS total_dollars_gold
    FROM {{ ref('fct_claims') }}
),
final AS (SELECT 
    b.resource_type,
    b.raw_record_count,
    g.unique_claim_count,
    (b.raw_record_count - g.unique_claim_count) AS records_filtered_out_as_dups,
    b.total_dollars_raw,
    g.total_dollars_gold,
    (b.total_dollars_raw - g.total_dollars_gold) AS dollar_variance
FROM bronze_stats b
JOIN gold_stats g ON b.resource_type = g.resource_type)

SELECT * FROM final