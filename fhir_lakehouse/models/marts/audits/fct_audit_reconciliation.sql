WITH bronze_counts AS (
    SELECT 
        resource_type,
        COUNT(*) AS bronze_record_count,
        COUNT(DISTINCT RAW_JSON:id::STRING) AS bronze_unique_ids
    FROM {{ source('BRONZE', 'FHIR_RAW') }}
    GROUP BY 1
),

gold_counts AS (
    SELECT 'Patient' AS resource_type, COUNT(*) AS gold_record_count FROM {{ ref('dim_patients') }}
    UNION ALL
    SELECT 'Observation', COUNT(*) FROM {{ ref('fct_observations') }}
    UNION ALL
    SELECT 'Encounter', COUNT(*) FROM {{ ref('fct_encounters') }}
    UNION ALL
    SELECT 'Condition', COUNT(*) FROM {{ ref('fct_conditions') }}
    UNION ALL
    SELECT 'Claim', COUNT(*) FROM {{ ref('fct_claims') }} 
    UNION ALL
    SELECT 'Procedure', COUNT(*) FROM {{ ref('fct_procedures') }}
),
final as(
SELECT 
    b.resource_type,
    b.bronze_record_count AS total_raw_messages,
    b.bronze_unique_ids AS total_unique_entities_raw,
    g.gold_record_count AS total_entities_in_gold,
    (b.bronze_unique_ids - g.gold_record_count) AS missing_records_gap
FROM bronze_counts b
LEFT JOIN gold_counts g ON b.resource_type = g.resource_type
WHERE TOTAL_ENTITIES_IN_GOLD IS NOT NULL
ORDER BY missing_records_gap DESC


)
SELECT * FROM final
