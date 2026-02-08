WITH claim_summaries AS (
    SELECT
        claim_id,
        patient_id,
        MAX(total_claim_amount) AS total_amount, 
        total_currency,
        claim_status,
        claim_use,
        provider_id,
        insurance_plan,
        MIN(created_at) AS claim_date,
        COUNT(item_sequence) AS total_items_on_claim
    FROM {{ ref('int_claims') }}
    GROUP BY 1, 2, 4, 5, 6, 7, 8
)

SELECT 
    s.*,
    pat.age_bucket,
    pat.patient_city
FROM claim_summaries s
LEFT JOIN {{ ref('dim_patients') }} pat 
    ON s.patient_id = pat.patient_id