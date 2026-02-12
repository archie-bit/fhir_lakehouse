WITH claim_summaries AS (
    SELECT
        claim_id,
        patient_id,
        provider_id, 
        insurance_plan,
        claim_status,
        claim_use,
        MAX(total_claim_amount) AS total_amount, 
        MAX(total_currency) as total_currency,
        MIN(created_at) AS claim_date,
        COUNT(item_sequence) AS total_items_on_claim
    FROM {{ ref('int_claims') }}
    GROUP BY 
        claim_id, patient_id, provider_id, insurance_plan, claim_status, claim_use
)

SELECT 
    s.*,
    pat.age_bucket,
    pat.patient_city,
    pro.provider_name
FROM claim_summaries s
LEFT JOIN {{ ref('dim_patients') }} pat 
    ON s.patient_id = pat.patient_id
LEFT JOIN {{ ref('dim_providers') }} pro
    ON s.provider_id = pro.provider_id