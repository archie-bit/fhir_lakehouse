WITH claim_items AS (
    SELECT * FROM {{ ref('int_claims') }}
),

final AS (
    SELECT
        cli.claim_item_sk,
        cli.claim_id,
        cli.item_sequence,
        cli.patient_id,
        cli.encounter_id,
        cli.claim_status,
        cli.service_description,
        cli.total_claim_amount AS header_total_reference, 
        cli.total_currency,
        cli.created_at
    FROM claim_items cli
    LEFT JOIN {{ ref('dim_patients') }} pat 
        ON cli.patient_id = pat.patient_id
    LEFT JOIN {{ ref('fct_encounters') }} enc 
        ON cli.encounter_id = enc.encounter_id
)

SELECT * FROM final