WITH fct_conditions AS(
    SELECT
        cond.condition_id,
        ptnt.patient_id,
        enct.encounter_id,
        cond.clinical_status,
        cond.verification_status,
        cond.condition_category,
        cond.condition_code,
        cond.condition_name,
        cond.condition_description,
        cond.onset_at,
        cond.resolved_at,
        cond.recorded_at
    FROM {{ ref('int_conditions') }} AS cond
    LEFT JOIN {{ ref('dim_patients') }} AS ptnt
    ON cond.patient_id = ptnt.patient_id
    LEFT JOIN {{ ref('fct_encounters') }} AS enct
    ON cond.encounter_id= enct.encounter_id
)

SELECT * FROM fct_conditions