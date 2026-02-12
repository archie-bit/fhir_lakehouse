WITH fct_procedures AS(
    SELECT
        prod.procedure_item_sk,
        prod.procedure_id,
        ptnt.patient_id,
        enct.encounter_id,
        prod.procedure_status,
        prod.procedure_code,
        prod.procedure_text,
        prod.procedure_start,
        prod.procedure_end,
        prod.procedure_duration
    FROM {{ ref('int_procedures') }} prod
    LEFT JOIN {{ ref('dim_patients') }} ptnt
    ON ptnt.patient_id = prod.patient_id
    LEFT JOIN {{ ref('fct_encounters') }} AS enct
    ON prod.encounter_id= enct.encounter_id
)

SELECT * FROM fct_procedures