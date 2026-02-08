WITH fct_encounters AS(
    SELECT
        encounter_id,
        patient_id,
        encounter_type_code,
        encounter_type,
        encounter_status,
        encounter_reason_code,
        encounter_reason,
        encounter_participant,
        encounter_start,
        encounter_end,
        DATEDIFF(minute, encounter_start, encounter_end)/60 AS stay_duration_hours,
        encounter_location
    FROM {{ ref('int_encounters') }}
)

SELECT * FROM fct_encounters