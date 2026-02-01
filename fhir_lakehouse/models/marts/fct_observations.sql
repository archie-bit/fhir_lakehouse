WITH fact_observations AS(
    SELECT
        obs.observation_id,
        ptnt.patient_id,
        ptnt.age_bucket,
        ptnt.patient_city,
        obs.loinc_code,
        obs.observation_name,
        obs.observation_value,
        obs.observation_unit,
        obs.observation_at,
        obs.extraction_type
    FROM {{ ref('int_observations') }} AS obs
    LEFT JOIN {{ ref('dim_patients') }} AS ptnt
    ON obs.patient_id = ptnt.patient_id
)
SELECT * FROM fact_observations
