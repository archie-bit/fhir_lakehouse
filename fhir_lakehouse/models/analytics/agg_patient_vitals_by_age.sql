WITH patient_vitals_by_age AS(
    SELECT
        ptnt.age_bucket,
        ptnt.patient_gender,
        obs.observation_name,
        AVG(obs.observation_value) as avg_value,
        obs.observation_unit,
        COUNT(DISTINCT obs.patient_id) as total_patients
    FROM {{ ref('fct_observations') }} obs
    LEFT JOIN {{ ref('dim_patients') }} ptnt
    ON obs.patient_id = ptnt.patient_id
    WHERE loinc_code IN ('8462-4', '8867-4', '8867-4', '8480-6',
                         '5354-9', '85354-9', '38208-5', '8302-2',
                         '72514-3', '29463-7')
    GROUP BY 1, 2, 3, 5
    ORDER BY 1, 2 DESC
)
SELECT * FROM patient_vitals_by_age