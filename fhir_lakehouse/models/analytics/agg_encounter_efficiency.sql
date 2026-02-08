with final as (SELECT
    encounter_location,
    encounter_type,
    DATE_TRUNC('day', encounter_start) AS encounter_date,
    COUNT(encounter_id) AS total_visits,
    AVG(stay_duration_hours) AS avg_stay_duration,
    CASE 
        WHEN COUNT(encounter_id) > 50 THEN 'High Volume'
        WHEN COUNT(encounter_id) BETWEEN 20 AND 50 THEN 'Normal'
        ELSE 'Low Volume'
    END AS volume_category
FROM {{ ref('fct_encounters') }}
GROUP BY 1, 2, 3)

SELECT * from final