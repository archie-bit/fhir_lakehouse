WITH dim_patients AS(
    SELECT
        patient_id,
        patient_given_names AS patient_name,
        patient_gender,
        patient_birthdate,
        DATEDIFF(year, patient_birthdate, CURRENT_DATE()) AS patient_age,
        case 
            when patient_age <18 then 'Under 18'
            when patient_age between 18 and 24 then '18-24'
            when patient_age between 25 and 34 then '25-34'
            when patient_age between 35 and 44 then '35-44'
            when patient_age between 45 and 54 then '45-54'
            when patient_age between 55 and 64 then '55-64'
            else 'Above 65'
        END as age_bucket,
        patient_race,
        patient_number,
        patient_city,
        patient_state,
        patient_country,
        patient_marital

    FROM {{ ref('int_patients') }}
)
SELECT * from dim_patients