WITH patient AS (
    SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE = 'Patient'
),
flattened_patients AS (
    SELECT
       RAW_JSON:id::STRING AS patient_id,
       ARRAY_TO_STRING(RAW_JSON:name[0]:given, ' ')::STRING AS patient_given_names,
    --    CONCAT(
    --         ARRAY_TO_STRING(RAW_JSON:name[0]:given, ' '), 
    --         ' ', 
    --         RAW_JSON:name[0]:family::STRING
    --     ) AS patient_fullname,
       RAW_JSON:gender::STRING AS patient_gender,
       RAW_JSON:extension[0]:extension[1]:valueString::STRING AS patient_race,
       RAW_JSON:birthDate::DATE AS patient_birthdate,
       YEAR(getdate()) - YEAR(RAW_JSON:birthDate::DATE) AS patient_age,
       RAW_JSON:telecom[0]:value::STRING AS patient_number,
       RAW_JSON:address[0]:city::STRING AS patient_city,
       RAW_JSON:address[0]:state::STRING AS patient_state,
       RAW_JSON:address[0]:country::STRING AS patient_country,
       RAW_JSON:maritalStatus:coding[0]:code::STRING AS patient_marital,
       
    FROM patient
),
test as (
    SELECT 
        TYPEOF(RAW_JSON), 
        RAW_JSON 
    FROM {{ source('BRONZE', 'FHIR_RAW') }}
    ORDER BY TYPEOF(RAW_JSON) desc
)
SELECT * FROM flattened_patients