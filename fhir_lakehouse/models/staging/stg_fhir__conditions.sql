WITH conditions AS(
    SELECT
        *
    FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE ='Condition'
),
flattened_conditions AS(
    SELECT
        RAW_JSON:id::STRING AS condition_id,
        REGEXP_REPLACE(RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        REGEXP_REPLACE(RAW_JSON:encounter:reference::STRING, '^(urn:uuid:|Patient/)', '') AS encounter_id,
        RAW_JSON:clinicalStatus:coding[0]:code::STRING AS clinical_status,
        RAW_JSON:verificationStatus:coding[0]:code::STRING AS verification_status,
        RAW_JSON:category[0]:coding[0]:code::STRING AS condition_category,
        RAW_JSON:code:coding[0]:code::STRING AS condition_code,
        RAW_JSON:code:coding[0]:display::STRING AS condition_name,
        COALESCE(RAW_JSON:code:text::STRING, RAW_JSON:code:coding[0]:display::STRING) AS condition_description,
        RAW_JSON:onsetDateTime::TIMESTAMP_NTZ AS onset_at,
        RAW_JSON:abatementDateTime::TIMESTAMP_NTZ AS resolved_at,
        RAW_JSON:recordedDate::TIMESTAMP_NTZ AS recorded_at,
        INGESTED_AT AS ingested_at
    FROM conditions
)

SELECT * FROM flattened_conditions