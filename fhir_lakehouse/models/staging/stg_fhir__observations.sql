WITH observation AS(
    SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE ='Observation'
),
flattened_observations AS(
    SELECT 
        RAW_JSON:id::STRING AS observation_id,
        REGEXP_REPLACE(RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        REGEXP_REPLACE(RAW_JSON:encounter:reference::STRING, '^(urn:uuid:|Patient/)', '') AS encounter_id,
        RAW_JSON:status::STRING AS observation_status,
        RAW_JSON:code:coding[0]:code::STRING AS loinc_code,
        RAW_JSON:code:text::STRING AS observation_code,
        RAW_JSON:valueQuantity:value::FLOAT AS observation_value,
        RAW_JSON:valueQuantity:unit::STRING AS observation_unit,
        RAW_JSON:effectiveDateTime::TIMESTAMP_NTZ AS observation_at,
    FROM observation
)

SELECT * FROM flattened_observations