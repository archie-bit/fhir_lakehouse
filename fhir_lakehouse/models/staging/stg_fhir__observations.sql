-- WITH observation AS(
--     SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
--     WHERE RESOURCE_TYPE ='Observation'
-- ),
-- flattened_observations AS(
--     SELECT 
--         RAW_JSON:id::STRING AS observation_id,
--         REGEXP_REPLACE(RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
--         REGEXP_REPLACE(RAW_JSON:encounter:reference::STRING, '^(urn:uuid:|Patient/)', '') AS encounter_id,
--         RAW_JSON:status::STRING AS observation_status,
--         RAW_JSON:code:coding[0]:code::STRING AS loinc_code,
--         RAW_JSON:code:text::STRING AS observation_code,
--         RAW_JSON:valueQuantity:value::FLOAT AS observation_value,
--         RAW_JSON:valueQuantity:unit::STRING AS observation_unit,
--         RAW_JSON:effectiveDateTime::TIMESTAMP_NTZ AS observation_at,
--         INGESTED_AT AS ingested_at

--     FROM observation obs,
--     LATERAL FLATTEN(input => obs.RAW_JSON:component) comp
-- )

-- SELECT * FROM flattened_observations



WITH observation_raw AS (
    SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE = 'Observation'
),

simple_obs AS (
    SELECT
        RAW_JSON:id::STRING AS observation_id,
        REGEXP_REPLACE(RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        REGEXP_REPLACE(RAW_JSON:encounter:reference::STRING, '^(urn:uuid:|Patient/)', '') AS encounter_id,
        RAW_JSON:effectiveDateTime::TIMESTAMP_NTZ AS observation_at,
        RAW_JSON:code:text::STRING AS observation_name,
        COALESCE(
            RAW_JSON:valueQuantity:value::STRING,
            RAW_JSON:valueCodeableConcept:text::STRING,
            RAW_JSON:valueCodeableConcept:coding[0]:display::STRING,
            RAW_JSON:valueString::STRING
        ) AS observation_value,
        RAW_JSON:valueQuantity:unit::STRING AS observation_unit,
        RAW_JSON:code:coding[0]:code::STRING AS loinc_code,
        'simple' AS extraction_type,
        INGESTED_AT AS ingested_at
    FROM observation_raw
    WHERE RAW_JSON:component IS NULL 
),

component_obs AS (
    SELECT
        obs.RAW_JSON:id::STRING AS observation_id,
        REGEXP_REPLACE(obs.RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        REGEXP_REPLACE(RAW_JSON:encounter:reference::STRING, '^(urn:uuid:|Patient/)', '') AS encounter_id,

        obs.RAW_JSON:effectiveDateTime::TIMESTAMP_NTZ AS observation_at,
        -- Use the component's text as the name (e.g., "Housing Status")
        COALESCE(
            comp.value:code:text::STRING,
            comp.value:code:coding[0]:display::STRING
        ) AS observation_name,
        -- Result logic inside the component
        COALESCE(
            comp.value:valueQuantity:value::STRING,
            comp.value:valueCodeableConcept:text::STRING,
            comp.value:valueCodeableConcept:coding[0]:display::STRING,
            comp.value:valueString::STRING
        ) AS observation_value,
        comp.value:valueQuantity:unit::STRING AS observation_unit,
        comp.value:code:coding[0]:code::STRING AS loinc_code,
        'component' AS extraction_type,
        INGESTED_AT AS ingested_at
    FROM observation_raw obs,
    LATERAL FLATTEN(input => obs.RAW_JSON:component) comp
    WHERE obs.RAW_JSON:component IS NOT NULL
),
combined_obs AS (
SELECT * FROM simple_obs
UNION ALL
SELECT * FROM component_obs)

-- test_compined AS (

-- SELECT * FROM (    
-- SELECT * FROM simple_obs
-- UNION ALL
-- SELECT * FROM component_obs
-- ) t WHERE extraction_type = 'component'

-- )

SELECT * FROM combined_obs