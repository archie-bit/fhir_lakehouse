WITH raw_procedures AS(
    SELECT 
        * 
    FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE resource_type= 'Procedure'
),
flattened_producres AS(
    SELECT
        RAW_JSON:id::STRING AS procedure_id,
        REGEXP_REPLACE(RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        REGEXP_REPLACE(RAW_JSON:encounter:reference::STRING, '^(urn:uuid:|Patient/)', '') AS encounter_id,
        RAW_JSON:status::STRING AS procedure_status,
        RAW_JSON:code:coding[0]:code::STRING AS procedure_code,
        RAW_JSON:code:coding[0]:display::STRING AS procedure_text,
        RAW_JSON:performedPeriod:start::TIMESTAMP_NTZ AS procedure_start,
        RAW_JSON:performedPeriod:end::TIMESTAMP_NTZ AS procedure_end,
        RAW_JSON:location:display::STRING AS procedure_location,
        INGESTED_AT as ingested_at
    FROM raw_procedures
)

SELECT * FROM flattened_producres