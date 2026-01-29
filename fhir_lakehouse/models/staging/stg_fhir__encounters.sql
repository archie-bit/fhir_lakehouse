WITH encounter AS(
    SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE ='Encounter'
),
encounter_flattened AS(
    SELECT
        RAW_JSON:id::STRING AS encounter_id,
        REGEXP_REPLACE(RAW_JSON:subject:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        RAW_JSON:type[0]:coding[0]:code::STRING AS encounter_type_code,
        RAW_JSON:type[0]:text::STRING AS encounter_type,
        RAW_JSON:status::STRING AS encounter_status,
        RAW_JSON:participant[0]:individual:display::STRING AS encounter_participant,
        RAW_JSON:period:start::TIMESTAMP_NTZ AS encounter_start,
        RAW_JSON:period:end::TIMESTAMP_NTZ AS encounter_end,
        RAW_JSON:location[0]:location:display::STRING AS encounter_location,

    FROM encounter
)

SELECT * FROM encounter_flattened