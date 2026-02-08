-- WITH raw_claims AS(
--     SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
--     WHERE RESOURCE_TYPE= 'Claim'
-- ),
-- flattened_claims AS(
--     SELECT
--         RAW_JSON:id::STRING AS claim_id,
--         REGEXP_REPLACE(RAW_JSON:patient:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
--         RAW_JSON:status::STRING AS claim_status,
--         RAW_JSON:type:coding[0]:code::STRING AS claim_type,
--         RAW_JSON:use::STRING AS claim_use,
--         RAW_JSON:billablePeriod:start::TIMESTAMP_NTZ AS claim_bill_start,
--         RAW_JSON:billablePeriod:end::TIMESTAMP_NTZ AS claim_bill_end,

--     FROM raw_claims
-- )


-- SELECT * FROM raw_claims




WITH raw_claims AS (
    SELECT * FROM {{ source('BRONZE', 'FHIR_RAW') }}
    WHERE RESOURCE_TYPE = 'Claim'
),

flattened_items AS (
    SELECT
        RAW_JSON:id::STRING AS claim_id,
        REGEXP_REPLACE(RAW_JSON:patient:reference::STRING, '^(urn:uuid:|Patient/)', '') AS patient_id,
        COALESCE(
        REGEXP_REPLACE(item.value:encounter[0]:reference::STRING, '^(urn:uuid:|Encounter/)', ''), -- Item level
        REGEXP_REPLACE(RAW_JSON:item[0]:encounter[0]:reference::STRING, '^(urn:uuid:|Encounter/)', '') -- Fallback to first item's encounter
        ) AS encounter_id,
        REGEXP_REPLACE(RAW_JSON:provider:reference::STRING, '.*\\|', '') AS provider_id,
        RAW_JSON:provider:display::STRING AS provider_name, 
        RAW_JSON:insurance[0]:coverage:display::STRING AS insurance_plan,
        RAW_JSON:status::STRING AS claim_status,
        RAW_JSON:use::STRING AS claim_use,
        RAW_JSON:total:value::FLOAT AS total_claim_amount,
        RAW_JSON:total:currency::STRING AS total_currency,
        RAW_JSON:created::TIMESTAMP_NTZ AS created_at,       
        item.value:sequence::INT AS item_sequence,
        item.value:productOrService:text::STRING AS service_description,
        INGESTED_AT
    FROM raw_claims,
    LATERAL FLATTEN(input => RAW_JSON:item) item
)

SELECT * FROM flattened_items