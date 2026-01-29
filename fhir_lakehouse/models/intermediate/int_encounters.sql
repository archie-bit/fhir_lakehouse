{{
    config(
        materialized= 'incremental',
        unique_key= 'encounter_id',
        incremental_strategy= 'merge'
    )
}}

WITH raw_encounters AS(
    SELECT * FROM {{ ref('stg_fhir__encounters') }}
    {%if dbt.is_incremental()%}
        where ingested_at >= (SELECT COALESCE(max(ingested_at), '1900-01-01') from {{ this }})
    {%endif%}
),
 deduplicated_encounters AS(
    SELECT 
        *,
    FROM raw_encounters
    QUALIFY ROW_NUMBER() OVER(PARTITION BY encounter_id ORDER BY ingested_at DESC) = 1 
)
SELECT * FROM deduplicated_encounters
