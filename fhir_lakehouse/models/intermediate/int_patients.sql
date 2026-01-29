{{
    config(
        materialized= 'incremental',
        unique_key= 'patient_id',
        incremental_strategy= 'merge'
    )
}}

WITH raw_patients AS(
    SELECT * FROM {{ ref('stg_fhir__patients') }}

    {%if dbt.is_incremental()%}
        where ingested_at >= (SELECT COALESCE(max(ingested_at), '1900-01-01') from {{ this }})
    {%endif%}
),

deduplicated_patients AS (
    SELECT
        *,
    FROM raw_patients
    QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY ingested_at DESC) = 1
)

SELECT * FROM deduplicated_patients