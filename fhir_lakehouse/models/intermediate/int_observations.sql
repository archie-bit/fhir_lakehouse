{{
    config(
        materialized= 'incremental',
        unique_key= 'observation_id',
        incremental_strategy= 'merge'
    )
}}

WITH raw_observations AS(
    SELECT * FROM {{ ref('stg_fhir__observations') }}
    {%if dbt.is_incremental()%}
        where ingested_at >= (SELECT COALESCE(max(ingested_at), '1900-01-01') from {{ this }})
    {%endif%}
),
 deduplicated_observations AS(
    SELECT 
        *,
    FROM raw_observations
    QUALIFY ROW_NUMBER() OVER(PARTITION BY observation_id ORDER BY ingested_at DESC) = 1 
)
SELECT * FROM deduplicated_observations
