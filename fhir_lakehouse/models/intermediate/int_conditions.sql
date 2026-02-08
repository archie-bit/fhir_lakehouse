{{
    config(
        materialized= 'incremental',
        unique_key= 'condition_id',
        incremental_strategy= 'merge'
    )
}}

WITH raw_conditions AS(
    SELECT * FROM {{ ref('stg_fhir__conditions') }}
    {%if dbt.is_incremental()%}
        where ingested_at >= (SELECT COALESCE(max(ingested_at), '1900-01-01') from {{ this }})
    {%endif%}
),
deduplicated_conditions AS(
    SELECT
        *
    FROM raw_conditions
    QUALIFY ROW_NUMBER() OVER(PARTITION BY condition_id ORDER BY ingested_at DESC) = 1
)
SELECT * FROM deduplicated_conditions