{{
    config(
        materialized= 'incremental',
        unique_key= 'procedure_id', 
        incremental_strategy= 'merge'
    )
}}

WITH raw_procedures AS(
    SELECT
        {{ dbt_utils.generate_surrogate_key(['procedure_id', 'procedure_code']) }} AS procedure_item_sk, 
        * 
    FROM {{ ref('stg_fhir__procedures') }}
    {% if is_incremental() %}
        where ingested_at >= (SELECT COALESCE(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
),
deduplicated_procedures AS(
    SELECT 
    *,
    DATEDIFF(minute,procedure_start, procedure_end) AS procedure_duration
    FROM raw_procedures
    QUALIFY ROW_NUMBER() OVER(PARTITION BY procedure_id ORDER BY ingested_at DESC) = 1
)
SELECT * FROM deduplicated_procedures