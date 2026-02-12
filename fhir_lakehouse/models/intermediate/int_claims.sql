{{
    config(
        materialized= 'incremental',
        unique_key= ['claim_id', 'item_sequence'],
        incremental_strategy= 'merge'
    )
}}

WITH deduped_claims AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['claim_id', 'item_sequence']) }} AS claim_item_sk,
        *
     FROM {{ ref('stg_fhir__claims') }}
    {% if is_incremental() %}
    WHERE ingested_at >= (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY claim_id, item_sequence 
        ORDER BY ingested_at DESC
    ) = 1
)

SELECT * FROM deduped_claims