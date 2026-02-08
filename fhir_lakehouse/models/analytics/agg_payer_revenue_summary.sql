WITH final AS (SELECT
    insurance_plan,
    claim_status,
    DATE_TRUNC('month', claim_date) AS billing_month,
    COUNT(claim_id) AS claim_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_claim,
    SUM(total_items_on_claim) AS total_services_billed
FROM {{ ref('fct_claims') }}
GROUP BY 1, 2, 3
ORDER BY billing_month DESC, total_revenue DESC)

SELECT * FROM final