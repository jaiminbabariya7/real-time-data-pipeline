-- dim_users.sql
-- User dimension table with lifetime engagement metrics and segments.

WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

purchase_stats AS (
    SELECT
        user_id,
        SUM(purchase_amount_usd)   AS lifetime_value_usd,
        COUNT(DISTINCT order_id)   AS total_orders,
        AVG(purchase_amount_usd)   AS avg_order_value_usd,
        MAX(event_ts)              AS last_purchase_at
    FROM {{ ref('fct_events') }}
    WHERE event_type = 'PURCHASE'
      AND purchase_amount_usd IS NOT NULL
    GROUP BY user_id
)

SELECT
    u.user_id,
    u.first_seen_at,
    u.last_seen_at,
    u.total_events,
    u.total_sessions,
    u.total_purchases,
    u.active_days,
    u.primary_country,
    u.primary_platform,

    -- LTV and purchase stats
    COALESCE(ps.lifetime_value_usd, 0)  AS lifetime_value_usd,
    COALESCE(ps.total_orders, 0)        AS total_orders,
    COALESCE(ps.avg_order_value_usd, 0) AS avg_order_value_usd,
    ps.last_purchase_at,

    -- User tier segment
    CASE
        WHEN COALESCE(ps.lifetime_value_usd, 0) >= 5000 THEN 'platinum'
        WHEN COALESCE(ps.lifetime_value_usd, 0) >= 1000 THEN 'gold'
        WHEN COALESCE(ps.lifetime_value_usd, 0) >= 200  THEN 'silver'
        WHEN COALESCE(ps.total_orders, 0) > 0            THEN 'bronze'
        ELSE 'prospect'
    END AS user_segment,

    -- Engagement tier
    CASE
        WHEN u.active_days >= 30 THEN 'power_user'
        WHEN u.active_days >= 7  THEN 'regular'
        WHEN u.active_days >= 2  THEN 'occasional'
        ELSE 'one_time'
    END AS engagement_tier,

    CURRENT_TIMESTAMP() AS _dbt_updated_at
FROM users u
LEFT JOIN purchase_stats ps USING (user_id)
