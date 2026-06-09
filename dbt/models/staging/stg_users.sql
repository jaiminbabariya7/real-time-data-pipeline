-- stg_users.sql
-- Derives a user spine from the events stream.
-- One row per user_id with first/last seen timestamps.

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

user_activity AS (
    SELECT
        user_id,
        MIN(event_ts)    AS first_seen_at,
        MAX(event_ts)    AS last_seen_at,
        COUNT(*)         AS total_events,
        COUNT(DISTINCT session_id)                AS total_sessions,
        COUNTIF(event_type = 'PURCHASE')          AS total_purchases,
        COUNTIF(event_type = 'ADD_TO_CART')       AS total_cart_adds,
        COUNT(DISTINCT DATE(event_ts))            AS active_days,
        ARRAY_AGG(DISTINCT country IGNORE NULLS ORDER BY country) AS countries,
        ARRAY_AGG(DISTINCT platform IGNORE NULLS ORDER BY platform) AS platforms
    FROM events
    GROUP BY user_id
)

SELECT
    user_id,
    first_seen_at,
    last_seen_at,
    total_events,
    total_sessions,
    total_purchases,
    total_cart_adds,
    active_days,
    countries[SAFE_OFFSET(0)]   AS primary_country,
    platforms[SAFE_OFFSET(0)]   AS primary_platform,
    CURRENT_TIMESTAMP()         AS _dbt_loaded_at
FROM user_activity
