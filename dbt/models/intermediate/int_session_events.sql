-- int_session_events.sql
-- Aggregates events to the session level.
-- One row per (user_id, session_id) with engagement metrics.

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    user_id,
    session_id,
    MIN(event_ts)                           AS session_start,
    MAX(event_ts)                           AS session_end,
    TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND)
                                            AS session_duration_secs,
    COUNT(*)                                AS event_count,
    COUNTIF(event_type = 'PAGE_VIEW')       AS page_views,
    COUNTIF(event_type = 'ADD_TO_CART')     AS cart_adds,
    COUNTIF(event_type = 'PURCHASE')        AS purchases,
    COUNTIF(event_type = 'SEARCH')          AS searches,
    MAX(CASE WHEN event_type = 'PURCHASE'
             THEN CAST(JSON_VALUE(properties,'$.total_usd') AS FLOAT64)
             END)                           AS purchase_amount_usd,
    ANY_VALUE(country)                      AS country,
    ANY_VALUE(platform)                     AS platform,
    CURRENT_TIMESTAMP()                     AS _dbt_loaded_at
FROM events
GROUP BY user_id, session_id
