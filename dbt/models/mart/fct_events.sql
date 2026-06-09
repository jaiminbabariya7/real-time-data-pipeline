-- fct_events.sql
-- Event fact table — one row per event, enriched with session context.
-- Partitioned by event_date, clustered by event_type and country.

{{
  config(
    materialized  = "incremental",
    unique_key    = "event_id",
    partition_by  = {"field": "event_date", "data_type": "date"},
    cluster_by    = ["event_type", "country"],
    on_schema_change = "sync_all_columns"
  )
}}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    {% if is_incremental() %}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

sessions AS (
    SELECT * FROM {{ ref('int_session_events') }}
)

SELECT
    e.event_id,
    e.event_type,
    e.user_id,
    e.session_id,
    e.event_ts,
    e.event_date,
    e.country,
    e.platform,

    -- Session context
    s.session_duration_secs,
    s.event_count          AS session_event_count,
    s.purchases            AS session_purchases,

    -- Purchase-specific
    CAST(JSON_VALUE(e.properties, '$.total_usd')  AS FLOAT64) AS purchase_amount_usd,
    JSON_VALUE(e.properties, '$.order_id')                    AS order_id,
    JSON_VALUE(e.properties, '$.payment_method')              AS payment_method,
    JSON_VALUE(e.properties, '$.purchase_tier')               AS purchase_tier,

    -- Product-specific
    JSON_VALUE(e.properties, '$.product_id')                  AS product_id,
    CAST(JSON_VALUE(e.properties, '$.quantity') AS INT64)     AS cart_quantity,

    -- Search-specific
    JSON_VALUE(e.properties, '$.query')                       AS search_query,

    e.processed_at,
    e._dbt_loaded_at
FROM events e
LEFT JOIN sessions s USING (user_id, session_id)
