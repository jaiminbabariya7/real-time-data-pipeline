-- stg_events.sql
-- Clean and type-cast raw events from BigQuery streaming insert table.
-- Deduplicates on event_id and normalises timestamps.

WITH source AS (
    SELECT * FROM {{ source('streaming_etl', 'events') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY processed_at DESC) AS rn
    FROM source
    WHERE is_valid = TRUE
),

final AS (
    SELECT
        event_id,
        UPPER(TRIM(event_type))             AS event_type,
        TRIM(user_id)                        AS user_id,
        TRIM(session_id)                     AS session_id,
        TIMESTAMP(event_ts)                  AS event_ts,
        DATE(event_ts)                       AS event_date,
        SAFE.PARSE_JSON(properties)          AS properties,
        COALESCE(country, 'UNKNOWN')         AS country,
        LOWER(COALESCE(platform, 'web'))     AS platform,
        processed_at,
        window_start,
        window_end,
        CURRENT_TIMESTAMP()                  AS _dbt_loaded_at
    FROM deduped
    WHERE rn = 1
      AND event_ts IS NOT NULL
      AND user_id IS NOT NULL
)

SELECT * FROM final
