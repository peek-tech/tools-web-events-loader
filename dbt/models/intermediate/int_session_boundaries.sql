{{ config(materialized='view') }}

WITH session_events AS (
  SELECT *,
    LAG(event_timestamp) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS prev_event_timestamp,
    
    LEAD(event_timestamp) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS next_event_timestamp
    
  FROM {{ ref('stg_raw_web_events') }}
),

session_with_boundaries AS (
  SELECT 
    *,
    -- First event in session
    prev_event_timestamp IS NULL AS is_session_start,
    
    -- Last event in session  
    next_event_timestamp IS NULL AS is_session_end,
    
    -- Time between events (for engagement analysis)
    COALESCE(
      EXTRACT(EPOCH FROM (next_event_timestamp - event_timestamp)),
      30 -- Default 30 seconds for last event
    ) AS seconds_to_next_event,
    
    -- Session position metrics
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS session_event_rank,
    COUNT(*) OVER (PARTITION BY session_id) AS total_session_events,
    
    -- Journey progression
    CASE 
      WHEN session_event_sequence = 1 THEN 'entry'
      WHEN next_event_timestamp IS NULL THEN 'exit'
      ELSE 'progression'
    END AS journey_stage
    
  FROM session_events
)

SELECT * FROM session_with_boundaries