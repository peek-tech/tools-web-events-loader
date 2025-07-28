{{ config(
    materialized='table',
    table_type='iceberg',
    tblproperties={
      'write.target-file-size-bytes': '67108864'
    }
) }}

-- Time Travel Analysis: Compare current vs historical data states
-- This model showcases Iceberg's time travel capabilities for data validation and analysis

WITH current_metrics AS (
  -- Current state of the data
  SELECT 
    'current' AS snapshot_type,
    CURRENT_TIMESTAMP AS snapshot_timestamp,
    event_date,
    space_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT anonymous_id) AS unique_users,
    AVG(engagement_score) AS avg_engagement_score,
    AVG(time_on_page_seconds) AS avg_time_on_page,
    COUNT(CASE WHEN is_core_virtual_tour_event THEN 1 END) AS core_tour_events,
    AVG(data_quality_score) AS avg_data_quality
  FROM {{ ref('fct_web_events_iceberg') }}
  WHERE event_date >= CURRENT_DATE - INTERVAL '7' DAY
  GROUP BY event_date, space_type
),

historical_metrics AS (
  -- Historical snapshot from 24 hours ago (if available)
  SELECT 
    'historical_24h' AS snapshot_type,
    CURRENT_TIMESTAMP - INTERVAL '1' DAY AS snapshot_timestamp,
    event_date,
    space_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT anonymous_id) AS unique_users,
    AVG(engagement_score) AS avg_engagement_score,
    AVG(time_on_page_seconds) AS avg_time_on_page,
    COUNT(CASE WHEN is_core_virtual_tour_event THEN 1 END) AS core_tour_events,
    AVG(data_quality_score) AS avg_data_quality
  FROM {{ ref('fct_web_events_iceberg') }}
  FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
  WHERE event_date >= CURRENT_DATE - INTERVAL '7' DAY
  GROUP BY event_date, space_type
),

comparison_metrics AS (
  -- Compare current vs historical
  SELECT 
    c.event_date,
    c.space_type,
    
    -- Current metrics
    c.total_events AS current_events,
    c.unique_sessions AS current_sessions,
    c.unique_users AS current_users,
    c.avg_engagement_score AS current_engagement,
    c.avg_time_on_page AS current_time_on_page,
    c.core_tour_events AS current_core_events,
    c.avg_data_quality AS current_quality,
    
    -- Historical metrics
    COALESCE(h.total_events, 0) AS historical_events,
    COALESCE(h.unique_sessions, 0) AS historical_sessions,
    COALESCE(h.unique_users, 0) AS historical_users,
    COALESCE(h.avg_engagement_score, 0) AS historical_engagement,
    COALESCE(h.avg_time_on_page, 0) AS historical_time_on_page,
    COALESCE(h.core_tour_events, 0) AS historical_core_events,
    COALESCE(h.avg_data_quality, 0) AS historical_quality,
    
    -- Change calculations
    c.total_events - COALESCE(h.total_events, 0) AS events_change,
    c.unique_sessions - COALESCE(h.unique_sessions, 0) AS sessions_change,
    c.unique_users - COALESCE(h.unique_users, 0) AS users_change,
    c.avg_engagement_score - COALESCE(h.avg_engagement_score, 0) AS engagement_change,
    c.avg_time_on_page - COALESCE(h.avg_time_on_page, 0) AS time_on_page_change,
    c.core_tour_events - COALESCE(h.core_tour_events, 0) AS core_events_change,
    c.avg_data_quality - COALESCE(h.avg_data_quality, 0) AS quality_change,
    
    -- Percentage changes (with null handling)
    CASE 
      WHEN COALESCE(h.total_events, 0) > 0 
      THEN (c.total_events - h.total_events) * 100.0 / h.total_events 
      ELSE NULL 
    END AS events_change_pct,
    
    CASE 
      WHEN COALESCE(h.unique_sessions, 0) > 0 
      THEN (c.unique_sessions - h.unique_sessions) * 100.0 / h.unique_sessions 
      ELSE NULL 
    END AS sessions_change_pct,
    
    CASE 
      WHEN COALESCE(h.avg_engagement_score, 0) > 0 
      THEN (c.avg_engagement_score - h.avg_engagement_score) * 100.0 / h.avg_engagement_score 
      ELSE NULL 
    END AS engagement_change_pct
    
  FROM current_metrics c
  LEFT JOIN historical_metrics h 
    ON c.event_date = h.event_date 
    AND c.space_type = h.space_type
),

anomaly_detection AS (
  -- Detect significant changes that might indicate data issues
  SELECT 
    *,
    
    -- Flag potential anomalies
    CASE 
      WHEN ABS(COALESCE(events_change_pct, 0)) > 50 THEN 'high_variance'
      WHEN ABS(COALESCE(events_change_pct, 0)) > 25 THEN 'medium_variance'
      ELSE 'normal_variance'
    END AS events_variance_flag,
    
    CASE 
      WHEN ABS(COALESCE(engagement_change_pct, 0)) > 30 THEN 'high_variance'
      WHEN ABS(COALESCE(engagement_change_pct, 0)) > 15 THEN 'medium_variance'
      ELSE 'normal_variance'
    END AS engagement_variance_flag,
    
    CASE 
      WHEN quality_change < -0.1 THEN 'quality_degradation'
      WHEN quality_change > 0.1 THEN 'quality_improvement'
      ELSE 'quality_stable'
    END AS quality_trend,
    
    -- Data freshness indicator
    CASE 
      WHEN events_change > 0 AND sessions_change > 0 THEN 'data_growing'
      WHEN events_change = 0 AND sessions_change = 0 THEN 'data_stale'
      WHEN events_change < 0 THEN 'data_decreasing'
      ELSE 'data_stable'
    END AS data_freshness_status
    
  FROM comparison_metrics
),

summary_analysis AS (
  -- Overall summary across all space types
  SELECT 
    event_date,
    'all_spaces' AS space_type,
    
    SUM(current_events) AS current_events,
    SUM(current_sessions) AS current_sessions,
    SUM(current_users) AS current_users,
    AVG(current_engagement) AS current_engagement,
    AVG(current_time_on_page) AS current_time_on_page,
    SUM(current_core_events) AS current_core_events,
    AVG(current_quality) AS current_quality,
    
    SUM(historical_events) AS historical_events,
    SUM(historical_sessions) AS historical_sessions,
    SUM(historical_users) AS historical_users,
    AVG(historical_engagement) AS historical_engagement,
    AVG(historical_time_on_page) AS historical_time_on_page,
    SUM(historical_core_events) AS historical_core_events,
    AVG(historical_quality) AS historical_quality,
    
    SUM(events_change) AS events_change,
    SUM(sessions_change) AS sessions_change,
    SUM(users_change) AS users_change,
    AVG(engagement_change) AS engagement_change,
    AVG(time_on_page_change) AS time_on_page_change,
    SUM(core_events_change) AS core_events_change,
    AVG(quality_change) AS quality_change,
    
    -- Overall change percentages
    CASE 
      WHEN SUM(historical_events) > 0 
      THEN SUM(events_change) * 100.0 / SUM(historical_events) 
      ELSE NULL 
    END AS events_change_pct,
    
    CASE 
      WHEN SUM(historical_sessions) > 0 
      THEN SUM(sessions_change) * 100.0 / SUM(historical_sessions) 
      ELSE NULL 
    END AS sessions_change_pct,
    
    -- Anomaly flags
    'summary' AS events_variance_flag,
    'summary' AS engagement_variance_flag,
    'summary' AS quality_trend,
    'summary' AS data_freshness_status
    
  FROM anomaly_detection
  GROUP BY event_date
)

-- Final union of detailed and summary results
SELECT * FROM anomaly_detection
UNION ALL
SELECT * FROM summary_analysis
ORDER BY event_date DESC, space_type