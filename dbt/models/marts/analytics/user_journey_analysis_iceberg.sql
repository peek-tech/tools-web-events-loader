{{ config(
    materialized='table',
    table_type='iceberg',
    tblproperties={
      'write.target-file-size-bytes': '67108864',
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'zstd'
    },
    partitioned_by=['analysis_date']
) }}

-- Enhanced user journey analysis leveraging Iceberg time travel and ACID properties
WITH daily_sessions AS (
  SELECT 
    event_date AS analysis_date,
    session_id,
    anonymous_id,
    MIN(event_timestamp) AS session_start,
    MAX(event_timestamp) AS session_end,
    COUNT(*) AS total_events,
    COUNT(DISTINCT space_id) AS unique_spaces_viewed,
    SUM(time_on_page_seconds) AS total_time_on_site_seconds,
    
    -- Enhanced journey path construction using Iceberg array functions
    ARRAY_AGG(
      STRUCT(
        session_event_sequence,
        page_path,
        space_type,
        journey_milestone,
        engagement_score,
        event_timestamp
      ) 
      ORDER BY session_event_sequence
    ) AS journey_sequence,
    
    -- Journey characteristics (enhanced)
    MAX(exploration_depth) AS max_exploration_depth,
    SUM(engagement_score) AS total_engagement_score,
    AVG(engagement_score) AS avg_engagement_score,
    
    -- Entry and exit analysis (enhanced)
    FIRST_VALUE(referrer) OVER (
      PARTITION BY session_id 
      ORDER BY session_event_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS entry_referrer,
    
    FIRST_VALUE(page_path) OVER (
      PARTITION BY session_id 
      ORDER BY session_event_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS entry_page,
    
    LAST_VALUE(page_path) OVER (
      PARTITION BY session_id 
      ORDER BY session_event_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS exit_page,
    
    -- Virtual tour specific metrics (enhanced)
    COUNT(CASE WHEN is_virtual_tour_page THEN 1 END) AS virtual_tour_events,
    COUNT(CASE WHEN has_space_context THEN 1 END) AS space_context_events,
    COUNT(CASE WHEN is_core_virtual_tour_event THEN 1 END) AS core_tour_events,
    
    -- Space type engagement pattern
    ARRAY_AGG(DISTINCT space_type) FILTER (WHERE space_type IS NOT NULL) AS space_types_engaged,
    
    -- Time-based patterns
    FIRST_VALUE(time_period) OVER (
      PARTITION BY session_id 
      ORDER BY session_event_sequence
    ) AS session_time_period,
    
    -- Device and location (enhanced)
    FIRST_VALUE(device_type) OVER (
      PARTITION BY session_id 
      ORDER BY session_event_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS device_type,
    
    FIRST_VALUE(detailed_region) OVER (
      PARTITION BY session_id 
      ORDER BY session_event_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS detailed_region,
    
    -- Data quality indicators
    AVG(data_quality_score) AS avg_data_quality_score,
    MIN(data_quality_score) AS min_data_quality_score
    
  FROM {{ ref('fct_web_events_iceberg') }}
  WHERE event_date >= CURRENT_DATE - INTERVAL '30' DAY
    AND data_quality_score >= 0.7
  GROUP BY 
    event_date, session_id, anonymous_id
),

journey_classification AS (
  SELECT 
    *,
    
    -- Session duration in minutes (enhanced)
    DATE_DIFF('second', session_start, session_end) / 60.0 AS session_duration_minutes,
    
    -- Journey type classification (enhanced for virtual tours)
    CASE 
      WHEN virtual_tour_events = 0 THEN 'non_tour_session'
      WHEN core_tour_events >= 10 AND unique_spaces_viewed >= 5 THEN 'comprehensive_tour'
      WHEN core_tour_events >= 5 AND unique_spaces_viewed >= 3 THEN 'extensive_tour'
      WHEN unique_spaces_viewed >= 2 THEN 'multi_space_tour' 
      WHEN space_context_events > 0 THEN 'single_space_tour'
      ELSE 'tour_navigation_only'
    END AS journey_type,
    
    -- Engagement classification (enhanced)
    CASE 
      WHEN total_engagement_score >= 25 THEN 'extremely_engaged'
      WHEN total_engagement_score >= 15 THEN 'highly_engaged'
      WHEN total_engagement_score >= 8 THEN 'moderately_engaged'
      WHEN total_engagement_score >= 3 THEN 'lightly_engaged'
      ELSE 'minimal_engagement'
    END AS engagement_tier,
    
    -- Journey quality assessment
    CASE 
      WHEN avg_data_quality_score >= 0.9 THEN 'high_quality'
      WHEN avg_data_quality_score >= 0.7 THEN 'medium_quality'
      ELSE 'low_quality'
    END AS journey_quality,
    
    -- Bounce detection (enhanced)
    CASE 
      WHEN total_events = 1 THEN 'immediate_bounce'
      WHEN total_events <= 3 AND session_duration_minutes < 1 THEN 'quick_bounce'
      ELSE 'engaged_session'
    END AS bounce_classification,
    
    -- Conversion indicators (enhanced - customize based on business logic)
    CASE 
      WHEN core_tour_events >= 5 AND 
           unique_spaces_viewed >= 3 AND 
           session_duration_minutes >= 3 AND
           total_engagement_score >= 10 THEN 'high_conversion_intent'
      WHEN virtual_tour_events >= 3 AND 
           unique_spaces_viewed >= 2 AND 
           session_duration_minutes >= 2 THEN 'medium_conversion_intent'
      WHEN space_context_events > 0 AND 
           session_duration_minutes >= 1 THEN 'low_conversion_intent'
      ELSE 'no_conversion_intent'
    END AS conversion_intent_level,
    
    -- Space preference analysis
    CASE 
      WHEN ARRAY_CONTAINS(space_types_engaged, 'unit') AND 
           ARRAY_CONTAINS(space_types_engaged, 'amenity') THEN 'comprehensive_explorer'
      WHEN ARRAY_CONTAINS(space_types_engaged, 'unit') THEN 'unit_focused'
      WHEN ARRAY_CONTAINS(space_types_engaged, 'amenity') THEN 'amenity_focused'
      WHEN ARRAY_CONTAINS(space_types_engaged, 'tour') THEN 'tour_focused'
      ELSE 'unfocused_browsing'
    END AS space_preference_pattern
    
  FROM daily_sessions
),

-- Journey flow patterns with enhanced analytics
journey_flow_patterns AS (
  SELECT 
    analysis_date,
    journey_type,
    engagement_tier,
    journey_quality,
    device_type,
    detailed_region,
    session_time_period,
    conversion_intent_level,
    space_preference_pattern,
    
    -- Session metrics (enhanced)
    COUNT(*) AS session_count,
    COUNT(DISTINCT anonymous_id) AS unique_users,
    
    -- Engagement metrics (enhanced)
    AVG(session_duration_minutes) AS avg_session_duration_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_duration_minutes) AS median_session_duration_minutes,
    AVG(total_events) AS avg_events_per_session,
    AVG(unique_spaces_viewed) AS avg_spaces_per_session,
    AVG(total_engagement_score) AS avg_engagement_score,
    MAX(total_engagement_score) AS max_engagement_score,
    
    -- Virtual tour specific metrics
    AVG(virtual_tour_events) AS avg_virtual_tour_events,
    AVG(core_tour_events) AS avg_core_tour_events,
    AVG(CAST(space_context_events AS DOUBLE) / CAST(total_events AS DOUBLE)) AS space_context_ratio,
    
    -- Journey outcome metrics (enhanced)
    COUNT(CASE WHEN bounce_classification = 'immediate_bounce' THEN 1 END) AS immediate_bounce_sessions,
    COUNT(CASE WHEN bounce_classification = 'quick_bounce' THEN 1 END) AS quick_bounce_sessions,
    COUNT(CASE WHEN bounce_classification = 'engaged_session' THEN 1 END) AS engaged_sessions,
    
    -- Conversion intent metrics
    COUNT(CASE WHEN conversion_intent_level = 'high_conversion_intent' THEN 1 END) AS high_intent_sessions,
    COUNT(CASE WHEN conversion_intent_level = 'medium_conversion_intent' THEN 1 END) AS medium_intent_sessions,
    COUNT(CASE WHEN conversion_intent_level = 'low_conversion_intent' THEN 1 END) AS low_intent_sessions,
    
    -- Entry point analysis (enhanced)
    COUNT(CASE WHEN entry_referrer != '' AND entry_referrer IS NOT NULL THEN 1 END) AS referred_sessions,
    COUNT(CASE WHEN entry_page LIKE '%/viewer%' THEN 1 END) AS direct_tour_entries,
    
    -- Quality metrics
    AVG(avg_data_quality_score) AS avg_session_quality_score,
    COUNT(CASE WHEN journey_quality = 'high_quality' THEN 1 END) AS high_quality_sessions
    
  FROM journey_classification
  WHERE min_data_quality_score >= 0.5  -- Filter low quality sessions
  GROUP BY 
    analysis_date, journey_type, engagement_tier, journey_quality,
    device_type, detailed_region, session_time_period, 
    conversion_intent_level, space_preference_pattern
)

SELECT 
  analysis_date,
  journey_type,
  engagement_tier,
  journey_quality,
  device_type,
  detailed_region,
  session_time_period,
  conversion_intent_level,
  space_preference_pattern,
  session_count,
  unique_users,
  avg_session_duration_minutes,
  median_session_duration_minutes,
  avg_events_per_session,
  avg_spaces_per_session,
  avg_engagement_score,
  max_engagement_score,
  avg_virtual_tour_events,
  avg_core_tour_events,
  space_context_ratio,
  immediate_bounce_sessions,
  quick_bounce_sessions,
  engaged_sessions,
  high_intent_sessions,
  medium_intent_sessions,
  low_intent_sessions,
  referred_sessions,
  direct_tour_entries,
  avg_session_quality_score,
  high_quality_sessions,
  
  -- Calculated metrics
  unique_users * 1.0 / session_count AS users_per_session_ratio,
  (immediate_bounce_sessions + quick_bounce_sessions) * 100.0 / session_count AS total_bounce_rate,
  engaged_sessions * 100.0 / session_count AS engagement_rate,
  (high_intent_sessions + medium_intent_sessions) * 100.0 / session_count AS conversion_intent_rate,
  referred_sessions * 100.0 / session_count AS referral_rate,
  high_quality_sessions * 100.0 / session_count AS high_quality_rate,
  
  -- Classification categories
  CASE 
    WHEN total_bounce_rate < 20 THEN 'low_bounce'
    WHEN total_bounce_rate < 50 THEN 'medium_bounce' 
    ELSE 'high_bounce'
  END AS bounce_category,
  
  CASE 
    WHEN conversion_intent_rate >= 40 THEN 'high_converting_segment'
    WHEN conversion_intent_rate >= 20 THEN 'medium_converting_segment'
    ELSE 'low_converting_segment'
  END AS conversion_segment,
  
  -- Processing metadata
  CURRENT_TIMESTAMP AS created_at,
  'iceberg_enhanced' AS processing_version

FROM journey_flow_patterns
WHERE session_count >= 10  -- Filter out very small segments
ORDER BY analysis_date DESC, session_count DESC