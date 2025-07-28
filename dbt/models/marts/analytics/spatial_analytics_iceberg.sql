{{ 
  config(
    materialized='table',
    table_type='iceberg',
    tblproperties={
      'write.target-file-size-bytes': '67108864',
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'zstd',
      'write.merge.mode': 'merge-on-read'
    },
    partitioned_by=['analysis_date']
  ) 
}}

-- Spatial Analytics for Virtual Tours using MongoDB Enhanced Data
WITH spatial_events AS (
  SELECT 
    event_date AS analysis_date,
    community,
    building,
    space_type,
    space_name,
    room_id,
    room_name,
    event_name,
    coordinates,
    session_id,
    anonymous_id,
    user_id,  -- NEW: Track authenticated users
    event_timestamp,
    engagement_score,
    time_on_page_seconds,
    
    -- Extract 3D navigation metrics
    CASE 
      WHEN coordinates IS NOT NULL THEN coordinates.pitch
      ELSE NULL 
    END AS pitch_angle,
    
    CASE 
      WHEN coordinates IS NOT NULL THEN coordinates.yaw
      ELSE NULL 
    END AS yaw_angle,
    
    CASE 
      WHEN coordinates IS NOT NULL THEN coordinates.hfov
      ELSE NULL 
    END AS field_of_view,
    
    -- Room categorization
    CASE 
      WHEN UPPER(room_name) LIKE '%LIVING%' OR UPPER(room_name) LIKE '%FAMILY%' THEN 'living_space'
      WHEN UPPER(room_name) LIKE '%BEDROOM%' OR UPPER(room_name) LIKE '%BED%' THEN 'bedroom'
      WHEN UPPER(room_name) LIKE '%KITCHEN%' THEN 'kitchen'
      WHEN UPPER(room_name) LIKE '%BATHROOM%' OR UPPER(room_name) LIKE '%BATH%' THEN 'bathroom'
      WHEN UPPER(room_name) LIKE '%PATIO%' OR UPPER(room_name) LIKE '%BALCONY%' THEN 'outdoor_space'
      WHEN UPPER(room_name) LIKE '%OFFICE%' OR UPPER(room_name) LIKE '%STUDY%' THEN 'work_space'
      WHEN room_name IS NOT NULL THEN 'other_room'
      ELSE 'no_room_context'
    END AS room_category,
    
    -- 3D Navigation event classification
    CASE 
      WHEN event_name LIKE '%Room View Position%' THEN 'position_change'
      WHEN event_name LIKE '%Room View%' THEN 'room_entry'
      WHEN event_name LIKE '%Toggle%' THEN 'ui_interaction'
      ELSE 'standard_event'
    END AS navigation_event_type
    
  FROM {{ ref('fct_web_events_iceberg') }}
  WHERE event_date >= '{{ var("start_date", "2024-01-01") }}'
    AND (coordinates IS NOT NULL OR room_name IS NOT NULL OR community IS NOT NULL)
),

-- Community-level analytics
community_analytics AS (
  SELECT 
    analysis_date,
    community,
    building,
    
    -- Volume metrics
    COUNT(*) AS total_spatial_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT anonymous_id) AS unique_anonymous_users,
    COUNT(DISTINCT user_id) AS unique_authenticated_users,
    COUNT(DISTINCT space_name) AS unique_spaces_viewed,
    COUNT(DISTINCT room_name) AS unique_rooms_viewed,
    
    -- Engagement metrics
    AVG(engagement_score) AS avg_engagement_score,
    AVG(time_on_page_seconds) AS avg_time_per_view,
    SUM(time_on_page_seconds) AS total_engagement_time,
    
    -- 3D navigation metrics
    COUNT(CASE WHEN coordinates IS NOT NULL THEN 1 END) AS three_d_navigation_events,
    COUNT(CASE WHEN navigation_event_type = 'position_change' THEN 1 END) AS position_changes,
    AVG(CASE WHEN pitch_angle IS NOT NULL THEN ABS(pitch_angle) END) AS avg_pitch_variation,
    AVG(CASE WHEN yaw_angle IS NOT NULL THEN ABS(yaw_angle) END) AS avg_yaw_variation,
    
    -- Room popularity
    COUNT(CASE WHEN room_category = 'living_space' THEN 1 END) AS living_space_views,
    COUNT(CASE WHEN room_category = 'bedroom' THEN 1 END) AS bedroom_views,
    COUNT(CASE WHEN room_category = 'kitchen' THEN 1 END) AS kitchen_views,
    COUNT(CASE WHEN room_category = 'bathroom' THEN 1 END) AS bathroom_views,
    COUNT(CASE WHEN room_category = 'outdoor_space' THEN 1 END) AS outdoor_space_views,
    
    -- Calculated metrics
    COUNT(CASE WHEN coordinates IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS three_d_engagement_rate,
    COUNT(CASE WHEN room_name IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS room_context_rate
    
  FROM spatial_events
  GROUP BY analysis_date, community, building
),

-- Room-level journey analysis
room_journey_analytics AS (
  SELECT 
    analysis_date,
    community,
    building,
    space_name,
    room_category,
    
    -- Room-specific metrics
    COUNT(DISTINCT session_id) AS sessions_visiting_room,
    COUNT(*) AS total_room_events,
    AVG(time_on_page_seconds) AS avg_time_in_room,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_on_page_seconds) AS median_time_in_room,
    
    -- 3D engagement in rooms
    COUNT(CASE WHEN coordinates IS NOT NULL THEN 1 END) AS three_d_events_in_room,
    AVG(CASE WHEN pitch_angle IS NOT NULL THEN pitch_angle END) AS avg_pitch_in_room,
    AVG(CASE WHEN yaw_angle IS NOT NULL THEN yaw_angle END) AS avg_yaw_in_room,
    AVG(CASE WHEN field_of_view IS NOT NULL THEN field_of_view END) AS avg_fov_in_room,
    
    -- Entry/exit patterns
    COUNT(CASE WHEN navigation_event_type = 'room_entry' THEN 1 END) AS room_entries,
    COUNT(CASE WHEN navigation_event_type = 'position_change' THEN 1 END) AS position_changes_in_room,
    
    -- Engagement quality
    COUNT(CASE WHEN engagement_score >= 3 THEN 1 END) AS high_engagement_events,
    COUNT(CASE WHEN engagement_score >= 3 THEN 1 END) * 100.0 / COUNT(*) AS high_engagement_rate
    
  FROM spatial_events
  WHERE room_name IS NOT NULL
  GROUP BY analysis_date, community, building, space_name, room_category
),

-- 3D navigation behavior patterns
navigation_behavior AS (
  SELECT 
    analysis_date,
    
    -- Navigation pattern classification
    CASE 
      WHEN ABS(pitch_angle) <= 10 AND ABS(yaw_angle) <= 45 THEN 'minimal_navigation'
      WHEN ABS(pitch_angle) <= 30 AND ABS(yaw_angle) <= 120 THEN 'moderate_navigation'
      WHEN ABS(pitch_angle) <= 60 AND ABS(yaw_angle) <= 270 THEN 'active_navigation'
      ELSE 'intensive_navigation'
    END AS navigation_intensity,
    
    COUNT(*) AS events_count,
    COUNT(DISTINCT session_id) AS unique_sessions,
    AVG(time_on_page_seconds) AS avg_time_per_view,
    AVG(engagement_score) AS avg_engagement,
    
    -- Field of view analysis
    AVG(field_of_view) AS avg_field_of_view,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY field_of_view) AS p25_fov,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY field_of_view) AS p75_fov
    
  FROM spatial_events
  WHERE coordinates IS NOT NULL
  GROUP BY 
    analysis_date,
    CASE 
      WHEN ABS(pitch_angle) <= 10 AND ABS(yaw_angle) <= 45 THEN 'minimal_navigation'
      WHEN ABS(pitch_angle) <= 30 AND ABS(yaw_angle) <= 120 THEN 'moderate_navigation'
      WHEN ABS(pitch_angle) <= 60 AND ABS(yaw_angle) <= 270 THEN 'active_navigation'
      ELSE 'intensive_navigation'
    END
),

-- User authentication impact analysis
auth_impact_analysis AS (
  SELECT 
    analysis_date,
    
    -- Authenticated vs anonymous comparison
    CASE 
      WHEN user_id IS NOT NULL THEN 'authenticated'
      ELSE 'anonymous'
    END AS user_type,
    
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT COALESCE(user_id, anonymous_id)) AS unique_users,
    
    -- Spatial engagement by user type
    COUNT(CASE WHEN coordinates IS NOT NULL THEN 1 END) AS three_d_events,
    COUNT(CASE WHEN room_name IS NOT NULL THEN 1 END) AS room_context_events,
    AVG(engagement_score) AS avg_engagement_score,
    AVG(time_on_page_seconds) AS avg_time_per_event,
    
    -- Room diversity
    COUNT(DISTINCT room_name) AS unique_rooms_accessed,
    COUNT(DISTINCT community) AS unique_communities_accessed,
    
    -- Calculated rates
    COUNT(CASE WHEN coordinates IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS three_d_usage_rate,
    COUNT(CASE WHEN room_name IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS room_context_rate
    
  FROM spatial_events
  GROUP BY 
    analysis_date,
    CASE 
      WHEN user_id IS NOT NULL THEN 'authenticated'
      ELSE 'anonymous'
    END
)

-- Final aggregated spatial analytics
SELECT 
  analysis_date,
  'community_level' AS analysis_level,
  community,
  building,
  NULL AS space_name,
  NULL AS room_category,
  NULL AS navigation_intensity,
  NULL AS user_type,
  
  total_spatial_events,
  unique_sessions,
  unique_anonymous_users,
  unique_authenticated_users,
  unique_spaces_viewed,
  unique_rooms_viewed,
  avg_engagement_score,
  avg_time_per_view,
  three_d_navigation_events,
  three_d_engagement_rate,
  room_context_rate,
  living_space_views,
  bedroom_views,
  kitchen_views,
  bathroom_views,
  outdoor_space_views,
  
  NULL AS avg_pitch_in_room,
  NULL AS avg_yaw_in_room,
  NULL AS avg_fov_in_room,
  NULL AS high_engagement_rate,
  NULL AS avg_field_of_view,
  NULL AS three_d_usage_rate
  
FROM community_analytics

UNION ALL

SELECT 
  analysis_date,
  'room_level' AS analysis_level,
  community,
  building,
  space_name,
  room_category,
  NULL AS navigation_intensity,
  NULL AS user_type,
  
  total_room_events AS total_spatial_events,
  sessions_visiting_room AS unique_sessions,
  NULL AS unique_anonymous_users,
  NULL AS unique_authenticated_users,
  NULL AS unique_spaces_viewed,
  NULL AS unique_rooms_viewed,
  NULL AS avg_engagement_score,
  avg_time_in_room AS avg_time_per_view,
  three_d_events_in_room AS three_d_navigation_events,
  NULL AS three_d_engagement_rate,
  NULL AS room_context_rate,
  NULL AS living_space_views,
  NULL AS bedroom_views,
  NULL AS kitchen_views,
  NULL AS bathroom_views,
  NULL AS outdoor_space_views,
  
  avg_pitch_in_room,
  avg_yaw_in_room,
  avg_fov_in_room,
  high_engagement_rate,
  NULL AS avg_field_of_view,
  NULL AS three_d_usage_rate
  
FROM room_journey_analytics

UNION ALL

SELECT 
  analysis_date,
  'navigation_behavior' AS analysis_level,
  NULL AS community,
  NULL AS building,
  NULL AS space_name,
  NULL AS room_category,
  navigation_intensity,
  NULL AS user_type,
  
  events_count AS total_spatial_events,
  unique_sessions,
  NULL AS unique_anonymous_users,
  NULL AS unique_authenticated_users,
  NULL AS unique_spaces_viewed,
  NULL AS unique_rooms_viewed,
  avg_engagement,
  avg_time_per_view,
  events_count AS three_d_navigation_events,
  NULL AS three_d_engagement_rate,
  NULL AS room_context_rate,
  NULL AS living_space_views,
  NULL AS bedroom_views,
  NULL AS kitchen_views,
  NULL AS bathroom_views,
  NULL AS outdoor_space_views,
  
  NULL AS avg_pitch_in_room,
  NULL AS avg_yaw_in_room,
  NULL AS avg_fov_in_room,
  NULL AS high_engagement_rate,
  avg_field_of_view,
  NULL AS three_d_usage_rate
  
FROM navigation_behavior

UNION ALL

SELECT 
  analysis_date,
  'user_type_analysis' AS analysis_level,
  NULL AS community,
  NULL AS building,
  NULL AS space_name,
  NULL AS room_category,
  NULL AS navigation_intensity,
  user_type,
  
  total_events AS total_spatial_events,
  unique_sessions,
  CASE WHEN user_type = 'anonymous' THEN unique_users ELSE NULL END AS unique_anonymous_users,
  CASE WHEN user_type = 'authenticated' THEN unique_users ELSE NULL END AS unique_authenticated_users,
  NULL AS unique_spaces_viewed,
  unique_rooms_accessed AS unique_rooms_viewed,
  avg_engagement_score,
  avg_time_per_event AS avg_time_per_view,
  three_d_events AS three_d_navigation_events,
  three_d_usage_rate AS three_d_engagement_rate,
  room_context_rate,
  NULL AS living_space_views,
  NULL AS bedroom_views,
  NULL AS kitchen_views,
  NULL AS bathroom_views,
  NULL AS outdoor_space_views,
  
  NULL AS avg_pitch_in_room,
  NULL AS avg_yaw_in_room,
  NULL AS avg_fov_in_room,
  NULL AS high_engagement_rate,
  NULL AS avg_field_of_view,
  three_d_usage_rate
  
FROM auth_impact_analysis