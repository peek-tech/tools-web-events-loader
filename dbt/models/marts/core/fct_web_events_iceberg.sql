{{ 
  config(
    materialized='table',
    table_type='iceberg',
    unique_key='event_id',
    tblproperties={
      'write.target-file-size-bytes': '134217728',
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'zstd',
      'write.merge.mode': 'merge-on-read'
    },
    partitioned_by=['event_date', 'space_type']
  ) 
}}

WITH enriched_events AS (
  SELECT 
    sb.*,
    
    -- Enhanced space categorization with room-level granularity (NEW)
    CASE 
      WHEN space_type = 'unit' AND room_name IS NOT NULL THEN 'unit_room_level'
      WHEN space_type = 'unit' THEN 'residential_unit'
      WHEN space_type IN ('common_area', 'amenity') THEN 'shared_space'
      WHEN space_type = 'tour' THEN 'guided_experience'
      WHEN space_type IS NULL AND is_virtual_tour_page THEN 'tour_navigation'
      WHEN space_type = 'unknown' AND page_path LIKE '%viewer%' THEN 'unclassified_space'
      ELSE 'other'
    END AS space_category,
    
    -- NEW: Room type categorization for detailed spatial analysis
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
    
    -- NEW: Location hierarchy analysis
    CASE 
      WHEN community IS NOT NULL AND building IS NOT NULL THEN 'full_location_context'
      WHEN community IS NOT NULL THEN 'community_level'
      WHEN building IS NOT NULL THEN 'building_level'
      ELSE 'no_location_hierarchy'
    END AS location_context_level,
    
    -- NEW: 3D Navigation analysis (coordinates available)
    CASE 
      WHEN coordinates IS NOT NULL AND coordinates.pitch IS NOT NULL THEN 'active_3d_navigation'
      WHEN event_name LIKE '%Room View Position%' THEN 'position_tracking'
      WHEN event_name LIKE '%Toggle%' THEN 'interaction_event'
      ELSE 'standard_pageview'
    END AS navigation_type,
    
    -- Enhanced user engagement scoring with MongoDB context
    CASE 
      WHEN event_name IS NOT NULL AND coordinates IS NOT NULL AND room_name IS NOT NULL THEN 5  -- Full 3D engagement
      WHEN event_name IS NOT NULL AND has_space_context THEN 4  -- Named event with space
      WHEN engagement_level = 'engaged' AND has_space_context THEN 3  -- Traditional engaged
      WHEN engagement_level = 'engaged' OR coordinates IS NOT NULL THEN 2  -- Basic engagement or 3D navigation
      WHEN engagement_level = 'moderate' OR room_name IS NOT NULL THEN 1  -- Moderate or room context
      ELSE 0
    END AS engagement_score,
    
    -- NEW: 3D Navigation engagement scoring
    CASE 
      WHEN coordinates IS NOT NULL THEN
        CASE 
          WHEN ABS(coordinates.pitch) > 45 OR ABS(coordinates.yaw) > 180 THEN 'high_exploration'
          WHEN ABS(coordinates.pitch) > 15 OR ABS(coordinates.yaw) > 90 THEN 'medium_exploration'
          ELSE 'low_exploration'
        END
      ELSE 'no_3d_data'
    END AS spatial_exploration_level,
    
    -- Journey milestone detection (enhanced)
    CASE 
      WHEN is_session_start AND referrer != '' AND referrer NOT LIKE '%' || page_url || '%' THEN 'external_referral_entry'
      WHEN is_session_start AND referrer != '' THEN 'internal_referral_entry'
      WHEN is_session_start THEN 'direct_entry'
      WHEN is_virtual_tour_page AND has_space_context AND session_event_sequence <= 3 THEN 'early_space_exploration'
      WHEN is_virtual_tour_page AND has_space_context THEN 'deep_space_exploration'
      WHEN journey_stage = 'exit' AND total_session_events > 5 THEN 'engaged_exit'
      WHEN journey_stage = 'exit' THEN 'quick_exit'
      ELSE 'navigation'
    END AS journey_milestone,
    
    -- Content interaction depth (enhanced)
    CASE 
      WHEN total_session_events >= 20 THEN 'very_deep_exploration'
      WHEN total_session_events >= 10 THEN 'deep_exploration'
      WHEN total_session_events >= 5 THEN 'moderate_exploration'  
      WHEN total_session_events >= 2 THEN 'light_exploration'
      ELSE 'single_page'
    END AS exploration_depth,
    
    -- Time-based engagement patterns
    CASE 
      WHEN event_hour BETWEEN 9 AND 17 THEN 'business_hours'
      WHEN event_hour BETWEEN 18 AND 22 THEN 'evening'
      WHEN event_hour BETWEEN 6 AND 8 THEN 'morning'
      ELSE 'off_hours'
    END AS time_period,
    
    -- Geographic region (from timezone)
    CASE 
      WHEN timezone LIKE '%America/New_York%' OR timezone LIKE '%America/Toronto%' THEN 'Eastern_US'
      WHEN timezone LIKE '%America/Chicago%' OR timezone LIKE '%America/Denver%' THEN 'Central_US'
      WHEN timezone LIKE '%America/Los_Angeles%' OR timezone LIKE '%America/Vancouver%' THEN 'Western_US'
      WHEN timezone LIKE '%Europe%' THEN 'Europe'
      WHEN timezone LIKE '%Asia%' OR timezone LIKE '%Pacific%' THEN 'Asia_Pacific'
      ELSE 'Other'
    END AS detailed_region,
    
    -- Session context enrichment
    session_event_sequence / CAST(total_session_events AS DOUBLE) AS session_progress_ratio,
    
    -- Virtual tour specific metrics
    CASE 
      WHEN has_space_context AND space_type IN ('unit', 'tour') THEN TRUE
      ELSE FALSE
    END AS is_core_virtual_tour_event,
    
    -- Data lineage and quality
    'iceberg_native' AS processing_source,
    CURRENT_TIMESTAMP AS dbt_processed_at

  FROM {{ ref('int_session_boundaries') }} sb
),

-- Add time travel capability for data validation
final_events AS (
  SELECT 
    event_id,
    event_timestamp,
    event_date,
    event_hour,
    time_period,
    event_type,
    anonymous_id,
    session_id,
    session_event_sequence,
    session_event_rank,
    total_session_events,
    session_progress_ratio,
    
    -- Page context
    page_url,
    page_path,
    page_title,
    
    -- Virtual tour context (enhanced)
    space_id,
    space_name,
    space_type,
    space_category,
    is_virtual_tour_page,
    has_space_context,
    is_core_virtual_tour_event,
    
    -- User context (enhanced)
    referrer,
    user_agent,
    timezone,
    locale,
    region,
    detailed_region,
    device_type,
    
    -- Engagement metrics (enhanced)
    time_on_page_seconds,
    seconds_to_next_event,
    engagement_level,
    engagement_score,
    
    -- Journey analysis (enhanced)
    journey_stage,
    journey_milestone,
    exploration_depth,
    is_session_start,
    is_session_end,
    
    -- Quality and processing metadata
    data_quality_score,
    processing_source,
    processed_timestamp,
    dbt_processed_at
    
  FROM enriched_events
  WHERE data_quality_score >= 0.7  -- Higher threshold for fact table
    AND NOT is_bot
    AND session_id IS NOT NULL
    AND anonymous_id IS NOT NULL
)

SELECT * FROM final_events

-- Add table comment for Iceberg metadata
{{ config(
  post_hook="COMMENT ON TABLE {{ this }} IS 'Core web events fact table in Iceberg format with enhanced analytics'"
) }}