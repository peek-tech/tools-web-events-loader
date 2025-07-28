{{ config(materialized='view') }}

WITH source_data AS (
  SELECT * FROM {{ source('s3_tables', 'web_events') }}
  WHERE event_date >= DATE('{{ var("start_date") }}')
    AND event_date <= CURRENT_DATE
    {% if var("bot_detection_enabled", true) %}
    AND NOT is_bot
    {% endif %}
    AND data_quality_score >= {{ var("min_quality_score", 0.5) }}
    -- Iceberg time travel capability (optional)
    {% if var("snapshot_timestamp", none) %}
    FOR TIMESTAMP AS OF TIMESTAMP '{{ var("snapshot_timestamp") }}'
    {% endif %}
),

standardized_events AS (
  SELECT 
    -- Generate unique event identifier
    {{ dbt_utils.generate_surrogate_key(['session_id', 'anonymous_id', 'event_timestamp']) }} AS event_id,
    
    -- Core event attributes
    event_timestamp,
    event_date,
    event_hour,
    event_type,
    anonymous_id,
    session_id,
    session_event_sequence,
    is_first_event_in_session,
    
    -- Page context
    page_url,
    page_path,
    page_title,
    
    -- Virtual tour context
    space_id,
    space_name,
    space_type,
    
    -- User context
    referrer,
    user_agent,
    timezone,
    locale,
    app_id,
    
    -- Device context
    screen_width,
    screen_height,
    device_type,  -- Already calculated in Iceberg processing
    
    -- Engagement metrics
    time_on_page_seconds,
    CASE 
      WHEN time_on_page_seconds >= 30 THEN 'engaged'
      WHEN time_on_page_seconds >= 10 THEN 'moderate'
      ELSE 'bounce'
    END AS engagement_level,
    
    -- Virtual tour specific flags
    page_path LIKE '{{ var("virtual_tour_path_pattern") }}%' AS is_virtual_tour_page,
    space_id IS NOT NULL AS has_space_context,
    
    -- Geographic context (derived from timezone)
    CASE 
      WHEN timezone LIKE '%America%' THEN 'Americas'
      WHEN timezone LIKE '%Europe%' THEN 'Europe'
      WHEN timezone LIKE '%Asia%' OR timezone LIKE '%Pacific%' THEN 'Asia-Pacific'
      ELSE 'Other'
    END AS region,
    
    -- Quality indicators
    data_quality_score,
    processed_timestamp
    
  FROM source_data
)

SELECT * FROM standardized_events