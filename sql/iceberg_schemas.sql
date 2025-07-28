-- S3 Tables (Iceberg) Schema Definitions
-- Enhanced schema design for virtual tour analytics with Iceberg optimizations

-- ============================================================================
-- 1. Main Web Events Table (Iceberg Format)
-- ============================================================================

CREATE TABLE IF NOT EXISTS s3_tables.analytics.web_events (
  -- Event identification (optimized for deduplication)
  event_id STRING COMMENT 'Unique event identifier for deduplication',
  
  -- Time dimensions (optimized for partitioning and time travel)
  event_timestamp TIMESTAMP COMMENT 'Event occurrence timestamp',
  event_date DATE COMMENT 'Event date for partitioning',  
  event_hour INT COMMENT 'Hour of the day (0-23)',
  
  -- Event classification
  event_type STRING COMMENT 'Type of event (page, track, identify, etc.)',
  event_name STRING COMMENT 'Specific event name (Room View Position, Toggle Fullscreen, etc.)',
  
  -- User identification (optimized for user journey analysis)
  anonymous_id STRING COMMENT 'Anonymous user identifier',
  user_id STRING COMMENT 'Authenticated user identifier (when available)',
  session_id STRING COMMENT 'User session identifier',
  
  -- Session context (enhanced for clickstream analysis)
  session_event_sequence INT COMMENT 'Event sequence within session',
  is_first_event_in_session BOOLEAN COMMENT 'First event flag',
  time_on_page_seconds BIGINT COMMENT 'Time spent on page in seconds',
  
  -- Application context
  app_id STRING COMMENT 'Application identifier',
  app_name STRING COMMENT 'Application name',
  
  -- Page/URL context (normalized for analytics)
  page_url STRING COMMENT 'Full page URL',
  page_path STRING COMMENT 'URL path component',
  page_title STRING COMMENT 'Page title',
  url_search STRING COMMENT 'URL search parameters',
  url_hash STRING COMMENT 'URL hash fragment',
  
  -- Virtual tour context (business-specific)
  space_id STRING COMMENT 'Virtual space identifier',
  space_name STRING COMMENT 'Virtual space name',  
  space_type STRING COMMENT 'Type of space (unit, amenity, etc.) - partition key',
  space_token STRING COMMENT 'Space access token',
  
  -- Room-level granularity (NEW from MongoDB)
  room_id STRING COMMENT 'Room identifier within space',
  room_name STRING COMMENT 'Room name (Living Room, Bedroom, etc.)',
  
  -- Location hierarchy (NEW from MongoDB)
  community STRING COMMENT 'Community/property name',
  building STRING COMMENT 'Building identifier or name',
  floorplan STRING COMMENT 'Floorplan reference',
  
  -- 3D Navigation context (NEW from MongoDB)
  coordinates STRUCT<
    pitch: DOUBLE COMMENT 'Vertical viewing angle',
    yaw: DOUBLE COMMENT 'Horizontal viewing angle', 
    hfov: DOUBLE COMMENT 'Horizontal field of view'
  > COMMENT '3D viewer position coordinates',
  
  -- User environment context
  user_agent STRING COMMENT 'Browser user agent string',
  os_name STRING COMMENT 'Operating system name',  
  locale STRING COMMENT 'User locale (language-country)',
  timezone STRING COMMENT 'User timezone',
  referrer STRING COMMENT 'Referring URL',
  
  -- Device context (derived)
  screen_width INT COMMENT 'Screen width in pixels',
  screen_height INT COMMENT 'Screen height in pixels', 
  device_type STRING COMMENT 'Device classification (mobile, tablet, desktop)',
  
  -- Engagement indicators (derived)
  is_virtual_tour_page BOOLEAN COMMENT 'True if viewing virtual tour',
  has_space_context BOOLEAN COMMENT 'True if space information available',
  
  -- Data quality and processing metadata
  data_quality_score DOUBLE COMMENT 'Data completeness score (0.0-1.0)',
  is_bot BOOLEAN COMMENT 'True if identified as bot traffic',
  processed_timestamp TIMESTAMP COMMENT 'Processing timestamp',
  seconds_utc_ts BIGINT COMMENT 'Original Unix timestamp (milliseconds)',
  
  -- Stream processing metadata (for debugging)
  kinesis_sequence_number STRING COMMENT 'Kinesis sequence number',
  kinesis_partition_key STRING COMMENT 'Kinesis partition key',
  
  -- Raw properties (for extensibility and debugging)
  properties_json STRING COMMENT 'Raw event properties as JSON',
  
  -- MongoDB metadata (for data lineage)
  mongodb_id STRING COMMENT 'Original MongoDB ObjectId',
  created_at TIMESTAMP COMMENT 'MongoDB document creation timestamp',
  updated_at TIMESTAMP COMMENT 'MongoDB document update timestamp'
  
) USING iceberg
PARTITIONED BY (event_date, space_type)
TBLPROPERTIES (
  'write.target-file-size-bytes'='134217728',  -- 128MB target file size
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd',
  'write.merge.mode'='merge-on-read',          -- Optimized for streaming inserts
  'format-version'='2',                        -- Iceberg v2 features
  'commit.retry.num-retries'='3',
  'commit.retry.min-wait-ms'='100'
)
COMMENT 'Main web events table in Iceberg format optimized for virtual tour analytics';

-- ============================================================================
-- 2. Session Metrics Aggregation Table (Iceberg Format)  
-- ============================================================================

CREATE TABLE IF NOT EXISTS s3_tables.analytics.session_metrics (
  -- Session identification
  session_id STRING COMMENT 'Unique session identifier',
  anonymous_id STRING COMMENT 'Anonymous user identifier',
  user_id STRING COMMENT 'Authenticated user identifier (when available)',
  
  -- Session timing
  session_start TIMESTAMP COMMENT 'Session start timestamp',
  session_end TIMESTAMP COMMENT 'Session end timestamp', 
  session_duration_seconds BIGINT COMMENT 'Total session duration',
  
  -- Session volume metrics
  total_events BIGINT COMMENT 'Total events in session',
  unique_spaces_viewed BIGINT COMMENT 'Number of unique spaces viewed',
  total_time_on_site_seconds BIGINT COMMENT 'Total time spent on site',
  
  -- Session classification
  bounce_session BOOLEAN COMMENT 'True if single-event session',
  
  -- Virtual tour engagement
  space_types_viewed ARRAY<STRING> COMMENT 'List of space types viewed',
  rooms_viewed ARRAY<STRING> COMMENT 'List of room names viewed',
  virtual_tour_events BIGINT COMMENT 'Number of virtual tour events',
  core_tour_events BIGINT COMMENT 'Number of core tour interaction events',
  three_d_navigation_events BIGINT COMMENT 'Number of 3D navigation events',
  
  -- Location context
  communities_visited ARRAY<STRING> COMMENT 'List of communities visited',
  buildings_visited ARRAY<STRING> COMMENT 'List of buildings visited',
  
  -- Session context
  referrer STRING COMMENT 'Session entry referrer',  
  user_agent STRING COMMENT 'User agent string',
  timezone STRING COMMENT 'User timezone',
  device_type STRING COMMENT 'Device classification',
  region STRING COMMENT 'Geographic region',
  
  -- Processing metadata
  processed_timestamp TIMESTAMP COMMENT 'Processing timestamp',
  event_date DATE COMMENT 'Session date for partitioning'
  
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'write.target-file-size-bytes'='67108864',   -- 64MB for smaller table
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd',
  'format-version'='2'
)
COMMENT 'Session-level aggregated metrics for user journey analysis';

-- ============================================================================
-- 3. User Journey Analysis Table (dbt Materialized)
-- ============================================================================

CREATE TABLE IF NOT EXISTS s3_tables.analytics.user_journey_analysis (
  -- Analysis dimensions
  analysis_date DATE COMMENT 'Date of analysis',
  journey_type STRING COMMENT 'Classification of user journey type', 
  engagement_tier STRING COMMENT 'User engagement level',
  journey_quality STRING COMMENT 'Data quality assessment',
  device_type STRING COMMENT 'Device classification',
  detailed_region STRING COMMENT 'Geographic region',
  session_time_period STRING COMMENT 'Time period of session',
  conversion_intent_level STRING COMMENT 'Conversion likelihood assessment',
  space_preference_pattern STRING COMMENT 'Space viewing preference pattern',
  
  -- Volume metrics
  session_count BIGINT COMMENT 'Number of sessions',
  unique_users BIGINT COMMENT 'Number of unique users',
  
  -- Engagement metrics
  avg_session_duration_minutes DOUBLE COMMENT 'Average session duration',
  median_session_duration_minutes DOUBLE COMMENT 'Median session duration',
  avg_events_per_session DOUBLE COMMENT 'Average events per session',
  avg_spaces_per_session DOUBLE COMMENT 'Average spaces viewed per session',
  avg_engagement_score DOUBLE COMMENT 'Average engagement score',
  max_engagement_score DOUBLE COMMENT 'Maximum engagement score observed',
  
  -- Virtual tour metrics
  avg_virtual_tour_events DOUBLE COMMENT 'Average virtual tour events per session',
  avg_core_tour_events DOUBLE COMMENT 'Average core tour events per session', 
  space_context_ratio DOUBLE COMMENT 'Ratio of events with space context',
  
  -- Outcome metrics
  immediate_bounce_sessions BIGINT COMMENT 'Sessions with immediate bounce',
  quick_bounce_sessions BIGINT COMMENT 'Sessions with quick bounce',
  engaged_sessions BIGINT COMMENT 'Sessions with engagement',
  high_intent_sessions BIGINT COMMENT 'High conversion intent sessions',
  medium_intent_sessions BIGINT COMMENT 'Medium conversion intent sessions',
  low_intent_sessions BIGINT COMMENT 'Low conversion intent sessions',
  
  -- Traffic source metrics
  referred_sessions BIGINT COMMENT 'Sessions from referral traffic',
  direct_tour_entries BIGINT COMMENT 'Sessions entering directly to tour',
  
  -- Quality metrics
  avg_session_quality_score DOUBLE COMMENT 'Average data quality score',
  high_quality_sessions BIGINT COMMENT 'High quality sessions count',
  
  -- Calculated KPIs
  users_per_session_ratio DOUBLE COMMENT 'Ratio of users to sessions',
  total_bounce_rate DOUBLE COMMENT 'Combined bounce rate percentage',
  engagement_rate DOUBLE COMMENT 'Engagement rate percentage',
  conversion_intent_rate DOUBLE COMMENT 'Conversion intent rate percentage', 
  referral_rate DOUBLE COMMENT 'Referral traffic percentage',
  high_quality_rate DOUBLE COMMENT 'High quality sessions percentage',
  
  -- Classification categories
  bounce_category STRING COMMENT 'Bounce rate classification',
  conversion_segment STRING COMMENT 'Conversion potential segment',
  
  -- Processing metadata
  created_at TIMESTAMP COMMENT 'Analysis creation timestamp',
  processing_version STRING COMMENT 'Processing pipeline version'
  
) USING iceberg
PARTITIONED BY (analysis_date)
TBLPROPERTIES (
  'write.target-file-size-bytes'='67108864',
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd',
  'format-version'='2'
)
COMMENT 'Daily user journey analysis with engagement and conversion metrics';

-- ============================================================================
-- 4. Time Travel Analysis Table (Iceberg-specific)
-- ============================================================================

CREATE TABLE IF NOT EXISTS s3_tables.analytics.time_travel_analysis (
  -- Analysis dimensions
  event_date DATE COMMENT 'Date being analyzed',
  space_type STRING COMMENT 'Space type being analyzed',
  
  -- Current state metrics
  current_events BIGINT COMMENT 'Current event count',
  current_sessions BIGINT COMMENT 'Current session count', 
  current_users BIGINT COMMENT 'Current user count',
  current_engagement DOUBLE COMMENT 'Current engagement score',
  current_time_on_page DOUBLE COMMENT 'Current time on page average',
  current_core_events BIGINT COMMENT 'Current core events count',
  current_quality DOUBLE COMMENT 'Current data quality score',
  
  -- Historical state metrics (24h ago)
  historical_events BIGINT COMMENT 'Historical event count',
  historical_sessions BIGINT COMMENT 'Historical session count',
  historical_users BIGINT COMMENT 'Historical user count', 
  historical_engagement DOUBLE COMMENT 'Historical engagement score',
  historical_time_on_page DOUBLE COMMENT 'Historical time on page average',
  historical_core_events BIGINT COMMENT 'Historical core events count',
  historical_quality DOUBLE COMMENT 'Historical data quality score',
  
  -- Change metrics (absolute)
  events_change BIGINT COMMENT 'Absolute change in events',
  sessions_change BIGINT COMMENT 'Absolute change in sessions',
  users_change BIGINT COMMENT 'Absolute change in users',
  engagement_change DOUBLE COMMENT 'Absolute change in engagement',
  time_on_page_change DOUBLE COMMENT 'Absolute change in time on page',
  core_events_change BIGINT COMMENT 'Absolute change in core events',
  quality_change DOUBLE COMMENT 'Absolute change in quality',
  
  -- Change metrics (percentage)
  events_change_pct DOUBLE COMMENT 'Percentage change in events',
  sessions_change_pct DOUBLE COMMENT 'Percentage change in sessions', 
  engagement_change_pct DOUBLE COMMENT 'Percentage change in engagement',
  
  -- Anomaly detection flags  
  events_variance_flag STRING COMMENT 'Event variance classification',
  engagement_variance_flag STRING COMMENT 'Engagement variance classification',
  quality_trend STRING COMMENT 'Data quality trend indicator',
  data_freshness_status STRING COMMENT 'Data freshness assessment'
  
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'write.target-file-size-bytes'='33554432',   -- 32MB for analysis table
  'write.format.default'='parquet', 
  'write.parquet.compression-codec'='zstd',
  'format-version'='2'
)
COMMENT 'Time travel analysis comparing current vs historical data states';

-- ============================================================================
-- 5. External Table Mappings for Athena/Redshift Compatibility
-- ============================================================================

-- External table for Athena queries (maps to Iceberg table)
CREATE OR REPLACE EXTERNAL TABLE peek_web_events.web_events_external (
  event_id string,
  event_timestamp timestamp,
  event_date date,
  event_hour int,
  event_type string,
  event_name string,
  anonymous_id string,
  user_id string,
  session_id string,
  session_event_sequence int,
  is_first_event_in_session boolean,
  time_on_page_seconds bigint,
  app_id string,
  app_name string,
  page_url string,
  page_path string,
  page_title string,
  space_id string,
  space_name string,
  space_type string,
  room_id string,
  room_name string,
  community string,
  building string,
  floorplan string,
  coordinates struct<pitch:double,yaw:double,hfov:double>,
  user_agent string,
  device_type string,
  is_virtual_tour_page boolean,
  has_space_context boolean,
  data_quality_score double,
  is_bot boolean,
  processed_timestamp timestamp,
  mongodb_id string,
  created_at timestamp,
  updated_at timestamp
)
STORED AS INPUTFORMAT 'org.apache.iceberg.mr.hive.HiveIcebergInputFormat'
OUTPUTFORMAT 'org.apache.iceberg.mr.hive.HiveIcebergOutputFormat'
LOCATION 's3://peek-web-events-tables-ACCOUNT_ID/namespaces/analytics/tables/web_events/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='iceberg'
);

-- ============================================================================
-- 6. Performance Optimization Views
-- ============================================================================

-- High-performance view for real-time dashboards
CREATE OR REPLACE VIEW s3_tables.analytics.realtime_metrics AS
SELECT 
  DATE_TRUNC('hour', event_timestamp) AS hour_bucket,
  space_type,
  device_type,
  COUNT(*) AS events_count,
  COUNT(DISTINCT session_id) AS unique_sessions,
  COUNT(DISTINCT anonymous_id) AS unique_users,
  AVG(engagement_score) AS avg_engagement,
  COUNT(CASE WHEN is_core_virtual_tour_event THEN 1 END) AS core_tour_events,
  AVG(time_on_page_seconds) AS avg_time_on_page
FROM s3_tables.analytics.web_events
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND NOT is_bot
  AND data_quality_score >= 0.7
GROUP BY DATE_TRUNC('hour', event_timestamp), space_type, device_type
ORDER BY hour_bucket DESC;

-- ============================================================================
-- 7. Data Quality Monitoring Views
-- ============================================================================

-- Data quality monitoring view
CREATE OR REPLACE VIEW s3_tables.analytics.data_quality_monitor AS
SELECT 
  event_date,  
  space_type,
  COUNT(*) AS total_events,
  COUNT(CASE WHEN session_id IS NULL THEN 1 END) AS missing_session_id,
  COUNT(CASE WHEN anonymous_id IS NULL THEN 1 END) AS missing_anonymous_id, 
  COUNT(CASE WHEN user_agent IS NULL THEN 1 END) AS missing_user_agent,
  COUNT(CASE WHEN is_bot THEN 1 END) AS bot_events,
  AVG(data_quality_score) AS avg_quality_score,
  MIN(data_quality_score) AS min_quality_score,
  MAX(processed_timestamp) AS latest_processed,
  COUNT(CASE WHEN data_quality_score < 0.5 THEN 1 END) AS low_quality_events,
  COUNT(CASE WHEN data_quality_score >= 0.9 THEN 1 END) AS high_quality_events
FROM s3_tables.analytics.web_events
WHERE event_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY event_date, space_type
ORDER BY event_date DESC, space_type;