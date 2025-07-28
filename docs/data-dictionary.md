# Data Dictionary

## Overview
This document provides comprehensive definitions of all data fields, transformations, and business logic in the Web Events Data Loader system.

## Event Data Structure

### Raw Event Schema
Events can arrive in two formats: **Legacy** (simple structure) or **Nested** (eventData wrapper).

#### Legacy Event Format
```json
{
  "type": "page",                    // Event type classification
  "anonymousId": "user-123",         // Anonymous user identifier
  "sessionId": "session-456",        // Session identifier
  "secondsUtcTS": 1704067200000,     // Unix timestamp in milliseconds
  "properties": {                     // Event-specific data
    "title": "Virtual Tour",
    "url": "https://example.com/viewer",
    "spaceId": "space-789"
  },
  "userAgent": "Mozilla/5.0...",     // Browser user agent
  "timezone": "America/New_York",    // User timezone
  "locale": "en-US"                  // User locale
}
```

#### Nested Event Format (Enhanced)
```json
{
  "_id": {"$oid": "65b1234567890abcdef12345"},  // Document identifier
  "eventTimestamp": {"$date": "2024-01-01T00:00:00.000Z"},
  "createdAt": {"$date": "2024-01-01T00:00:00.000Z"},
  "updatedAt": {"$date": "2024-01-01T00:00:01.000Z"},
  "metadata": {                       // Document metadata
    "anonymousId": "user-123",
    "appId": "web-viewer"
  },
  "eventData": {                     // Actual event payload
    "type": "track",
    "event": "Room View Position",    // Specific event name
    "userId": "auth-user-789",        // Authenticated user ID
    "anonymousId": "user-123",
    "sessionId": "session-456",
    "properties": {
      "roomId": "room-101",           // Room-level identifier
      "roomName": "LIVING ROOM",      // Human-readable room name
      "community": "Towers at Rincon", // Property community
      "building": "Building A",       // Building identifier
      "floorplan": "2BR-2BA",        // Floorplan type
      "coordinates": {                // 3D navigation data
        "pitch": 7.77,               // Vertical viewing angle
        "yaw": -121.71,              // Horizontal viewing angle
        "hfov": 123                  // Horizontal field of view
      }
    },
    "page": {                        // Page context (separate from properties)
      "title": "Peek | Virtual Tour",
      "url": "https://example.com/viewer?token=abc123",
      "path": "/viewer",
      "width": 1024,                 // Screen width
      "height": 768                  // Screen height
    }
  }
}
```

## Core Data Tables

### web_events (Primary Fact Table)
The main events table storing all processed web interactions.

| Field Name | Data Type | Nullable | Description | Business Logic |
|------------|-----------|----------|-------------|----------------|
| `event_id` | STRING | No | Unique event identifier | Generated: `{session_id}_{kinesis_sequence}_{timestamp}` |
| `event_timestamp` | TIMESTAMP | No | When the event occurred | From `secondsUtcTS` or current time if missing |
| `event_date` | DATE | No | Date partition key | Derived from `event_timestamp` |
| `event_hour` | INTEGER | No | Hour of day (0-23) | Extracted from `event_timestamp` |
| `event_type` | STRING | Yes | Event classification | Values: `page`, `track`, `identify`, `screen` |
| `event_name` | STRING | Yes | Specific event name | From nested structure: `eventData.event` |
| `anonymous_id` | STRING | Yes | Anonymous user ID | Primary user identifier for tracking |
| `user_id` | STRING | Yes | Authenticated user ID | Available when user is logged in |
| `session_id` | STRING | Yes | Session identifier | Groups related events |
| `app_id` | STRING | Yes | Application identifier | Source application |
| `app_name` | STRING | Yes | Application name | Human-readable app name |

#### Page Context Fields
| Field Name | Data Type | Description | Source |
|------------|-----------|-------------|---------|
| `page_url` | STRING | Full page URL | `page.url` or `properties.url` |
| `page_path` | STRING | URL path component | `page.path` or `properties.path` |
| `page_title` | STRING | Page title | `page.title` or `properties.title` |
| `url_search` | STRING | Query parameters | `page.search` |
| `url_hash` | STRING | URL fragment | `page.hash` |

#### Virtual Tour Context Fields
| Field Name | Data Type | Description | Business Rules |
|------------|-----------|-------------|----------------|
| `space_id` | STRING | Virtual space identifier | Unique identifier for toured space |
| `space_name` | STRING | Human-readable space name | E.g., "Unit 2B - Master Bedroom" |
| `space_type` | STRING | Space classification | Values: `unit`, `amenity`, `common_area`, `tour` |
| `space_token` | STRING | Access token for space | Security token for private tours |

#### Room-Level Granularity (Enhanced Spatial Data)
| Field Name | Data Type | Description | Added From Analysis |
|------------|-----------|-------------|-------------------|
| `room_id` | STRING | Room identifier within space | From `properties.roomId` |
| `room_name` | STRING | Room name | E.g., "LIVING ROOM", "BEDROOM" |
| `community` | STRING | Property community/complex | E.g., "Towers at Rincon" |
| `building` | STRING | Building identifier | E.g., "Building A", "Tower 1" |
| `floorplan` | STRING | Floorplan type | E.g., "2BR-2BA", "Studio" |

#### 3D Navigation Coordinates
| Field Name | Data Type | Description | Technical Details |
|------------|-----------|-------------|------------------|
| `coordinates` | STRUCT | 3D viewing position | Nested structure with pitch/yaw/hfov |
| `coordinates.pitch` | DOUBLE | Vertical viewing angle | Range: -90° to +90° |
| `coordinates.yaw` | DOUBLE | Horizontal viewing angle | Range: -180° to +180° |
| `coordinates.hfov` | DOUBLE | Horizontal field of view | Lens width in degrees |

#### User Context Fields
| Field Name | Data Type | Description | Derivation Logic |
|------------|-----------|-------------|-----------------|
| `user_agent` | STRING | Browser user agent | Raw user agent string |
| `os_name` | STRING | Operating system | Extracted from user agent |
| `locale` | STRING | User locale | E.g., "en-US", "es-ES" |
| `timezone` | STRING | User timezone | E.g., "America/New_York" |
| `referrer` | STRING | Referrer URL | Source page URL |
| `screen_width` | INTEGER | Screen width in pixels | From page context |
| `screen_height` | INTEGER | Screen height in pixels | From page context |
| `device_type` | STRING | Device classification | See Device Classification |

#### Data Quality and Processing Fields
| Field Name | Data Type | Description | Calculation Logic |
|------------|-----------|-------------|------------------|
| `data_quality_score` | DOUBLE | Event completeness score | Range: 0.0 to 1.0, see Quality Scoring |
| `is_bot` | BOOLEAN | Bot traffic indicator | Based on user agent and behavior patterns |
| `has_space_context` | BOOLEAN | Virtual tour event flag | True if space_id is present |
| `is_virtual_tour_page` | BOOLEAN | Tour page indicator | True if path contains "/viewer" |
| `processed_timestamp` | TIMESTAMP | Processing time | When event was processed by system |
| `document_id` | STRING | Source document ID | From nested structure metadata |
| `created_at` | TIMESTAMP | Document creation time | From source system |
| `updated_at` | TIMESTAMP | Document update time | From source system |
| `properties_json` | STRING | Raw properties JSON | Original properties for extensibility |

## Derived Analytics Tables

### spatial_analytics_iceberg
Advanced spatial analytics for virtual tour engagement.

#### Community-Level Metrics
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `analysis_date` | DATE | Analysis date partition |
| `analysis_level` | STRING | Aggregation level: `community_level`, `room_level`, `navigation_behavior`, `user_type_analysis` |
| `community` | STRING | Property community name |
| `building` | STRING | Building identifier |
| `total_spatial_events` | BIGINT | Total events with spatial context |
| `unique_sessions` | BIGINT | Distinct sessions |
| `unique_anonymous_users` | BIGINT | Distinct anonymous users |
| `unique_authenticated_users` | BIGINT | Distinct authenticated users |
| `avg_engagement_score` | DOUBLE | Average engagement score |
| `three_d_engagement_rate` | DOUBLE | Percentage of events with 3D navigation |

#### Room-Level Journey Metrics
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `room_category` | STRING | Room type: `living_space`, `bedroom`, `kitchen`, `bathroom`, `outdoor_space` |
| `sessions_visiting_room` | BIGINT | Sessions that visited this room |
| `avg_time_in_room` | DOUBLE | Average time spent in room (seconds) |
| `three_d_events_in_room` | BIGINT | 3D navigation events in room |
| `avg_pitch_in_room` | DOUBLE | Average pitch angle in room |
| `avg_yaw_in_room` | DOUBLE | Average yaw angle in room |
| `high_engagement_rate` | DOUBLE | Percentage of high-engagement events |

#### Navigation Behavior Patterns
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `navigation_intensity` | STRING | `minimal_navigation`, `moderate_navigation`, `active_navigation`, `intensive_navigation` |
| `avg_field_of_view` | DOUBLE | Average horizontal field of view |
| `p25_fov` | DOUBLE | 25th percentile field of view |
| `p75_fov` | DOUBLE | 75th percentile field of view |

### user_journey_analysis_iceberg
User journey and conversion funnel analysis.

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `journey_type` | STRING | `new_visitor`, `returning_visitor`, `authenticated_user` |
| `journey_stage` | STRING | `entry`, `exploration`, `engagement`, `conversion`, `exit` |
| `session_sequence` | INTEGER | Event position within session |
| `time_between_events` | INTEGER | Seconds between events |
| `conversion_event` | BOOLEAN | Indicates conversion milestone |
| `bounce_session` | BOOLEAN | Single-event session indicator |
| `session_duration_minutes` | DOUBLE | Total session length |

### time_travel_analysis
Historical data analysis using Iceberg time travel capabilities.

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `snapshot_timestamp` | TIMESTAMP | Table snapshot time |
| `records_at_snapshot` | BIGINT | Record count at snapshot |
| `quality_score_trend` | DOUBLE | Quality score at snapshot |
| `schema_version` | STRING | Table schema version |

## Business Logic and Calculations

### Data Quality Scoring Algorithm
```sql
-- Base score starts at 1.0, deductions applied for missing data
CASE 
  WHEN session_id IS NULL THEN score - 0.3
  WHEN anonymous_id IS NULL AND user_id IS NULL THEN score - 0.3  
  WHEN user_agent IS NULL THEN score - 0.2
  WHEN space_id IS NULL AND page_path LIKE '%/viewer%' THEN score - 0.1
  -- Bonuses for enhanced data
  WHEN room_id IS NOT NULL AND room_name IS NOT NULL THEN score + 0.1
  WHEN coordinates IS NOT NULL THEN score + 0.05
  WHEN event_name IS NOT NULL THEN score + 0.05
  ELSE score
END
```

### Device Classification Logic
```sql
CASE 
  WHEN screen_width < 768 THEN 'mobile'
  WHEN screen_width < 1024 THEN 'tablet'  
  WHEN screen_width >= 1024 THEN 'desktop'
  ELSE 'unknown'
END
```

### Room Categorization
```sql
CASE 
  WHEN UPPER(room_name) LIKE '%LIVING%' OR UPPER(room_name) LIKE '%FAMILY%' 
    THEN 'living_space'
  WHEN UPPER(room_name) LIKE '%BEDROOM%' OR UPPER(room_name) LIKE '%BED%' 
    THEN 'bedroom'
  WHEN UPPER(room_name) LIKE '%KITCHEN%' 
    THEN 'kitchen'
  WHEN UPPER(room_name) LIKE '%BATHROOM%' OR UPPER(room_name) LIKE '%BATH%' 
    THEN 'bathroom'
  WHEN UPPER(room_name) LIKE '%PATIO%' OR UPPER(room_name) LIKE '%BALCONY%' 
    THEN 'outdoor_space'
  WHEN UPPER(room_name) LIKE '%OFFICE%' OR UPPER(room_name) LIKE '%STUDY%' 
    THEN 'work_space'
  WHEN room_name IS NOT NULL 
    THEN 'other_room'
  ELSE 'no_room_context'
END
```

### Spatial Exploration Scoring
```sql
CASE 
  WHEN coordinates IS NOT NULL THEN
    CASE 
      WHEN ABS(coordinates.pitch) > 45 OR ABS(coordinates.yaw) > 180 
        THEN 'high_exploration'
      WHEN ABS(coordinates.pitch) > 15 OR ABS(coordinates.yaw) > 90 
        THEN 'medium_exploration'
      ELSE 'low_exploration'
    END
  ELSE 'no_3d_data'
END
```

### Bot Detection Logic
```sql
CASE 
  WHEN LOWER(user_agent) LIKE '%bot%' 
    OR LOWER(user_agent) LIKE '%crawler%'
    OR LOWER(user_agent) LIKE '%spider%'
    OR LOWER(user_agent) LIKE '%scraper%'
    THEN TRUE
  WHEN user_agent IS NULL OR user_agent = '' 
    THEN TRUE
  ELSE FALSE
END
```

### Journey Stage Classification
```sql
CASE 
  WHEN session_event_sequence = 1 THEN 'entry'
  WHEN session_event_sequence <= 3 AND has_space_context THEN 'exploration'
  WHEN engagement_score >= 3 AND time_on_page_seconds > 30 THEN 'engagement'
  WHEN event_type = 'track' AND event_name LIKE '%Conversion%' THEN 'conversion'
  WHEN session_event_sequence = total_session_events THEN 'exit'
  ELSE 'navigation'
END
```

## Data Lineage

### Processing Flow
1. **Raw Events** → Kinesis Stream or S3 Files
2. **Lambda/Glue Processing** → Structure normalization and enrichment
3. **S3 Tables Storage** → Iceberg format with ACID properties
4. **dbt Transformations** → Business logic and analytics
5. **Analytics Tables** → Query-optimized datasets

### Data Freshness
- **Real-time**: Lambda processing with 1-2 minute latency
- **Batch**: Daily processing with full historical refresh
- **Analytics**: dbt models updated every 4 hours

### Data Retention
- **Raw Events**: 7 years in production, 30 days in staging
- **Processed Tables**: Indefinite with intelligent tiering
- **Staging Data**: 7 days cleanup cycle

## Data Governance

### Data Classification
- **Public**: Aggregated metrics and anonymized insights
- **Internal**: Individual session data with anonymous IDs
- **Restricted**: Authenticated user data with PII considerations

### Access Patterns
- **Analytics Team**: Full read access to all tables
- **Product Team**: Aggregated metrics and conversion data
- **Engineering**: Technical metrics and data quality monitoring
- **Executives**: High-level KPI dashboards

### Compliance Considerations
- **GDPR**: Anonymous ID can be deleted on request
- **CCPA**: User data purging capabilities
- **Data Residency**: All data stored in specified AWS regions
- **Audit Trail**: Full processing lineage maintained

This data dictionary should be updated whenever schema changes are made to ensure accuracy and completeness.