# System Architecture Guide

## Overview
The Web Events Data Loader is a comprehensive AWS-native data processing system that transforms raw web event data into actionable analytics using AWS S3 Tables with Apache Iceberg format.

## High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Events    │───▶│   AWS Kinesis    │───▶│   Lambda        │
│   (JavaScript)  │    │   Data Streams   │    │   Processor     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐             ▼
│  Historical     │───▶│   AWS Glue       │    ┌─────────────────┐
│  S3 Data (75GB) │    │   ETL Jobs       │◀───│   S3 Tables     │
└─────────────────┘    └──────────────────┘    │   (Iceberg)     │
                                ▲               └─────────────────┘
                                │                        │
┌─────────────────┐    ┌──────────────────┐             ▼
│   Apache        │───▶│   dbt Models     │    ┌─────────────────┐
│   Airflow       │    │   Transformations│◀───│   Amazon        │
└─────────────────┘    └──────────────────┘    │   Athena        │
                                                └─────────────────┘
```

## Core Components

### 1. Data Ingestion Layer

#### AWS Kinesis Data Streams
- **Purpose**: Real-time event ingestion from web applications
- **Configuration**: Auto-scaling shards based on throughput
- **Data Format**: Base64-encoded JSON events
- **Retention**: 24 hours for processing reliability

#### Historical S3 Data
- **Purpose**: Batch processing of existing 75GB event archive
- **Format**: JSON files with mixed event structures
- **Organization**: Time-based prefixes for efficient processing

### 2. Real-Time Processing

#### Lambda Kinesis Processor
- **Trigger**: Kinesis stream events
- **Function**: `lambda/kinesis-processor/index_iceberg.py`
- **Processing**:
  - Base64 decoding of event payloads
  - Event structure normalization (legacy vs nested)
  - Data quality scoring and enrichment
  - Bot detection and filtering
  - S3 Tables staging write

**Event Processing Logic**:
```python
# Handles both event structures:
# Legacy: { "type": "page", "properties": {...} }
# Nested: { "eventData": { "type": "track", "properties": {...} } }

if 'eventData' in event_data:
    # Extract nested structure
    event_payload = event_data['eventData']
    metadata = event_data.get('metadata', {})
else:
    # Use direct structure
    event_payload = event_data
    metadata = {}
```

### 3. Batch Processing

#### AWS Glue ETL Jobs
- **Historical Processor**: `glue/s3_processor_iceberg.py`
- **Streaming Processor**: `glue/kinesis_processor_iceberg.py`
- **Functions**:
  - Large-scale data transformation
  - Schema validation and normalization
  - Data quality assessment
  - Iceberg table maintenance

**Processing Flow**:
1. Read raw events from S3 or Kinesis
2. Parse and validate JSON structure
3. Apply data quality scoring
4. Transform to standardized schema
5. Write to S3 Tables with partitioning
6. Update table metadata and statistics

### 4. Storage Layer - AWS S3 Tables with Iceberg

#### Table Structure
```sql
-- Primary events table
CREATE TABLE s3_tables.analytics.web_events (
  event_id STRING,
  event_timestamp TIMESTAMP,
  event_date DATE,
  event_name STRING,           -- Specific event classification
  anonymous_id STRING,
  user_id STRING,              -- Authenticated user tracking
  session_id STRING,
  
  -- Spatial data
  space_id STRING,
  space_name STRING,
  space_type STRING,
  room_id STRING,              -- Room-level granularity
  room_name STRING,
  community STRING,            -- Location hierarchy
  building STRING,
  floorplan STRING,
  coordinates STRUCT<          -- 3D navigation data
    pitch: DOUBLE,
    yaw: DOUBLE,
    hfov: DOUBLE
  >,
  
  -- Quality and processing metadata
  data_quality_score DOUBLE,
  is_bot BOOLEAN,
  processed_timestamp TIMESTAMP
) USING iceberg
PARTITIONED BY (event_date, space_type)
```

#### Iceberg Features
- **ACID Transactions**: Guaranteed data consistency
- **Time Travel**: Query historical table versions
- **Schema Evolution**: Safe column additions/modifications
- **Efficient Updates**: Merge operations for late-arriving data

### 5. Orchestration Layer - Apache Airflow

#### Primary DAG: `web_events_s3_tables_pipeline.py`
```python
# Daily processing workflow
dag_tasks = [
    "validate_source_data",
    "trigger_glue_etl",
    "run_dbt_transformations", 
    "update_table_statistics",
    "run_data_quality_checks",
    "optimize_table_partitions"
]
```

**Scheduling**: Daily at 2 AM UTC with 6-hour retry window
**Monitoring**: CloudWatch integration with SNS alerting
**Dependencies**: Ensures data consistency across processing stages

### 6. Analytics Layer - dbt Transformations

#### Model Hierarchy
```
models/
├── staging/          # Raw data cleaning
│   └── stg_raw_web_events.sql
├── intermediate/     # Business logic
│   └── int_session_boundaries.sql
└── marts/           # Analytics-ready datasets
    ├── core/
    │   └── fct_web_events_iceberg.sql
    └── analytics/
        ├── spatial_analytics_iceberg.sql
        ├── user_journey_analysis_iceberg.sql
        └── time_travel_analysis.sql
```

#### Key Analytics Models

**Spatial Analytics**: 3D virtual tour engagement
```sql
-- Room-level engagement scoring
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
END AS spatial_exploration_level
```

**User Journey Analysis**: Conversion funnel tracking
```sql
-- Journey milestone detection
CASE 
  WHEN is_session_start AND referrer != '' THEN 'external_referral_entry'
  WHEN is_virtual_tour_page AND has_space_context THEN 'space_exploration'
  WHEN journey_stage = 'exit' AND total_session_events > 5 THEN 'engaged_exit'
  ELSE 'navigation'
END AS journey_milestone
```

## Data Flow Details

### Real-Time Data Flow
1. **Event Generation**: Web application JavaScript SDK sends events
2. **Kinesis Ingestion**: Events queued in Kinesis Data Streams
3. **Lambda Processing**: 
   - Events decoded and enriched
   - Quality scoring applied
   - Bot filtering performed
   - Staged to S3 Tables
4. **Glue Streaming**: Continuous processing into main tables
5. **dbt Transformations**: Near real-time analytics updates
6. **Athena Queries**: Sub-second query performance

### Batch Data Flow
1. **Historical Data**: 75GB+ of archived events in S3
2. **Glue ETL**: Large-scale processing job
   - Parallel processing across multiple workers
   - Schema normalization and quality scoring
   - Direct write to S3 Tables
3. **dbt Batch Processing**: Full analytics refresh
4. **Table Optimization**: Compaction and statistics updates

### Event Structure Handling

The system intelligently handles two event formats:

**Legacy Structure** (Simple):
```json
{
  "type": "page",
  "anonymousId": "user-123",
  "sessionId": "session-456",
  "properties": {
    "spaceId": "space-789",
    "url": "https://example.com/viewer"
  }
}
```

**Nested Structure** (Enhanced):
```json
{
  "_id": {"$oid": "65b1234567890abcdef12345"},
  "eventTimestamp": {"$date": "2024-01-01T00:00:00.000Z"},
  "metadata": {
    "anonymousId": "user-123",
    "appId": "web-viewer"
  },
  "eventData": {
    "type": "track",
    "event": "Room View Position",
    "userId": "auth-user-789",
    "properties": {
      "roomId": "room-101",
      "roomName": "LIVING ROOM",
      "coordinates": {
        "pitch": 7.77,
        "yaw": -121.71,
        "hfov": 123
      }
    }
  }
}
```

## Infrastructure Components

### AWS CDK Stack: `cdk/lib/web-events-data-lake-stack.ts`
- **S3 Tables**: Main data storage with Iceberg catalog
- **Lambda Functions**: Real-time processing with auto-scaling
- **Glue Jobs**: ETL with configurable instance types
- **Kinesis Streams**: Event ingestion with shard management
- **IAM Roles**: Least-privilege access policies
- **CloudWatch**: Monitoring and alerting infrastructure

### Security Architecture
- **Encryption**: KMS-managed keys for all data at rest
- **Network**: VPC endpoints for private AWS service access
- **Access Control**: IAM policies with service-specific permissions
- **Audit Logging**: CloudTrail for all API operations

### Monitoring and Observability
- **Real-time Metrics**: Lambda execution, Kinesis throughput
- **Data Quality Metrics**: Processing success rates, error counts
- **Business Metrics**: User engagement, conversion rates
- **Cost Monitoring**: Resource utilization and spend tracking

## Deployment Architecture

### Environment Management
- **Development**: Single-AZ deployment for cost optimization
- **Staging**: Production-like configuration for testing
- **Production**: Multi-AZ with auto-scaling and redundancy

### Deployment Process
1. **CDK Infrastructure**: `cdk deploy` creates AWS resources
2. **Lambda Deployment**: Automated packaging and deployment
3. **Glue Scripts**: Uploaded to S3 and registered
4. **dbt Models**: Version-controlled with CI/CD integration
5. **Airflow DAGs**: Deployed with dependency validation

This architecture provides a scalable, reliable, and cost-effective solution for processing web event data at scale while maintaining data quality and providing rich analytics capabilities.