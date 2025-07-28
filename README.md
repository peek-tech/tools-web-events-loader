# Web Events Data Loader

A comprehensive AWS S3 Tables/Iceberg data loader for processing and analyzing web event data from virtual tours and website interactions, designed to support clickstream analysis and user journey tracking.

## Architecture Overview

This system implements a modern Lambda Architecture pattern with real-time and batch processing capabilities using AWS S3 Tables with Apache Iceberg:

- **Ingestion Layer**: AWS Kinesis streams for real-time events + S3 for historical data
- **Processing Layer**: AWS Glue ETL jobs + Lambda functions for data transformation  
- **Storage Layer**: AWS S3 Tables with Iceberg format for ACID transactions and time travel
- **Analytics Layer**: Amazon Athena with Iceberg support for high-performance queries
- **Orchestration**: Apache Airflow for workflow management and table maintenance
- **Transformation**: dbt for data modeling and analytics with Iceberg table support

## Project Structure

```
├── cdk/                    # AWS CDK infrastructure code
│   ├── lib/
│   │   └── web-events-data-lake-stack.ts
│   ├── app.ts
│   └── package.json
├── glue/                   # Glue ETL job scripts
│   ├── kinesis_processor.py
│   └── s3_processor.py
├── lambda/                 # Lambda function code
│   └── kinesis-processor/
├── airflow/               # Airflow DAGs
│   └── dags/
│       └── web_events_comprehensive_pipeline.py
├── dbt/                   # dbt models for analytics
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── dbt_project.yml
├── sql/                   # SQL schemas and queries
├── monitoring/            # Monitoring configurations
└── docs/                  # Documentation
```

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- Node.js 18+ (for CDK)
- Python 3.9+ (for Glue/Lambda)
- dbt CLI (for analytics models)

### 1. Deploy Infrastructure

```bash
cd cdk
npm install
npm run build
cdk bootstrap
cdk deploy WebEventsDataLakeStack
```

### 2. Configure Airflow

```bash
# Set required Airflow variables
airflow variables set aws_account_id YOUR_AWS_ACCOUNT_ID
airflow variables set source_s3_bucket YOUR_SOURCE_BUCKET_NAME

# Upload DAGs
cp airflow/dags/* $AIRFLOW_HOME/dags/
```

### 3. Initialize dbt Project

```bash
cd dbt
dbt deps
dbt seed
dbt run --target dev
dbt test
```

### 4. Process Historical Data

```bash
# Trigger historical data processing DAG
airflow dags trigger web_events_historical_processing \
  --conf '{"source_bucket": "your-source-bucket"}'
```

## Data Flow

### Real-time Processing
1. **Events → Kinesis Stream** - Web events sent to Kinesis Data Streams
2. **Lambda Processing** - Base64 decoding, enrichment, and S3 writing
3. **Glue Streaming** - Continuous processing and data quality checks
4. **S3 Raw Zone** - Partitioned storage by date/hour
5. **Athena/Redshift** - Real-time querying capabilities

### Batch Processing  
1. **Historical S3 Data** - 75GB+ of existing web events
2. **Glue ETL Jobs** - Data parsing, quality scoring, and enrichment
3. **S3 Curated Zone** - Clean, analysis-ready data
4. **dbt Transformations** - Business logic and analytics models
5. **S3 Analytics Zone** - Aggregated metrics and insights

## Key Features

### Data Quality Framework
- **Automated Quality Scoring** - Events scored 0-1 based on completeness
- **Bot Detection** - User agent analysis and behavioral patterns
- **Schema Validation** - Ensures data consistency across pipeline
- **Data Lineage** - Full traceability from source to analytics

### Virtual Tour Analytics
- **Space Interaction Tracking** - Detailed engagement with virtual spaces
- **User Journey Mapping** - Complete clickstream flow analysis  
- **Conversion Funnel Analysis** - Multi-step user progression tracking
- **Device & Geographic Analysis** - Cross-platform user behavior

### Performance Optimizations  
- **Partitioned Storage** - Date-based partitioning for query performance
- **Columnar Format** - Parquet with Snappy compression
- **Query Optimization** - Predicate pushdown and partition pruning
- **Auto-scaling** - Dynamic resource allocation based on load

## Analytics Use Cases

### 1. User Journey Analysis
```sql
-- Example: Virtual tour engagement funnel
SELECT 
  journey_type,
  engagement_tier,
  COUNT(*) as sessions,
  AVG(session_duration_minutes) as avg_duration,
  conversion_rate
FROM analytics.user_journey_analysis 
WHERE analysis_date >= CURRENT_DATE - 7
GROUP BY journey_type, engagement_tier
ORDER BY sessions DESC;
```

### 2. Space Performance Analysis
```sql  
-- Example: Most engaging virtual spaces
SELECT 
  space_name,
  space_type,
  unique_sessions,
  avg_time_on_space,
  views_per_session
FROM analytics.daily_space_engagement
WHERE analysis_date = CURRENT_DATE - 1
ORDER BY avg_time_on_space DESC
LIMIT 10;
```

### 3. Cohort Retention Analysis
```sql
-- Example: Weekly user retention
SELECT
  cohort_week,
  weeks_since_first_visit,
  retention_rate
FROM analytics.daily_cohort_analysis
WHERE analysis_date = CURRENT_DATE - 1
ORDER BY cohort_week, weeks_since_first_visit;
```

## Monitoring & Alerting

### CloudWatch Dashboards
- **Real-time Metrics** - Kinesis ingestion, Lambda performance
- **Data Quality Metrics** - Processing success rates, error counts
- **Business Metrics** - User engagement, conversion rates
- **Cost Monitoring** - Resource utilization and spend tracking

### Key Alerts
- Data processing delays > 15 minutes
- Error rates > 1% over 1 hour
- Data quality score drops below 0.8
- Storage costs exceed budget thresholds

## Development Workflow

### 1. Local Development
```bash
# Test dbt models locally
dbt run --target dev --models staging
dbt test --target dev

# Validate Glue jobs
python glue/s3_processor.py --local-mode
```

### 2. Deployment Pipeline
```bash
# Infrastructure changes
cdk diff
cdk deploy

# Data model updates  
dbt run --target prod
dbt docs generate
dbt docs serve
```

### 3. Data Quality Testing
```bash
# Run comprehensive data quality checks
airflow dags trigger web_events_data_quality_monitoring

# Manual quality validation
python scripts/validate_data_quality.py --date 2024-01-15
```

## Cost Optimization

### Current Scale (75GB)
- **Monthly AWS Costs**: ~$6,500
  - S3 Storage: $1,500
  - Redshift: $3,000  
  - Glue: $800
  - Kinesis: $500
  - Other: $700

### Optimization Strategies
- **S3 Intelligent Tiering** - Automatic cost optimization
- **Spot Instances** - 60-80% savings on EMR workloads
- **Reserved Capacity** - Redshift reserved instances
- **Lifecycle Policies** - Automated data archival

## Security & Compliance

### Data Encryption
- **At Rest**: S3 KMS encryption, Redshift cluster encryption
- **In Transit**: TLS 1.2 for all data transfers
- **Key Management**: Customer-managed KMS keys

### Access Control
- **IAM Policies** - Least privilege access
- **VPC Endpoints** - Private AWS service connectivity
- **Data Masking** - PII protection in non-production
- **Audit Logging** - CloudTrail for all API calls

## Troubleshooting

### Common Issues

**1. Kinesis Processing Delays**
```bash
# Check Lambda concurrency limits
aws lambda get-function-concurrency --function-name KinesisProcessor

# Monitor Kinesis metrics
aws cloudwatch get-metric-statistics --namespace AWS/Kinesis \
  --metric-data MetricName=IncomingRecords,StreamName=peek-web-events-stream
```

**2. Glue Job Failures**
```bash
# Check job logs
aws logs describe-log-groups --log-group-name-prefix /aws-glue/jobs

# Review job bookmarks
aws glue get-job-bookmark --job-name peek-web-events-s3-processor
```

**3. Data Quality Issues**
```bash
# Run data quality validation
dbt test --models staging
dbt run-operation check_data_freshness
```

## Documentation

### 📋 Complete System Documentation
- **[System Architecture](docs/architecture.md)**: Comprehensive architecture overview with data flow diagrams
- **[Deployment Guide](docs/deployment-guide.md)**: Step-by-step deployment instructions for all environments
- **[Operational Runbook](docs/operational-runbook.md)**: Daily operations, incident response, and maintenance procedures
- **[Data Dictionary](docs/data-dictionary.md)**: Complete field definitions, business logic, and transformations
- **[Test Documentation](tests/README.md)**: Testing strategy, test cases, and coverage reports

### 🔧 Development Resources
- **dbt Docs**: Available at `http://localhost:8080` after running `dbt docs serve`
- **API Reference**: Generated from code documentation
- **Schema Definitions**: See `sql/iceberg_schemas.sql` for complete table schemas

### 📊 Key Features Documentation
- **S3 Tables/Iceberg Integration**: ACID transactions, time travel queries, schema evolution
- **Spatial Analytics**: 3D virtual tour navigation analysis with room-level insights
- **Data Quality Framework**: Automated scoring, bot detection, and validation
- **Real-time Processing**: Kinesis stream processing with sub-second latency
- **Advanced Analytics**: User journey analysis, conversion funnels, cohort retention

## Contributing

1. Follow the established project structure
2. Add comprehensive tests for new features
3. Update documentation for any changes
4. Follow data quality standards and validation rules
5. Test changes in dev environment before production deployment

---

**Project Team**: Data Engineering Team  
**Last Updated**: 2024-07-28  
**Version**: 1.0.0