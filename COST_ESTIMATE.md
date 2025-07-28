# Web Events Data Lake Processing (800GB) Cost Analysis Estimate Report

## Service Overview

Web Events Data Lake Processing (800GB) is a fully managed, serverless service that allows you to This project uses multiple AWS services.. This service follows a pay-as-you-go pricing model, making it cost-effective for various workloads.

## Pricing Model

This cost analysis estimate is based on the following pricing model:
- **ON DEMAND** pricing (pay-as-you-go) unless otherwise specified
- Standard service configurations without reserved capacity or savings plans
- No caching or optimization techniques applied

## Assumptions

- Processing 800GB of historical web events data from prod-backup-web-events bucket
- Data contains JSON files with Kinesis Records wrapper format
- Processing includes MongoDB document transformation and enrichment
- One-time historical data migration with ongoing incremental processing
- Standard ON DEMAND pricing model for all services
- Data processing optimized for S3 Tables/Iceberg format
- Estimated 20-40 DPU hours for complete historical data processing (scaled from 75GB baseline)
- Processing in parallel batches to optimize performance and cost

## Limitations and Exclusions

- Data transfer costs between regions (assuming same region processing)
- S3 Tables storage costs beyond initial setup
- Ongoing operational costs after initial migration
- Network egress charges for data access
- CloudWatch logging and monitoring costs
- IAM and other administrative service costs
- Development and testing environment costs
- Airflow or other orchestration service costs

## Cost Breakdown

### Unit Pricing Details

| Service | Resource Type | Unit | Price | Free Tier |
|---------|--------------|------|-------|------------|
| AWS Glue ETL Processing | Standard Etl | DPU-Hour for standard ETL processing | $0.44 | No free tier for AWS Glue ETL jobs |
| AWS Glue ETL Processing | Flex Etl | DPU-Hour for Flex ETL jobs (recommended) | $0.29 | No free tier for AWS Glue ETL jobs |
| AWS Glue ETL Processing | Iceberg Optimization | DPU-Hour for Iceberg table optimization | $0.44 | No free tier for AWS Glue ETL jobs |
| Amazon S3 Storage | Standard Storage | GB-month for first 50TB | $0.023 | First 5GB of S3 Standard storage is free for 12 months (negligible for this volume) |
| Amazon S3 Storage | Requests | 1,000 PUT requests | $0.0004 | First 5GB of S3 Standard storage is free for 12 months (negligible for this volume) |
| Amazon S3 Storage | Data Retrieval | 1,000 GET requests | $0.0004 | First 5GB of S3 Standard storage is free for 12 months (negligible for this volume) |
| AWS Lambda | Requests | request | $0.0000002 | 1M requests and 400,000 GB-seconds free monthly (will be exceeded) |
| AWS Lambda | Compute | GB-second | $0.0000166667 | 1M requests and 400,000 GB-seconds free monthly (will be exceeded) |
| Amazon Athena | Data Scanned | TB of data scanned | $5.00 | 10GB of data scanned per month free (minimal impact) |
| Amazon Kinesis (if used) | Shard Hours | shard-hour | $0.015 | No free tier for Kinesis Data Streams |
| Amazon Kinesis (if used) | Data Ingestion | GB of data written | $0.08 | No free tier for Kinesis Data Streams |
| Amazon Kinesis (if used) | Data Retrieval | GB of data read | $0.04 | No free tier for Kinesis Data Streams |
| AWS S3 Tables/Iceberg | Table Storage | GB-month (same as S3 Standard) | $0.023 | No specific free tier for S3 Tables beyond standard S3 storage |
| AWS S3 Tables/Iceberg | Table Requests | 1 unit | Standard S3 request pricing | No specific free tier for S3 Tables beyond standard S3 storage |
| AWS S3 Tables/Iceberg | Metadata Operations | 1 unit | Minimal additional cost for Iceberg metadata | No specific free tier for S3 Tables beyond standard S3 storage |

### Cost Calculation

| Service | Usage | Calculation | Monthly Cost |
|---------|-------|-------------|-------------|
| AWS Glue ETL Processing | Processing 800GB of web events data with ETL transformations, quality scoring, and Iceberg format conversion (Estimated Dpu Hours: 32-64 DPU-Hours for complete 800GB processing, Data Volume: 800GB of JSON web events data, Processing Batches: 8-16 parallel processing jobs) | Standard: $0.44 × 32-64 DPU-Hours = $14.08-$28.16 | Flex (recommended): $0.29 × 32-64 DPU-Hours = $9.28-$18.56 | $14.08 - $28.16 |
| Amazon S3 Storage | Storage for source data, intermediate processing files, and final Iceberg format data (Source Data: 800GB source data storage, Processed Data: ~530GB processed Iceberg format data (compressed), Processing Overhead: ~270GB temporary/intermediate files, Total Storage: ~1,600GB peak storage during processing) | Peak storage: 1,600GB × $0.023 = $36.80/month | Post-processing: (800GB + 530GB) × $0.023 = $30.59/month (assuming 1-month processing window) | $26.68 - $35.00 |
| AWS Lambda | Event processing, transformation functions, metadata operations, and batch coordination (Estimated Requests: ~1,067,000 requests for 800GB processing, Compute Time: ~53,333 GB-seconds (512MB × 106,667 seconds), Batch Coordination: Additional overhead for parallel processing) | Requests: $0.0000002 × 1,067,000 = $0.21 | Compute: $0.0000166667 × 53,333 GB-seconds = $0.89 | Total: $1.10 | $1.07 - $2.67 |
| Amazon Athena | Query processing for data validation, quality checks, analytics setup, and monitoring (Data Scanned: ~320-800GB for validation, quality checks, and setup queries, Validation Queries: Multiple passes for data quality verification) | Conservative: $5.00 × 0.32TB = $1.60 | Comprehensive: $5.00 × 0.80TB = $4.00 | $1.60 - $4.00 |
| Amazon Kinesis (if used) | Stream processing for real-time components and batch coordination during migration (Shard Hours: 240-480 shard-hours for migration period (4-8 shards × 60-120 hours), Data Processed: 800GB throughput, Coordination Overhead: Additional stream management) | Shards: $0.015 × 320 shard-hours = $4.80 | Data: ($0.08 + $0.04) × 0.8TB = $0.096 × 800GB = $0.77 | Total: $5.57 | $5.33 - $10.67 |
| AWS S3 Tables/Iceberg | S3 Tables storage and Iceberg table management for final data lake format (Final Table Storage: 530GB compressed Iceberg format, Metadata Overhead: ~10GB for Iceberg metadata and manifests, Monthly Storage: 540GB ongoing storage) | Storage: 540GB × $0.023 = $12.42/month | Request overhead: ~$0.50/month | Total: ~$12.92/month | $12.19 - $18.40 |
| **Total** | **All services** | **Sum of all calculations** | **$60.95/month** |

### Free Tier

Free tier information by service:
- **AWS Glue ETL Processing**: No free tier for AWS Glue ETL jobs
- **Amazon S3 Storage**: First 5GB of S3 Standard storage is free for 12 months (negligible for this volume)
- **AWS Lambda**: 1M requests and 400,000 GB-seconds free monthly (will be exceeded)
- **Amazon Athena**: 10GB of data scanned per month free (minimal impact)
- **Amazon Kinesis (if used)**: No free tier for Kinesis Data Streams
- **AWS S3 Tables/Iceberg**: No specific free tier for S3 Tables beyond standard S3 storage

## Cost Scaling with Usage

The following table illustrates how cost estimates scale with different usage levels:

| Service | Low Usage | Medium Usage | High Usage |
|---------|-----------|--------------|------------|
| AWS Glue ETL Processing | $7/month | $14/month | $28/month |
| Amazon S3 Storage | $13/month | $26/month | $53/month |
| AWS Lambda | $0/month | $1/month | $2/month |
| Amazon Athena | $0/month | $1/month | $3/month |
| Amazon Kinesis (if used) | $2/month | $5/month | $10/month |
| AWS S3 Tables/Iceberg | $6/month | $12/month | $24/month |

### Key Cost Factors

- **AWS Glue ETL Processing**: Processing 800GB of web events data with ETL transformations, quality scoring, and Iceberg format conversion
- **Amazon S3 Storage**: Storage for source data, intermediate processing files, and final Iceberg format data
- **AWS Lambda**: Event processing, transformation functions, metadata operations, and batch coordination
- **Amazon Athena**: Query processing for data validation, quality checks, analytics setup, and monitoring
- **Amazon Kinesis (if used)**: Stream processing for real-time components and batch coordination during migration
- **AWS S3 Tables/Iceberg**: S3 Tables storage and Iceberg table management for final data lake format

## Projected Costs Over Time

The following projections show estimated monthly costs over a 12-month period based on different growth patterns:

Base monthly cost calculation:

| Service | Monthly Cost |
|---------|-------------|
| AWS Glue ETL Processing | $14.08 |
| Amazon S3 Storage | $26.68 |
| AWS Lambda | $1.07 |
| Amazon Athena | $1.60 |
| Amazon Kinesis (if used) | $5.33 |
| AWS S3 Tables/Iceberg | $12.19 |
| **Total Monthly Cost** | **$60** |

| Growth Pattern | Month 1 | Month 3 | Month 6 | Month 12 |
|---------------|---------|---------|---------|----------|
| Steady | $60/mo | $60/mo | $60/mo | $60/mo |
| Moderate | $60/mo | $67/mo | $77/mo | $104/mo |
| Rapid | $60/mo | $73/mo | $98/mo | $173/mo |

* Steady: No monthly growth (1.0x)
* Moderate: 5% monthly growth (1.05x)
* Rapid: 10% monthly growth (1.1x)

## Detailed Cost Analysis

### Pricing Model

ON DEMAND


### Exclusions

- Data transfer costs between regions (assuming same region processing)
- S3 Tables storage costs beyond initial setup
- Ongoing operational costs after initial migration
- Network egress charges for data access
- CloudWatch logging and monitoring costs
- IAM and other administrative service costs
- Development and testing environment costs
- Airflow or other orchestration service costs

### Recommendations

#### Immediate Actions

- Use AWS Glue Flex ETL jobs ($0.29/DPU-hour) instead of standard ETL ($0.44/DPU-hour) for 34% cost savings (~$9.28-$18.56 vs $14.08-$28.16)
- Process data in parallel batches (8-16 jobs) to optimize DPU usage and reduce total processing time
- Enable S3 Intelligent Tiering immediately to reduce storage costs by 20-40% for processed data
- Use S3 lifecycle policies to transition intermediate/temporary files to cheaper storage classes after processing



## Cost Optimization Recommendations

### Immediate Actions

- Use AWS Glue Flex ETL jobs ($0.29/DPU-hour) instead of standard ETL ($0.44/DPU-hour) for 34% cost savings (~$9.28-$18.56 vs $14.08-$28.16)
- Process data in parallel batches (8-16 jobs) to optimize DPU usage and reduce total processing time
- Enable S3 Intelligent Tiering immediately to reduce storage costs by 20-40% for processed data

### Best Practices

- Regularly review costs with AWS Cost Explorer
- Consider reserved capacity for predictable workloads
- Implement automated scaling based on demand

## Conclusion

By following the recommendations in this report, you can optimize your Web Events Data Lake Processing (800GB) costs while maintaining performance and reliability. Regular monitoring and adjustment of your usage patterns will help ensure cost efficiency as your workload evolves.
