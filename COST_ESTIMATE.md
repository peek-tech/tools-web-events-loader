# One-Time Web Events Data Migration (800GB) Cost Analysis Estimate Report

## Service Overview

One-Time Web Events Data Migration (800GB) is a fully managed, serverless service that allows you to This project uses multiple AWS services.. This service follows a pay-as-you-go pricing model, making it cost-effective for various workloads.

## Pricing Model

This cost analysis estimate is based on the following pricing model:
- **ON DEMAND** pricing (pay-as-you-go) unless otherwise specified
- Standard service configurations without reserved capacity or savings plans
- No caching or optimization techniques applied

## Assumptions

- ONE-TIME processing of 800GB of historical web events data from prod-backup-web-events bucket
- Data contains JSON files with Kinesis Records wrapper format
- Processing includes MongoDB document transformation and enrichment
- One-time historical data migration to S3 Tables/Iceberg format
- Standard ON DEMAND pricing model for all services
- Processing duration: 3-7 days for complete migration
- Estimated 32-64 DPU hours total for complete historical data processing
- Processing in parallel batches to optimize performance and cost

## Limitations and Exclusions

- Ongoing monthly storage costs (calculated separately)
- Future incremental processing costs
- Data transfer costs between regions (assuming same region processing)
- Network egress charges for data access
- CloudWatch logging and monitoring costs
- IAM and other administrative service costs
- Development and testing environment costs
- Airflow or other orchestration service costs

## Cost Breakdown

### Unit Pricing Details

| Service | Resource Type | Unit | Price | Free Tier |
|---------|--------------|------|-------|------------|
| AWS Glue ETL Processing (One-time) | Standard Etl | DPU-Hour for standard ETL processing | $0.44 | No free tier for AWS Glue ETL jobs |
| AWS Glue ETL Processing (One-time) | Flex Etl | DPU-Hour for Flex ETL jobs (recommended for one-time migration) | $0.29 | No free tier for AWS Glue ETL jobs |
| AWS Glue ETL Processing (One-time) | Iceberg Optimization | DPU-Hour for Iceberg table optimization | $0.44 | No free tier for AWS Glue ETL jobs |
| Amazon S3 Storage (Migration Period) | Standard Storage | GB-month (prorated for migration period) | $0.023 | First 5GB of S3 Standard storage is free for 12 months (negligible for this volume) |
| Amazon S3 Storage (Migration Period) | Requests | 1,000,000 PUT requests | $5.40 | First 5GB of S3 Standard storage is free for 12 months (negligible for this volume) |
| Amazon S3 Storage (Migration Period) | Data Retrieval | 1,000,000 GET requests | $0.40 | First 5GB of S3 Standard storage is free for 12 months (negligible for this volume) |
| AWS Lambda (Migration Period) | Requests | 1,000,000 requests | $0.20 | 1M requests and 400,000 GB-seconds free monthly (will partially cover this) |
| AWS Lambda (Migration Period) | Compute | 1,000,000 GB-seconds | $16.67 | 1M requests and 400,000 GB-seconds free monthly (will partially cover this) |
| Amazon Athena (Migration Validation) | Data Scanned | TB of data scanned | $5.00 | 10GB of data scanned per month free (minimal impact) |
| Amazon Kinesis (Migration Period) | Shard Hours | shard-hour | $0.015 | No free tier for Kinesis Data Streams |
| Amazon Kinesis (Migration Period) | Data Ingestion | GB of data written | $0.08 | No free tier for Kinesis Data Streams |
| Amazon Kinesis (Migration Period) | Data Retrieval | GB of data read | $0.04 | No free tier for Kinesis Data Streams |

### Cost Calculation

| Service | Usage | Calculation | Monthly Cost |
|---------|-------|-------------|-------------|
| AWS Glue ETL Processing (One-time) | ONE-TIME processing of 800GB of web events data with ETL transformations, quality scoring, and Iceberg format conversion (Estimated Dpu Hours: 32-64 DPU-Hours total for complete 800GB processing, Data Volume: 800GB of JSON web events data, Processing Duration: 3-7 days with parallel batches) | Standard: $0.44 × 32-64 DPU-Hours = $14.08-$28.16 (ONE-TIME) | Flex (recommended): $0.29 × 32-64 DPU-Hours = $9.28-$18.56 (ONE-TIME) | $14.08 - $28.16 (or $9.28 - $18.56 with Flex) |
| Amazon S3 Storage (Migration Period) | Temporary storage during 3-7 day migration period for source data, intermediate processing files, and final Iceberg format data (Peak Storage During Migration: ~1,600GB peak storage for 3-7 days, Average Storage: ~1,200GB average during processing, Migration Duration: 3-7 days) | Peak storage for 1 week: 1,600GB × $0.023 × (7/30) = $8.59 | Average for migration: 1,200GB × $0.023 × (5/30) = $4.60 | $6.20 - $8.60 (for migration period only) |
| AWS Lambda (Migration Period) | Event processing, transformation functions, metadata operations, and batch coordination during migration (Estimated Requests: ~1,067,000 requests for 800GB processing, Compute Time: ~53,333 GB-seconds total, Processing Coordination: Batch coordination and monitoring) | Requests over free tier: (1,067K - 1,000K) × $0.20 = $0.013 | Compute over free tier: (53.3K - 400K) GB-sec = $0 (within free tier) | Total: ~$0.21 (one-time) | $1.07 - $2.67 (one-time) |
| Amazon Athena (Migration Validation) | One-time query processing for data validation, quality checks, and analytics setup after migration (Validation Scans: ~320-800GB for post-migration validation and quality checks, Setup Queries: Initial analytics and schema validation queries) | Conservative validation: $5.00 × 0.32TB = $1.60 (one-time) | Comprehensive validation: $5.00 × 0.80TB = $4.00 (one-time) | $1.60 - $4.00 (one-time) |
| Amazon Kinesis (Migration Period) | Stream processing coordination during 3-7 day migration period (if used) (Shard Hours: 72-168 shard-hours for 3-7 day migration (4 shards), Data Processed: 800GB throughput during migration, Coordination Period: 3-7 days total) | Shards: $0.015 × 120 shard-hours = $1.80 | Data: ($0.08 + $0.04) × 0.8TB = $0.096 | Total: $1.90 (one-time) | $1.08 - $2.52 (one-time) |
| **Total** | **All services** | **Sum of all calculations** | **$24.03/month** |

### Free Tier

Free tier information by service:
- **AWS Glue ETL Processing (One-time)**: No free tier for AWS Glue ETL jobs
- **Amazon S3 Storage (Migration Period)**: First 5GB of S3 Standard storage is free for 12 months (negligible for this volume)
- **AWS Lambda (Migration Period)**: 1M requests and 400,000 GB-seconds free monthly (will partially cover this)
- **Amazon Athena (Migration Validation)**: 10GB of data scanned per month free (minimal impact)
- **Amazon Kinesis (Migration Period)**: No free tier for Kinesis Data Streams

## Cost Scaling with Usage

The following table illustrates how cost estimates scale with different usage levels:

| Service | Low Usage | Medium Usage | High Usage |
|---------|-----------|--------------|------------|
| AWS Glue ETL Processing (One-time) | $7/month | $14/month | $28/month |
| Amazon S3 Storage (Migration Period) | $3/month | $6/month | $12/month |
| AWS Lambda (Migration Period) | $0/month | $1/month | $2/month |
| Amazon Athena (Migration Validation) | $0/month | $1/month | $3/month |
| Amazon Kinesis (Migration Period) | $0/month | $1/month | $2/month |

### Key Cost Factors

- **AWS Glue ETL Processing (One-time)**: ONE-TIME processing of 800GB of web events data with ETL transformations, quality scoring, and Iceberg format conversion
- **Amazon S3 Storage (Migration Period)**: Temporary storage during 3-7 day migration period for source data, intermediate processing files, and final Iceberg format data
- **AWS Lambda (Migration Period)**: Event processing, transformation functions, metadata operations, and batch coordination during migration
- **Amazon Athena (Migration Validation)**: One-time query processing for data validation, quality checks, and analytics setup after migration
- **Amazon Kinesis (Migration Period)**: Stream processing coordination during 3-7 day migration period (if used)

## Projected Costs Over Time

The following projections show estimated monthly costs over a 12-month period based on different growth patterns:

Base monthly cost calculation:

| Service | Monthly Cost |
|---------|-------------|
| AWS Glue ETL Processing (One-time) | $14.08 |
| Amazon S3 Storage (Migration Period) | $6.20 |
| AWS Lambda (Migration Period) | $1.07 |
| Amazon Athena (Migration Validation) | $1.60 |
| Amazon Kinesis (Migration Period) | $1.08 |
| **Total Monthly Cost** | **$24** |

| Growth Pattern | Month 1 | Month 3 | Month 6 | Month 12 |
|---------------|---------|---------|---------|----------|
| Steady | $24/mo | $24/mo | $24/mo | $24/mo |
| Moderate | $24/mo | $26/mo | $30/mo | $41/mo |
| Rapid | $24/mo | $29/mo | $38/mo | $68/mo |

* Steady: No monthly growth (1.0x)
* Moderate: 5% monthly growth (1.05x)
* Rapid: 10% monthly growth (1.1x)

## Detailed Cost Analysis

### Pricing Model

ON DEMAND


### Exclusions

- Ongoing monthly storage costs (calculated separately)
- Future incremental processing costs
- Data transfer costs between regions (assuming same region processing)
- Network egress charges for data access
- CloudWatch logging and monitoring costs
- IAM and other administrative service costs
- Development and testing environment costs
- Airflow or other orchestration service costs

### Ongoing Costs

| Key | Value |
|-----|-------|
| Aws S3 Tables/Iceberg Storage (Monthly Ongoing) | See nested table below |
#### Aws S3 Tables/Iceberg Storage (Monthly Ongoing)

| Key | Value |
|-----|-------|
| Usage | Monthly ongoing storage costs for final 530GB compressed Iceberg format data |
| Estimated Cost | $12.19 - $18.40 per month ongoing |
| Free Tier Info | No specific free tier for S3 Tables beyond standard S3 storage |
| Unit Pricing | See nested table below |
| Usage Quantities | See nested table below |
| Calculation Details | Storage: 540GB × $0.023 = $12.42/month ongoing | Request overhead: ~$0.50/month | Total: ~$12.92/month ongoing |
### Recommendations

#### Immediate Actions

- Use AWS Glue Flex ETL jobs ($0.29/DPU-hour) for 34% cost savings on the one-time migration ($9.28-$18.56 vs $14.08-$28.16)
- Process data in parallel batches to complete migration in 3-5 days and minimize storage costs during processing
- Enable S3 Intelligent Tiering immediately after migration to reduce ongoing storage costs by 20-40%
- Delete intermediate/temporary files immediately after successful processing to minimize storage costs



## Cost Optimization Recommendations

### Immediate Actions

- Use AWS Glue Flex ETL jobs ($0.29/DPU-hour) for 34% cost savings on the one-time migration ($9.28-$18.56 vs $14.08-$28.16)
- Process data in parallel batches to complete migration in 3-5 days and minimize storage costs during processing
- Enable S3 Intelligent Tiering immediately after migration to reduce ongoing storage costs by 20-40%

### Best Practices

- Regularly review costs with AWS Cost Explorer
- Consider reserved capacity for predictable workloads
- Implement automated scaling based on demand

## Conclusion

By following the recommendations in this report, you can optimize your One-Time Web Events Data Migration (800GB) costs while maintaining performance and reliability. Regular monitoring and adjustment of your usage patterns will help ensure cost efficiency as your workload evolves.
