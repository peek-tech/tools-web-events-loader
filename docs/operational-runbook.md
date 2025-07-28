# Operational Runbook

## System Overview
This runbook provides procedures for operating and maintaining the Web Events Data Loader system in production environments.

## Daily Operations Checklist

### Morning Health Check (9 AM UTC)
```bash
# 1. Check Airflow DAG status
airflow dags state web_events_s3_tables_pipeline $(date -d "yesterday" +%Y-%m-%d)

# 2. Verify Lambda function health
aws lambda get-function --function-name peek-web-events-kinesis-processor

# 3. Check Glue job status
aws glue get-job-runs --job-name peek-web-events-s3-processor --max-results 5

# 4. Validate S3 Tables data freshness
aws s3api head-object --bucket s3-tables-bucket --key "web-events/$(date +%Y/%m/%d)/"

# 5. Monitor data quality scores
dbt run-operation check_data_quality_trends
```

### End-of-Day Summary (6 PM UTC)
```bash
# 1. Check processing volumes
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --start-time $(date -d "1 day ago" --iso-8601) \
  --end-time $(date --iso-8601) \
  --period 3600 \
  --statistics Sum

# 2. Verify cost tracking
aws ce get-cost-and-usage \
  --time-period Start=$(date -d "1 day ago" +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity DAILY \
  --metrics BlendedCost

# 3. Generate daily analytics summary
dbt run --models marts.analytics.daily_summary
```

## Incident Response Procedures

### High Priority Alerts

#### 1. Data Processing Failure (P1)
**Alert**: Airflow DAG failure or Glue job failure
**Response Time**: 15 minutes

**Investigation Steps**:
```bash
# Check Airflow logs
airflow logs web_events_s3_tables_pipeline process_historical_data $(date +%Y-%m-%d)

# Examine Glue job errors
aws glue get-job-run --job-name peek-web-events-s3-processor --run-id [RUN_ID]

# Check CloudWatch error metrics
aws logs filter-log-events \
  --log-group-name /aws-glue/jobs/peek-web-events-s3-processor \
  --start-time $(date -d "2 hours ago" +%s)000
```

**Resolution Actions**:
1. **Data Quality Issues**: Run data validation queries
2. **Resource Limits**: Scale up Glue job resources
3. **Schema Changes**: Update dbt models and redeploy
4. **Access Issues**: Verify IAM permissions

**Recovery Procedure**:
```bash
# 1. Fix underlying issue
# 2. Clear failed task state
airflow tasks clear web_events_s3_tables_pipeline process_historical_data

# 3. Restart processing
airflow dags trigger web_events_s3_tables_pipeline

# 4. Monitor recovery
watch "airflow tasks state web_events_s3_tables_pipeline process_historical_data $(date +%Y-%m-%d)"
```

#### 2. High Error Rate (P1)
**Alert**: Lambda error rate > 5% or data quality score < 0.7
**Response Time**: 30 minutes

**Investigation**:
```bash
# Check Lambda errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/peek-web-events-kinesis-processor \
  --filter-pattern "ERROR" \
  --start-time $(date -d "1 hour ago" +%s)000

# Analyze error patterns
aws logs insights start-query \
  --log-group-name /aws/lambda/peek-web-events-kinesis-processor \
  --start-time $(date -d "2 hours ago" +%s) \
  --end-time $(date +%s) \
  --query-string 'fields @timestamp, @message | filter @message like /ERROR/ | stats count() by bin(5m)'
```

**Resolution**:
1. **Code Issues**: Deploy hotfix from staging environment
2. **Data Format Changes**: Update event parsing logic
3. **Resource Constraints**: Increase Lambda memory/timeout
4. **Downstream Issues**: Check S3 Tables and Glue connectivity

### Medium Priority Alerts

#### 3. Performance Degradation (P2)
**Alert**: Query response time > 10s or processing lag > 1 hour
**Response Time**: 2 hours

**Investigation**:
```bash
# Check Athena query performance
aws athena get-query-execution --query-execution-id [QUERY_ID]

# Monitor S3 Tables metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name BucketSizeBytes \
  --dimensions Name=BucketName,Value=s3-tables-bucket \
  --start-time $(date -d "1 day ago" --iso-8601) \
  --end-time $(date --iso-8601) \
  --period 3600 \
  --statistics Average
```

**Optimization Actions**:
1. **Table Maintenance**: Run compaction and optimize operations
2. **Query Optimization**: Update dbt models for better performance
3. **Partition Pruning**: Verify query predicates use partition columns
4. **Resource Scaling**: Increase Glue DPU allocation

#### 4. Data Quality Degradation (P2)  
**Alert**: Data quality score declining trend or bot traffic spike
**Response Time**: 4 hours

**Analysis**:
```sql
-- Check recent data quality trends
SELECT 
  event_date,
  AVG(data_quality_score) as avg_quality,
  COUNT(CASE WHEN is_bot THEN 1 END) * 100.0 / COUNT(*) as bot_percentage,
  COUNT(*) as total_events
FROM s3_tables.analytics.web_events 
WHERE event_date >= CURRENT_DATE - 7
GROUP BY event_date
ORDER BY event_date DESC;
```

**Remediation**:
1. **Update Bot Detection**: Enhance user agent patterns
2. **Quality Rules**: Adjust scoring weights based on data patterns
3. **Source Investigation**: Check for new event sources or formats
4. **Filtering Updates**: Update validation rules in Lambda function

## Maintenance Procedures

### Weekly Maintenance (Sundays, 2 AM UTC)

#### Table Optimization
```bash
# 1. Optimize S3 Tables for query performance
aws glue start-job-run \
  --job-name optimize-s3-tables \
  --arguments '--table-name=web_events,--optimize-type=compaction'

# 2. Update table statistics
dbt run-operation update_table_statistics

# 3. Clean up old staging files
aws s3 rm s3://staging-bucket/old-data/ --recursive \
  --exclude "*" --include "*/$(date -d '30 days ago' +%Y/%m/%d)/*"
```

#### Performance Monitoring
```bash
# Generate weekly performance report
dbt run --models reports.weekly_performance_summary

# Check resource utilization trends
aws cloudwatch get-metric-statistics \
  --namespace AWS/Glue \
  --metric-name glue.driver.aggregate.numCompletedTasks \
  --start-time $(date -d "7 days ago" --iso-8601) \
  --end-time $(date --iso-8601) \
  --period 86400 \
  --statistics Sum
```

### Monthly Maintenance (First Sunday, 1 AM UTC)

#### Cost Optimization Review
```bash
# 1. Analyze storage costs
aws ce get-cost-and-usage \
  --time-period Start=$(date -d "1 month ago" +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=DIMENSION,Key=SERVICE

# 2. Review and cleanup unused resources
aws s3api list-objects-v2 --bucket temp-processing-bucket \
  --query 'Contents[?LastModified<=`$(date -d "7 days ago" --iso-8601)`]'

# 3. Update cost allocation tags
aws resourcegroupstaggingapi tag-resources \
  --resource-arn-list [S3_TABLE_ARNS] \
  --tags Environment=Production,Project=WebEventsLoader
```

#### Security Audit
```bash
# 1. Review IAM permissions
aws iam get-role-policy --role-name WebEventsProcessorRole --policy-name DataAccessPolicy

# 2. Check encryption settings
aws s3api get-bucket-encryption --bucket s3-tables-bucket

# 3. Verify CloudTrail logging
aws cloudtrail describe-trails --trail-name-list web-events-audit-trail
```

## Backup and Recovery

### Daily Backup Verification
```bash
# Check S3 Tables backup status
aws s3 ls s3://backup-bucket/iceberg-snapshots/$(date +%Y/%m/%d)/

# Verify cross-region replication
aws s3api get-bucket-replication --bucket s3-tables-bucket
```

### Disaster Recovery Procedures

#### Complete System Recovery
1. **Restore Infrastructure**: `cdk deploy --all` in DR region
2. **Restore Data**: 
   ```bash
   # Restore from latest S3 Tables snapshot
   aws s3 sync s3://backup-bucket/iceberg-snapshots/latest/ s3://dr-s3-tables-bucket/
   ```
3. **Validate Data**: Run data quality checks and sample queries
4. **Resume Processing**: Start Airflow DAGs and verify real-time ingestion

#### Partial Data Recovery
```bash
# Restore specific date partition
aws s3 sync s3://backup-bucket/iceberg-snapshots/2024/01/15/ \
  s3://s3-tables-bucket/web-events/event_date=2024-01-15/

# Refresh table metadata
dbt run-operation refresh_table_partitions --args "{'table_name': 'web_events'}"
```

## Performance Monitoring

### Key Metrics Dashboard
- **Throughput**: Events/minute, Processing lag
- **Quality**: Data quality scores, Error rates
- **Performance**: Query response times, Job durations
- **Cost**: Daily spend, Resource utilization

### Alert Thresholds
```yaml
Critical Alerts:
  - DAG failure: Immediate
  - Error rate > 5%: 15 minutes
  - Data lag > 4 hours: 30 minutes

Warning Alerts:
  - Quality score < 0.8: 1 hour
  - Query time > 30s: 2 hours
  - Cost increase > 20%: Daily
```

### Capacity Planning
```bash
# Monitor growth trends
aws cloudwatch get-metric-statistics \
  --namespace Custom/WebEvents \
  --metric-name EventsPerDay \
  --start-time $(date -d "30 days ago" --iso-8601) \
  --end-time $(date --iso-8601) \
  --period 86400 \
  --statistics Sum

# Project resource needs
python scripts/capacity_planning.py --forecast-days 90
```

## Contact Information

### Escalation Matrix
- **L1 Support**: On-call engineer (15 min response)
- **L2 Support**: Data engineering team (30 min response)  
- **L3 Support**: Architecture team (2 hour response)

### Emergency Contacts
- **Primary**: Data Engineering Team Lead
- **Secondary**: Platform Engineering Manager
- **Escalation**: VP Engineering

### External Dependencies
- **AWS Support**: Business support plan with 1-hour response
- **Vendor Contacts**: dbt support, Airflow community

This runbook should be updated quarterly and after any significant system changes.