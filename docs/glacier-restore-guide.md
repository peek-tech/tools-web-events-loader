# S3 Glacier Restore Operations Guide

## Overview

This guide provides detailed instructions for restoring archived web events data from S3 Glacier storage using S3 Batch Operations before processing with the data lake pipeline.

## Prerequisites

- AWS CLI configured with appropriate permissions
- S3BatchOperationsRole deployed via CDK infrastructure
- Access to peek-inventory-bucket with daily inventory manifests
- Understanding of Glacier retrieval tiers and timing

## Glacier Restore Process

### Step 1: Analyze Inventory Manifest

The daily inventory manifest is available at:
```
s3://peek-inventory-bucket/prod-backup-web-event/archived-web-events/YYYY-MM-DDTHH-MMZ/
```

**Find the latest manifest:**
```bash
aws s3 ls s3://peek-inventory-bucket/prod-backup-web-event/archived-web-events/ --recursive | sort -k1,2 | tail -20
```

**Examine manifest contents:**
```bash
# Download and inspect the manifest.json file
aws s3 cp s3://peek-inventory-bucket/prod-backup-web-event/archived-web-events/2025-07-27T01-00Z/manifest.json ./

# Check CSV inventory files
aws s3 ls s3://peek-inventory-bucket/prod-backup-web-event/archived-web-events/2025-07-27T01-00Z/data/
```

### Step 2: Create S3 Batch Operations Restore Job

**Basic restore job creation:**
```bash
aws s3control create-job \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --confirmation-required false \
  --operation '{
    "S3RestoreObject": {
      "ExpirationInDays": 7,
      "GlacierJobTier": "Standard",
      "Metadata": {
        "restored-by": "web-events-data-migration",
        "restore-date": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
      }
    }
  }' \
  --manifest '{
    "Spec": {
      "Format": "S3InventoryReport_CSV_20161130",
      "Fields": ["Bucket", "Key", "StorageClass"]
    },
    "Location": {
      "ObjectArn": "arn:aws:s3:::peek-inventory-bucket/prod-backup-web-event/archived-web-events/2025-07-27T01-00Z/manifest.json",
      "ETag": "MANIFEST_ETAG"
    }
  }' \
  --priority 100 \
  --role-arn "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/S3BatchOperationsRole" \
  --report '{
    "Bucket": "arn:aws:s3:::peek-batch-operations-reports-'$(aws sts get-caller-identity --query Account --output text)'",
    "Format": "Report_CSV_20180820",
    "Enabled": true,
    "Prefix": "glacier-restore-reports/prod-backup-web-events/",
    "ReportScope": "AllTasks"
  }' \
  --description "Glacier restore for prod-backup-web-events data migration" \
  --tags '[
    {"Key": "Project", "Value": "WebEventsDataMigration"},
    {"Key": "Environment", "Value": "Production"},
    {"Key": "CostCenter", "Value": "DataEngineering"}
  ]'
```

**Get the manifest ETag:**
```bash
aws s3api head-object \
  --bucket peek-inventory-bucket \
  --key prod-backup-web-event/archived-web-events/2025-07-27T01-00Z/manifest.json \
  --query 'ETag' --output text | tr -d '"'
```

### Step 3: Monitor Restore Job

**Check job status:**
```bash
# List all active jobs
aws s3control list-jobs \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --job-statuses Active,Complete,Failed,Cancelled,Cancelling,New,Preparing,Ready,Suspended

# Get detailed job information
aws s3control describe-job \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --job-id YOUR_JOB_ID
```

**Monitor progress:**
```bash
# Check progress summary (run periodically)
aws s3control describe-job \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --job-id YOUR_JOB_ID \
  --query 'Job.ProgressSummary'
```

### Step 4: Verify Restore Completion

**Check individual object restore status:**
```bash
# Verify restore status of a sample object
aws s3api head-object \
  --bucket prod-backup-web-events \
  --key "archived-web-events/2024/01/15/12/sample-file.json" \
  --query 'Restore'
```

**Successful restore shows:**
```json
"ongoing-request=\"false\", expiry-date=\"Sat, 03 Aug 2025 00:00:00 GMT\""
```

## Glacier Retrieval Options

### Standard Retrieval (Recommended)
- **Cost**: $0.01 per GB
- **Time**: 3-5 hours
- **Use case**: Standard migration timeline

### Bulk Retrieval (Cost-Optimized)
- **Cost**: $0.00 per GB
- **Time**: 5-12 hours
- **Use case**: Non-urgent migrations

### Expedited Retrieval (Time-Critical)
- **Cost**: $0.03 per GB
- **Time**: 1-5 minutes
- **Use case**: Emergency data recovery

## Cost Estimation

For 800GB restore using Standard retrieval:
- **Glacier retrieval**: $0.01 × 800GB = $8.00
- **Batch Operations job**: $0.25
- **Batch Operations objects**: $1.00 × (estimated objects in millions)
- **Total estimated cost**: $10.25 - $16.25

## Best Practices

### Timing and Coordination
1. **Plan ahead**: Start restore 3-5 days before ETL processing
2. **Monitor capacity**: Ensure adequate Glue ETL capacity is available
3. **Coordinate teams**: Notify data engineering team of restore timeline

### Cost Optimization
1. **Use Standard retrieval** for most migrations
2. **Filter inventory manifest** to restore only necessary objects
3. **Set appropriate expiration** (7 days recommended)
4. **Monitor restore usage** with AWS Cost Explorer

### Error Handling
1. **Check job reports** in the reports bucket
2. **Retry failed objects** individually if needed
3. **Validate sample objects** before proceeding with ETL
4. **Document issues** for future migrations

## Troubleshooting

### Common Issues

**Job Creation Fails**
```bash
# Check IAM role permissions
aws iam get-role --role-name S3BatchOperationsRole
aws iam list-attached-role-policies --role-name S3BatchOperationsRole
```

**Manifest Not Found**
```bash
# Verify manifest path and ETag
aws s3 ls s3://peek-inventory-bucket/prod-backup-web-event/archived-web-events/ --recursive
aws s3api head-object --bucket peek-inventory-bucket --key MANIFEST_PATH
```

**Objects Still in Glacier**
```bash
# Check restore status and wait time
aws s3api head-object --bucket prod-backup-web-events --key OBJECT_KEY
# Standard retrieval takes 3-5 hours
```

### Monitoring and Alerts

**Set up CloudWatch alerts for:**
- Batch Operations job failures
- Unexpected costs
- Long-running jobs (>6 hours for Standard retrieval)

**Cost monitoring:**
```bash
# Check current month costs
aws ce get-cost-and-usage \
  --time-period Start=2025-07-01,End=2025-07-31 \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=DIMENSION,Key=SERVICE
```

## Integration with ETL Pipeline

After successful restore:

1. **Verify data accessibility**:
   ```bash
   aws s3 ls s3://prod-backup-web-events/archived-web-events/ --recursive | head -20
   ```

2. **Trigger Glue ETL jobs**:
   ```bash
   aws glue start-job-run --job-name peek-web-events-s3-processor-iceberg
   ```

3. **Monitor ETL progress**:
   ```bash
   aws glue get-job-runs --job-name peek-web-events-s3-processor-iceberg --max-items 5
   ```

## Cleanup

After successful ETL processing:

1. **Verify data in S3 Tables**:
   ```sql
   -- Query the final Iceberg table
   SELECT COUNT(*) FROM s3_tables.analytics.web_events
   WHERE event_date >= '2024-01-01';
   ```

2. **Objects automatically expire** after 7 days (no manual cleanup needed)

3. **Clean up reports** (optional):
   ```bash
   aws s3 rm s3://peek-batch-operations-reports-ACCOUNT/glacier-restore-reports/ --recursive
   ```

## Support and Documentation

- **AWS S3 Batch Operations**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html
- **Glacier Restore Pricing**: https://aws.amazon.com/s3/pricing/
- **Internal Runbook**: See `docs/operational-runbook.md`
- **Cost Analysis**: See `COST_ESTIMATE.md`