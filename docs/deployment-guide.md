# Deployment Guide

## Overview
This guide provides step-by-step instructions for deploying the Web Events Data Loader system across different environments.

## Prerequisites

### Required Tools
```bash
# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# Install Node.js 18+ for CDK
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# Install AWS CDK
npm install -g aws-cdk

# Install Python 3.9+
sudo apt-get install python3.9 python3.9-pip python3.9-venv

# Install dbt
pip install dbt-core dbt-athena-community

# Install Airflow (optional, for local testing)
pip install apache-airflow==2.8.0
```

### AWS Account Setup
```bash
# Configure AWS credentials
aws configure
# Enter: Access Key, Secret Key, Region (us-east-1), Output format (json)

# Verify permissions
aws sts get-caller-identity
aws s3 ls  # Should list buckets without error
```

### Required AWS Permissions
The deployment user/role needs the following services:
- **IAM**: Create roles, policies, and service-linked roles
- **S3**: Create buckets, manage S3 Tables
- **Lambda**: Create and update functions
- **Glue**: Create jobs, crawlers, and databases
- **Kinesis**: Create and manage streams
- **CloudWatch**: Create alarms and log groups
- **Systems Manager**: Parameter store access

## Environment Configuration

### 1. Development Environment
```bash
# Create development configuration
cat > cdk/config/dev.json << EOF
{
  "environment": "dev",
  "region": "us-east-1",
  "account": "123456789012",
  "dataRetentionDays": 7,
  "enableDetailedMonitoring": false,
  "glueWorkerType": "G.1X",
  "glueNumberOfWorkers": 2,
  "lambdaMemorySize": 512,
  "kinesisShardCount": 1,
  "s3TablesEnabled": true,
  "tags": {
    "Environment": "Development",
    "Project": "WebEventsLoader",
    "Owner": "DataEngineering"
  }
}
EOF
```

### 2. Staging Environment
```bash
# Create staging configuration
cat > cdk/config/staging.json << EOF
{
  "environment": "staging",
  "region": "us-east-1", 
  "account": "123456789012",
  "dataRetentionDays": 30,
  "enableDetailedMonitoring": true,
  "glueWorkerType": "G.2X",
  "glueNumberOfWorkers": 5,
  "lambdaMemorySize": 1024,
  "kinesisShardCount": 2,
  "s3TablesEnabled": true,
  "tags": {
    "Environment": "Staging",
    "Project": "WebEventsLoader",
    "Owner": "DataEngineering"
  }
}
EOF
```

### 3. Production Environment
```bash
# Create production configuration
cat > cdk/config/prod.json << EOF
{
  "environment": "prod",
  "region": "us-east-1",
  "account": "123456789012", 
  "dataRetentionDays": 2555,  # 7 years
  "enableDetailedMonitoring": true,
  "glueWorkerType": "G.2X",
  "glueNumberOfWorkers": 10,
  "lambdaMemorySize": 2048,
  "kinesisShardCount": 5,
  "s3TablesEnabled": true,
  "enableBackup": true,
  "enableCrossRegionReplication": true,
  "tags": {
    "Environment": "Production",
    "Project": "WebEventsLoader",
    "Owner": "DataEngineering",
    "CostCenter": "DataPlatform"
  }
}
EOF
```

## Secret Management

### 1. Store Sensitive Configuration
```bash
# Database credentials (if using external DB)
aws ssm put-parameter \
  --name "/web-events-loader/dev/db-password" \
  --value "your-secure-password" \
  --type "SecureString" \
  --description "Database password for dev environment"

# API keys for external services
aws ssm put-parameter \
  --name "/web-events-loader/dev/external-api-key" \
  --value "your-api-key" \
  --type "SecureString"

# dbt profiles configuration
aws ssm put-parameter \
  --name "/web-events-loader/dev/dbt-profile" \
  --value '{
    "web_events": {
      "outputs": {
        "dev": {
          "type": "athena",
          "s3_staging_dir": "s3://aws-athena-query-results-dev/",
          "schema": "web_events_dev",
          "database": "web_events",
          "region_name": "us-east-1"
        }
      },
      "target": "dev"
    }
  }' \
  --type "SecureString"
```

### 2. IAM Role for Parameter Access
```bash
# The CDK stack creates these automatically, but for reference:
aws iam create-role \
  --role-name WebEventsParameterAccessRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "lambda.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'
```

## Step-by-Step Deployment

### Phase 1: Infrastructure Deployment

#### 1. Bootstrap CDK (First Time Only)
```bash
cd cdk
npm install

# Bootstrap CDK in your account/region
cdk bootstrap aws://123456789012/us-east-1
```

#### 2. Deploy Base Infrastructure
```bash
# Set environment
export ENVIRONMENT=dev  # or staging, prod

# Deploy infrastructure stack
cdk deploy WebEventsDataLakeStack-${ENVIRONMENT} \
  --require-approval never \
  --parameters Environment=${ENVIRONMENT}

# Verify deployment
aws cloudformation describe-stacks \
  --stack-name WebEventsDataLakeStack-${ENVIRONMENT} \
  --query 'Stacks[0].StackStatus'
```

#### 3. Validate Infrastructure
```bash
# Check S3 Tables bucket
aws s3 ls s3://web-events-s3-tables-${ENVIRONMENT}/

# Verify Lambda function
aws lambda get-function \
  --function-name web-events-kinesis-processor-${ENVIRONMENT}

# Check Kinesis stream
aws kinesis describe-stream \
  --stream-name web-events-stream-${ENVIRONMENT}

# Verify Glue database
aws glue get-database --name web_events_${ENVIRONMENT}
```

### Phase 2: Application Deployment

#### 1. Deploy Lambda Functions
```bash
# Package and deploy Lambda
cd lambda/kinesis-processor
zip -r function.zip .
aws lambda update-function-code \
  --function-name web-events-kinesis-processor-${ENVIRONMENT} \
  --zip-file fileb://function.zip

# Test Lambda function
aws lambda invoke \
  --function-name web-events-kinesis-processor-${ENVIRONMENT} \
  --payload '{"Records":[{"kinesis":{"data":"eyJ0ZXN0IjoidHJ1ZSJ9"}}]}' \
  response.json
```

#### 2. Deploy Glue ETL Jobs
```bash
# Upload Glue scripts to S3
aws s3 cp glue/s3_processor_iceberg.py \
  s3://web-events-glue-scripts-${ENVIRONMENT}/s3_processor_iceberg.py

aws s3 cp glue/kinesis_processor_iceberg.py \
  s3://web-events-glue-scripts-${ENVIRONMENT}/kinesis_processor_iceberg.py

# Update Glue job definitions (done automatically by CDK)
```

#### 3. Initialize dbt Project
```bash
cd dbt

# Create profiles directory
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
web_events:
  outputs:
    ${ENVIRONMENT}:
      type: athena
      s3_staging_dir: s3://aws-athena-query-results-${ENVIRONMENT}/
      schema: web_events_${ENVIRONMENT}
      database: web_events
      region_name: us-east-1
      threads: 4
  target: ${ENVIRONMENT}
EOF

# Install dbt dependencies
dbt deps

# Test dbt connection
dbt debug --target ${ENVIRONMENT}

# Run initial dbt models
dbt run --target ${ENVIRONMENT}
dbt test --target ${ENVIRONMENT}
```

### Phase 3: Orchestration Setup

#### 1. Deploy Airflow DAGs
```bash
# If using Amazon MWAA
aws s3 cp airflow/dags/web_events_s3_tables_pipeline.py \
  s3://your-mwaa-bucket-${ENVIRONMENT}/dags/

# If using self-hosted Airflow
cp airflow/dags/* $AIRFLOW_HOME/dags/

# Set Airflow variables
airflow variables set aws_account_id 123456789012
airflow variables set environment ${ENVIRONMENT}
airflow variables set source_s3_bucket web-events-raw-${ENVIRONMENT}
airflow variables set s3_tables_bucket web-events-s3-tables-${ENVIRONMENT}
```

#### 2. Configure Monitoring
```bash
# Import CloudWatch dashboard
aws cloudwatch put-dashboard \
  --dashboard-name "WebEventsLoader-${ENVIRONMENT}" \
  --dashboard-body file://monitoring/cloudwatch_dashboard.json

# Create CloudWatch alarms
aws cloudwatch put-metric-alarm \
  --alarm-name "WebEvents-LambdaErrors-${ENVIRONMENT}" \
  --alarm-description "High error rate in Lambda function" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=FunctionName,Value=web-events-kinesis-processor-${ENVIRONMENT} \
  --evaluation-periods 2
```

## Post-Deployment Validation

### 1. End-to-End Testing
```bash
# Send test event to Kinesis
aws kinesis put-record \
  --stream-name web-events-stream-${ENVIRONMENT} \
  --data '{"type":"test","timestamp":"2024-01-01T00:00:00Z","anonymousId":"test-user"}' \
  --partition-key test-user

# Wait 2-3 minutes, then query results
aws athena start-query-execution \
  --query-string "SELECT * FROM web_events_${ENVIRONMENT}.web_events WHERE anonymous_id = 'test-user' LIMIT 10" \
  --result-configuration OutputLocation=s3://aws-athena-query-results-${ENVIRONMENT}/
```

### 2. Data Quality Validation
```bash
# Run data quality tests
dbt test --target ${ENVIRONMENT}

# Check processing metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=web-events-kinesis-processor-${ENVIRONMENT} \
  --start-time $(date -d "1 hour ago" --iso-8601) \
  --end-time $(date --iso-8601) \
  --period 300 \
  --statistics Sum
```

### 3. Performance Validation  
```bash
# Test query performance
time aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM web_events_${ENVIRONMENT}.web_events WHERE event_date = CURRENT_DATE" \
  --result-configuration OutputLocation=s3://aws-athena-query-results-${ENVIRONMENT}/

# Check S3 Tables optimization
aws glue start-job-run \
  --job-name optimize-s3-tables-${ENVIRONMENT} \
  --arguments --table-name=web_events,--optimize-type=compaction
```

## Rollback Procedures

### 1. Application Rollback
```bash
# Rollback Lambda function
aws lambda update-function-code \
  --function-name web-events-kinesis-processor-${ENVIRONMENT} \
  --s3-bucket deployment-artifacts-${ENVIRONMENT} \
  --s3-key lambda/previous-version.zip

# Rollback dbt models
cd dbt
git checkout HEAD~1  # Go back one commit
dbt run --target ${ENVIRONMENT}
```

### 2. Infrastructure Rollback
```bash
# Rollback CDK stack to previous version
git checkout [PREVIOUS_COMMIT]
cd cdk
cdk deploy WebEventsDataLakeStack-${ENVIRONMENT} --require-approval never

# Or destroy and redeploy if needed
cdk destroy WebEventsDataLakeStack-${ENVIRONMENT}
# Wait for cleanup to complete
cdk deploy WebEventsDataLakeStack-${ENVIRONMENT}
```

### 3. Data Recovery
```bash
# Restore from S3 Tables snapshot
aws s3 sync s3://backup-bucket-${ENVIRONMENT}/snapshots/[TIMESTAMP]/ \
  s3://web-events-s3-tables-${ENVIRONMENT}/

# Refresh table metadata
dbt run-operation refresh_table_partitions --args "{'table_name': 'web_events'}"
```

## Automated Deployment (CI/CD)

### GitHub Actions Workflow
```yaml
# .github/workflows/deploy.yml
name: Deploy Web Events Loader
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'
      - name: Install CDK
        run: npm install -g aws-cdk
      - name: Configure AWS
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Deploy Infrastructure
        run: |
          cd cdk
          npm install
          cdk deploy --require-approval never
      - name: Deploy dbt Models
        run: |
          cd dbt
          dbt run --target prod
          dbt test --target prod
```

## Troubleshooting

### Common Deployment Issues

#### 1. CDK Bootstrap Errors
```bash
# Error: "Need to perform AWS CDK bootstrap"
# Solution: Run bootstrap command
cdk bootstrap aws://[ACCOUNT]/[REGION]
```

#### 2. Permission Errors
```bash
# Error: "User is not authorized to perform: iam:CreateRole"
# Solution: Ensure deployment user has sufficient IAM permissions
aws sts get-caller-identity
aws iam list-attached-user-policies --user-name [USERNAME]
```

#### 3. S3 Tables Conflicts
```bash
# Error: "Table already exists"
# Solution: Use different table names per environment
export TABLE_SUFFIX="-${ENVIRONMENT}"
```

#### 4. dbt Connection Issues
```bash
# Error: "Could not connect to Athena"
# Solution: Verify AWS credentials and region
dbt debug --target ${ENVIRONMENT}
aws athena list-databases
```

## Environment-Specific Notes

### Development
- Use smaller instance sizes for cost optimization
- Enable detailed logging for debugging
- Shorter data retention periods
- Single AZ deployment acceptable

### Staging
- Production-like configuration
- Full monitoring enabled
- Automated testing integration
- Load testing capabilities

### Production
- Multi-AZ deployment
- Enhanced monitoring and alerting
- Automated backups enabled
- Cross-region replication
- Strict change management

This deployment guide should be updated after any infrastructure changes and validated with each release.