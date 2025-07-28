#!/bin/bash

# Web Events Data Lake Deployment Script
# This script deploys the complete data lake infrastructure and analytics pipeline

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'  
NC='\033[0m' # No Color

# Configuration
AWS_REGION=${AWS_REGION:-us-east-1}
ENVIRONMENT=${ENVIRONMENT:-dev}
SOURCE_BUCKET=${SOURCE_BUCKET:-your-source-s3-bucket}

echo -e "${BLUE}🚀 Starting Web Events Data Lake Deployment${NC}"
echo -e "Environment: ${ENVIRONMENT}"
echo -e "AWS Region: ${AWS_REGION}"
echo -e "Source Bucket: ${SOURCE_BUCKET}"

# Function to print status
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
echo -e "${BLUE}📋 Checking prerequisites...${NC}"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI not found. Please install AWS CLI."
    exit 1
fi

# Check Node.js for CDK
if ! command -v node &> /dev/null; then
    print_error "Node.js not found. Please install Node.js 18+."
    exit 1
fi

# Check Python for Glue/Lambda
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 not found. Please install Python 3.9+."
    exit 1
fi

# Check CDK CLI
if ! command -v cdk &> /dev/null; then
    print_warning "CDK CLI not found. Installing..."
    npm install -g aws-cdk
fi

print_status "Prerequisites check completed"

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo -e "AWS Account ID: ${AWS_ACCOUNT_ID}"

# Step 1: Deploy Infrastructure
echo -e "${BLUE}🏗️  Deploying AWS Infrastructure...${NC}"

cd cdk

# Install dependencies
print_status "Installing CDK dependencies..."
npm install

# Bootstrap CDK (if not already done)
print_status "Bootstrapping CDK..."
cdk bootstrap aws://${AWS_ACCOUNT_ID}/${AWS_REGION} || print_warning "CDK already bootstrapped"

# Deploy infrastructure stack
print_status "Deploying infrastructure stack..."
cdk deploy WebEventsDataLakeStack \
    --parameters sourceS3Bucket=${SOURCE_BUCKET} \
    --require-approval never

if [ $? -eq 0 ]; then
    print_status "Infrastructure deployment completed successfully"
else
    print_error "Infrastructure deployment failed"
    exit 1
fi

cd ..

# Step 2: Upload Glue Scripts (Updated for S3 Tables/Iceberg)
echo -e "${BLUE}📦 Uploading Glue ETL Scripts for S3 Tables...${NC}"

GLUE_SCRIPTS_BUCKET="peek-glue-scripts-${AWS_ACCOUNT_ID}"

aws s3 cp glue/kinesis_processor_iceberg.py s3://${GLUE_SCRIPTS_BUCKET}/scripts/
aws s3 cp glue/s3_processor_iceberg.py s3://${GLUE_SCRIPTS_BUCKET}/scripts/

print_status "Glue scripts uploaded"

# Step 3: Create Lambda deployment package (Updated for S3 Tables)
echo -e "${BLUE}📦 Creating Lambda deployment package for S3 Tables...${NC}"

cd lambda/kinesis-processor
# Create deployment package with Iceberg-optimized code
cp index_iceberg.py index.py  # Use the S3 Tables version
zip -r lambda-deployment.zip . -x "*.pyc" "__pycache__/*"

# Update Lambda function code
aws lambda update-function-code \
    --function-name KinesisProcessor \
    --zip-file fileb://lambda-deployment.zip

print_status "Lambda function updated"
cd ../..

# Step 4: Setup Airflow (if MWAA is used)
echo -e "${BLUE}🌬️  Setting up Airflow...${NC}"

# Create Airflow S3 bucket for DAGs (if using MWAA)
AIRFLOW_BUCKET="peek-airflow-${AWS_ACCOUNT_ID}"

# Check if bucket exists
if aws s3 ls "s3://${AIRFLOW_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    print_status "Creating Airflow S3 bucket..."
    aws s3 mb s3://${AIRFLOW_BUCKET}
else
    print_status "Airflow S3 bucket already exists"
fi

# Upload DAGs (S3 Tables optimized)
aws s3 sync airflow/dags/ s3://${AIRFLOW_BUCKET}/dags/

print_status "Airflow DAGs uploaded"

# Step 5: Initialize dbt
echo -e "${BLUE}🔧 Initializing dbt analytics...${NC}"

cd dbt

# Install dbt dependencies (if using packages)
if [ -f "packages.yml" ]; then
    dbt deps
fi

# Test dbt connection
print_status "Testing dbt connection..."
dbt debug

# Run initial models
print_status "Running dbt models..."
dbt run --target ${ENVIRONMENT}

# Run dbt tests
print_status "Running dbt tests..."
dbt test --target ${ENVIRONMENT}

print_status "dbt setup completed"
cd ..

# Step 6: Create S3 Tables (Iceberg) schemas
echo -e "${BLUE}🗃️  Creating S3 Tables (Iceberg) schemas...${NC}"

# Execute S3 Tables schema creation
aws athena start-query-execution \
    --query-string "$(cat sql/iceberg_schemas.sql)" \
    --result-configuration OutputLocation=s3://peek-web-events-datalake-${AWS_ACCOUNT_ID}/athena-results/ \
    --work-group peek-web-events-workgroup

# Wait for S3 Tables setup to complete
sleep 30

print_status "S3 Tables (Iceberg) schemas created"

# Step 7: Setup CloudWatch Dashboard
echo -e "${BLUE}📊 Setting up monitoring dashboard...${NC}"

# Create CloudWatch dashboard
aws cloudwatch put-dashboard \
    --dashboard-name "WebEventsDataLakeDashboard" \
    --dashboard-body file://monitoring/cloudwatch_dashboard.json

print_status "CloudWatch dashboard created"

# Step 8: Run initial S3 Tables validation
echo -e "${BLUE}✅ Running S3 Tables validation...${NC}"

# Check S3 bucket access
aws s3 ls s3://peek-web-events-datalake-${AWS_ACCOUNT_ID}/ > /dev/null
print_status "S3 data lake bucket accessible"

# Check S3 Tables bucket
TABLE_BUCKET_NAME="peek-web-events-tables-${AWS_ACCOUNT_ID}"
aws s3tables list-tables --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} > /dev/null 2>&1 || print_warning "S3 Tables may still be initializing"
print_status "S3 Tables bucket accessible"

# Check Kinesis stream
aws kinesis describe-stream --stream-name peek-web-events-stream > /dev/null
print_status "Kinesis stream accessible"

# Check Glue database
aws glue get-database --name peek_web_events > /dev/null
print_status "Glue database accessible"

echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo -e ""
echo -e "${BLUE}📋 Next Steps (S3 Tables/Iceberg):${NC}"
echo -e "1. Configure your web application to send events to Kinesis stream: peek-web-events-stream"
echo -e "2. Trigger S3 Tables historical migration:"
echo -e "   airflow dags trigger web_events_s3_tables_migration --conf '{\"source_bucket\": \"${SOURCE_BUCKET}\", \"account_id\": \"${AWS_ACCOUNT_ID}\"}'"
echo -e "3. Start real-time processing:"
echo -e "   airflow dags trigger web_events_s3_tables_realtime"
echo -e "4. Monitor the pipeline at: https://console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=WebEventsDataLakeDashboard"
echo -e "5. Query S3 Tables using Athena workgroup: peek-web-events-workgroup"
echo -e ""
echo -e "${BLUE}📖 Resources (S3 Tables/Iceberg):${NC}"
echo -e "• Data Lake Bucket: s3://peek-web-events-datalake-${AWS_ACCOUNT_ID}"
echo -e "• S3 Tables Bucket: s3://peek-web-events-tables-${AWS_ACCOUNT_ID}"
echo -e "• Kinesis Stream: peek-web-events-stream"  
echo -e "• Athena Database: peek_web_events"
echo -e "• S3 Tables Namespace: s3_tables.analytics"
echo -e "• Redshift Cluster: webevents-redshift"
echo -e "• dbt Documentation: Run 'dbt docs serve' in the dbt/ directory"
echo -e "• Iceberg Time Travel: Use 'FOR TIMESTAMP AS OF' in queries"

# Optional: Run a test event
echo -e "${YELLOW}🧪 Would you like to send a test event to validate the pipeline? (y/n)${NC}"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo -e "${BLUE}📤 Sending test event...${NC}"
    
    # Create test event based on sample
    TEST_EVENT=$(cat web-event-sample.json | base64)
    
    # Send to Kinesis
    aws kinesis put-record \
        --stream-name peek-web-events-stream \
        --partition-key "test-user" \
        --data "${TEST_EVENT}"
    
    print_status "Test event sent! Check CloudWatch logs for processing confirmation."
fi

echo -e "${GREEN}🚀 Web Events S3 Tables Data Lake with Iceberg is ready for production!${NC}"