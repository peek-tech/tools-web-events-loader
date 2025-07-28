# GitHub Repository Setup Instructions

## Prerequisites
1. Ensure you have the GitHub CLI (`gh`) installed
2. Ensure you're authenticated with the peek-tech organization

## Commands to Run

### 1. Create Repository on GitHub
```bash
gh repo create peek-tech/web-events-data-lake \
  --public \
  --description "AWS S3 Tables/Iceberg data lake for web events analytics with MongoDB integration and spatial 3D navigation support" \
  --add-readme=false
```

### 2. Add Remote and Push
```bash
git remote add origin https://github.com/peek-tech/web-events-data-lake.git
git branch -M main
git push -u origin main
```

### 3. Set Repository Topics (Optional)
```bash
gh repo edit peek-tech/web-events-data-lake \
  --add-topic aws \
  --add-topic s3-tables \
  --add-topic iceberg \
  --add-topic data-lake \
  --add-topic analytics \
  --add-topic mongodb \
  --add-topic cdk \
  --add-topic airflow \
  --add-topic dbt \
  --add-topic lambda \
  --add-topic glue \
  --add-topic virtual-tours \
  --add-topic spatial-analytics
```

## Repository Features Included

### 📊 **Complete Data Lake Architecture**
- **AWS S3 Tables** with Apache Iceberg format
- **Real-time processing** via Lambda + Kinesis
- **Batch processing** via Glue ETL jobs
- **Data orchestration** via Apache Airflow
- **Analytics modeling** via dbt

### 🏗️ **Infrastructure as Code**
- **AWS CDK** TypeScript infrastructure
- **Automated deployment** scripts
- **Environment configuration** management
- **IAM roles and policies** for S3 Tables

### 🧭 **MongoDB Integration**
- **Nested document support** for eventData structure
- **3D navigation analytics** (pitch, yaw, hfov coordinates)
- **Room-level granularity** (roomId, roomName)
- **Location hierarchy** (community, building, floorplan)
- **User authentication** context (userId + anonymousId)

### 📈 **Advanced Analytics**
- **Spatial analytics** for virtual tour navigation
- **User journey analysis** with authenticated/anonymous tracking
- **Time travel queries** for historical analysis
- **Data quality scoring** with spatial data bonuses
- **Performance optimization** for analytical workloads

### 🧪 **Comprehensive Testing**
- **Unit tests** for all components
- **Integration tests** for end-to-end pipeline
- **Performance tests** for large-scale processing
- **Error recovery** and resilience testing
- **MongoDB structure** validation tests

### 🚀 **Key Features**
- **ACID Transactions** via Iceberg
- **Schema Evolution** without breaking changes
- **Automated partitioning** and optimization
- **CloudWatch monitoring** and alerting
- **Single-command deployment**

## File Structure
```
web-events-data-lake/
├── README.md                   # Comprehensive project documentation
├── deploy.sh                   # Single-command deployment script
├── cdk/                        # AWS CDK infrastructure
├── lambda/                     # Real-time Kinesis processing
├── glue/                       # Batch ETL jobs
├── airflow/                    # Data pipeline orchestration
├── dbt/                        # Analytics data modeling
├── sql/                        # Schema definitions
├── tests/                      # Comprehensive test suite
├── monitoring/                 # CloudWatch dashboards
└── web-event-sample.json       # Sample event structure
```

## Next Steps After Repository Creation
1. Review the README.md for detailed setup instructions
2. Configure AWS credentials and environment variables
3. Run the deployment script: `./deploy.sh`
4. Set up CI/CD pipeline (optional)
5. Configure monitoring and alerting thresholds