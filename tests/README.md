# S3 Tables/Iceberg Data Lake Test Suite

This directory contains comprehensive tests for the S3 Tables/Iceberg data lake implementation, ensuring enterprise-grade quality and reliability.

## Test Structure

```
tests/
├── unit/                    # Unit tests for individual components
│   ├── test_lambda_iceberg.py
│   ├── test_cdk_infrastructure.py
│   └── test_airflow_dags.py
├── integration/             # Integration tests for component interactions
│   └── test_glue_iceberg_etl.py
├── e2e/                    # End-to-end pipeline tests
│   └── test_s3_tables_pipeline_e2e.py
├── requirements.txt        # Test dependencies
└── README.md              # This file
```

## Test Coverage

### Unit Tests

1. **Lambda Function Tests** (`test_lambda_iceberg.py`)
   - ✅ Event processing and enrichment
   - ✅ Data quality scoring
   - ✅ Bot detection
   - ✅ S3 Tables integration
   - ✅ Error handling and recovery
   - ✅ Batch processing performance

2. **CDK Infrastructure Tests** (`test_cdk_infrastructure.py`)
   - ✅ S3 Tables resource creation
   - ✅ IAM permissions validation
   - ✅ Glue job configurations
   - ✅ Monitoring and alarms
   - ✅ Resource dependencies

3. **Airflow DAG Tests** (`test_airflow_dags.py`)
   - ✅ DAG structure validation
   - ✅ Task dependencies
   - ✅ S3 Tables operations
   - ✅ Maintenance schedules
   - ✅ Error handling strategies

### Integration Tests

1. **Glue ETL Tests** (`test_glue_iceberg_etl.py`)
   - ✅ Iceberg table creation
   - ✅ MERGE operations
   - ✅ Time travel capabilities
   - ✅ Schema evolution
   - ✅ Concurrent write handling
   - ✅ Performance optimization

### End-to-End Tests

1. **Complete Pipeline Tests** (`test_s3_tables_pipeline_e2e.py`)
   - ✅ Full data flow validation
   - ✅ Multi-layer consistency
   - ✅ High volume processing
   - ✅ Error recovery mechanisms
   - ✅ Performance benchmarks

### dbt Model Tests

1. **Iceberg Model Tests** (`dbt/tests/`)
   - ✅ Data quality validations
   - ✅ Referential integrity
   - ✅ Time travel consistency
   - ✅ Performance metrics
   - ✅ Schema evolution compatibility

## Running Tests

### Prerequisites

```bash
# Install test dependencies
pip install -r tests/requirements.txt

# Set up AWS credentials (for integration tests)
export AWS_PROFILE=test
export AWS_REGION=us-east-1
```

### Running All Tests

```bash
# Run all tests with coverage
pytest tests/ --cov=. --cov-report=html

# Run tests in parallel
pytest tests/ -n auto

# Run with verbose output
pytest tests/ -v
```

### Running Specific Test Suites

```bash
# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# End-to-end tests only
pytest tests/e2e/

# Specific test file
pytest tests/unit/test_lambda_iceberg.py

# Specific test method
pytest tests/unit/test_lambda_iceberg.py::TestLambdaIcebergProcessor::test_successful_event_processing
```

### Running dbt Tests

```bash
cd dbt/

# Run all dbt tests
dbt test

# Run tests for specific model
dbt test --select fct_web_events_iceberg

# Run tests with specific tag
dbt test --select tag:iceberg
```

## Test Configuration

### Environment Variables

```bash
# Test environment
export ENVIRONMENT=test
export LOG_LEVEL=DEBUG

# AWS resources (mocked for unit tests)
export TABLE_BUCKET_ARN=arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket
export NAMESPACE=test_analytics
export WEB_EVENTS_TABLE=test_web_events
```

### Performance Benchmarks

Expected performance metrics:

- **Lambda Processing**: < 100ms per event
- **Glue ETL**: < 5 minutes for 1GB batch
- **Athena Queries**: < 5 seconds for daily aggregations
- **Time Travel Queries**: < 10 seconds for 7-day lookback
- **dbt Transformations**: < 10 minutes for full refresh

## Test Data

### Generating Test Data

```python
# Use the test data generator
from tests.utils import generate_web_events

# Generate 1000 test events
test_events = generate_web_events(count=1000, 
                                 sessions=50,
                                 users=200,
                                 date_range_days=7)
```

### Test Scenarios

1. **Normal Operations**
   - Standard web event flow
   - Virtual tour interactions
   - User journey tracking

2. **Edge Cases**
   - Missing data fields
   - Malformed events
   - Duplicate events
   - Out-of-order events

3. **Failure Scenarios**
   - Service outages
   - Network failures
   - Data corruption
   - Schema mismatches

4. **Scale Testing**
   - 75GB historical data migration
   - 10K events/second streaming
   - 100M records query performance

## CI/CD Integration

### GitHub Actions

```yaml
name: Test Pipeline
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - run: pip install -r tests/requirements.txt
      - run: pytest tests/ --cov=. --cov-report=xml
      - uses: codecov/codecov-action@v3
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: unit-tests
        name: Unit Tests
        entry: pytest tests/unit/
        language: system
        pass_filenames: false
```

## Debugging Failed Tests

### Common Issues

1. **Import Errors**
   ```bash
   # Ensure Python path includes project root
   export PYTHONPATH="${PYTHONPATH}:$(pwd)"
   ```

2. **AWS Credential Issues**
   ```bash
   # Use moto for local testing
   export MOTO_MOCK_AWS_SERVICES=true
   ```

3. **Timeout Issues**
   ```bash
   # Increase test timeout
   pytest tests/ --timeout=300
   ```

### Test Logs

- Unit test logs: `tests/logs/unit/`
- Integration test logs: `tests/logs/integration/`
- Coverage reports: `htmlcov/index.html`

## Contributing

When adding new features:

1. Write tests first (TDD approach)
2. Ensure 90%+ code coverage
3. Add appropriate test documentation
4. Update this README with new test information

## Test Maintenance

### Weekly Tasks
- Review and update test data
- Check for deprecated assertions
- Update mock responses

### Monthly Tasks
- Performance benchmark review
- Test coverage analysis
- Dependency updates

### Quarterly Tasks
- Full test suite audit
- Scale testing validation
- Security test review