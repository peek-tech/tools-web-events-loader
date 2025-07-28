"""
Unit tests for deployment script validation and error handling
Tests deployment sequence, S3 Tables validation, prerequisite checks, and error recovery.
"""

import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
import subprocess
import os
import tempfile
from pathlib import Path


class TestDeploymentScriptStructure:
    """Test deployment script structure and organization"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_script_shebang_and_error_handling(self):
        """Test script has proper shebang and error handling"""
        # Should start with bash shebang
        assert self.script_content.startswith('#!/bin/bash'), "Should have bash shebang"
        
        # Should have error handling
        assert 'set -e' in self.script_content, "Should exit on error"
    
    def test_script_has_color_output(self):
        """Test script has color-coded output for better UX"""
        color_codes = ['RED=', 'GREEN=', 'YELLOW=', 'BLUE=', 'NC=']
        
        for color in color_codes:
            assert color in self.script_content, f"Should define color code: {color}"
    
    def test_script_has_helper_functions(self):
        """Test script has helper functions for status reporting"""
        helper_functions = [
            'print_status()',
            'print_warning()',
            'print_error()'
        ]
        
        for func in helper_functions:
            assert func in self.script_content, f"Should define helper function: {func}"
    
    def test_script_has_configuration_section(self):
        """Test script has configuration section"""
        config_vars = [
            'AWS_REGION=',
            'ENVIRONMENT=',
            'SOURCE_BUCKET='
        ]
        
        for var in config_vars:
            assert var in self.script_content, f"Should define config variable: {var}"
    
    def test_script_has_deployment_steps(self):
        """Test script has all major deployment steps"""
        deployment_steps = [
            '# Step 1: Deploy Infrastructure',
            '# Step 2: Upload Glue Scripts',
            '# Step 3: Create Lambda deployment package',
            '# Step 4: Setup Airflow',
            '# Step 5: Initialize dbt',
            '# Step 6: Create S3 Tables',
            '# Step 7: Setup CloudWatch Dashboard',
            '# Step 8: Run initial S3 Tables validation'
        ]
        
        for step in deployment_steps:
            assert step in self.script_content, f"Should include deployment step: {step}"


class TestPrerequisiteChecks:
    """Test prerequisite validation logic"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_aws_cli_check(self):
        """Test AWS CLI prerequisite check"""
        # Should check for AWS CLI
        assert 'command -v aws' in self.script_content, "Should check for AWS CLI"
        assert 'AWS CLI not found' in self.script_content, "Should provide error message for missing AWS CLI"
    
    def test_nodejs_check(self):
        """Test Node.js prerequisite check"""
        # Should check for Node.js (needed for CDK)
        assert 'command -v node' in self.script_content, "Should check for Node.js"
        assert 'Node.js not found' in self.script_content, "Should provide error message for missing Node.js"
    
    def test_python_check(self):
        """Test Python prerequisite check"""
        # Should check for Python 3
        assert 'command -v python3' in self.script_content, "Should check for Python 3"
        assert 'Python 3 not found' in self.script_content, "Should provide error message for missing Python"
    
    def test_cdk_cli_check(self):
        """Test CDK CLI check and auto-installation"""
        # Should check for CDK CLI
        assert 'command -v cdk' in self.script_content, "Should check for CDK CLI"
        
        # Should auto-install if missing
        assert 'npm install -g aws-cdk' in self.script_content, "Should auto-install CDK CLI"
    
    def test_prerequisite_check_completion(self):
        """Test prerequisite check completion message"""
        assert 'Prerequisites check completed' in self.script_content, "Should confirm prerequisite checks"


class TestInfrastructureDeployment:
    """Test infrastructure deployment steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_cdk_bootstrap(self):
        """Test CDK bootstrap step"""
        # Should bootstrap CDK
        assert 'cdk bootstrap' in self.script_content, "Should bootstrap CDK"
        
        # Should handle case where bootstrap already exists
        assert 'already bootstrapped' in self.script_content, "Should handle existing bootstrap"
    
    def test_cdk_deploy_command(self):
        """Test CDK deployment command"""
        # Should deploy the correct stack
        assert 'cdk deploy WebEventsDataLakeStack' in self.script_content, "Should deploy correct stack"
        
        # Should pass source bucket parameter
        assert '--parameters sourceS3Bucket=' in self.script_content, "Should pass source bucket parameter"
        
        # Should not require approval
        assert '--require-approval never' in self.script_content, "Should skip approval prompt"
    
    def test_infrastructure_deployment_validation(self):
        """Test infrastructure deployment success validation"""  
        # Should check deployment success
        assert 'if [ $? -eq 0 ]' in self.script_content, "Should check deployment exit code"
        
        # Should provide success/failure messages
        assert 'Infrastructure deployment completed successfully' in self.script_content
        assert 'Infrastructure deployment failed' in self.script_content
    
    def test_aws_account_id_retrieval(self):
        """Test AWS account ID retrieval"""
        # Should get AWS account ID
        assert 'aws sts get-caller-identity' in self.script_content, "Should get AWS account ID"
        assert 'AWS_ACCOUNT_ID=' in self.script_content, "Should store account ID"


class TestGlueScriptsDeployment:
    """Test Glue scripts deployment steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_glue_scripts_bucket_reference(self):
        """Test Glue scripts bucket reference"""
        # Should reference correct bucket
        assert 'peek-glue-scripts-${AWS_ACCOUNT_ID}' in self.script_content, "Should reference Glue scripts bucket"
    
    def test_iceberg_script_upload(self):
        """Test Iceberg-specific script upload"""
        # Should upload Iceberg versions of scripts
        iceberg_scripts = [
            'kinesis_processor_iceberg.py',
            's3_processor_iceberg.py'
        ]
        
        for script in iceberg_scripts:
            assert script in self.script_content, f"Should upload Iceberg script: {script}"
    
    def test_script_upload_location(self):
        """Test scripts are uploaded to correct location"""
        # Should upload to scripts/ prefix
        assert 's3://${GLUE_SCRIPTS_BUCKET}/scripts/' in self.script_content, "Should upload to scripts/ prefix"


class TestLambdaDeployment:
    """Test Lambda function deployment steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_lambda_package_creation(self):
        """Test Lambda deployment package creation"""
        # Should change to Lambda directory
        assert 'cd lambda/kinesis-processor' in self.script_content, "Should navigate to Lambda directory"
        
        # Should copy Iceberg version
        assert 'cp index_iceberg.py index.py' in self.script_content, "Should use Iceberg version"
        
        # Should create ZIP package
        assert 'zip -r lambda-deployment.zip' in self.script_content, "Should create deployment package"
    
    def test_lambda_function_update(self):
        """Test Lambda function code update"""
        # Should update Lambda function
        assert 'aws lambda update-function-code' in self.script_content, "Should update Lambda function"
        
        # Should reference correct function name
        assert '--function-name KinesisProcessor' in self.script_content, "Should update correct function"
        
        # Should use deployment package
        assert '--zip-file fileb://lambda-deployment.zip' in self.script_content, "Should use deployment package"


class TestAirflowSetup:
    """Test Airflow setup steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_airflow_bucket_creation(self):
        """Test Airflow S3 bucket creation"""
        # Should define Airflow bucket
        assert 'AIRFLOW_BUCKET="peek-airflow-${AWS_ACCOUNT_ID}"' in self.script_content
        
        # Should create bucket if it doesn't exist
        assert 'aws s3 mb s3://${AIRFLOW_BUCKET}' in self.script_content, "Should create Airflow bucket"
        
        # Should check if bucket exists first
        assert 'aws s3 ls' in self.script_content, "Should check bucket existence"
    
    def test_dag_upload(self):
        """Test DAG upload to S3"""
        # Should sync DAGs to S3
        assert 'aws s3 sync airflow/dags/' in self.script_content, "Should sync DAGs"
        assert 's3://${AIRFLOW_BUCKET}/dags/' in self.script_content, "Should upload to dags/ prefix"


class TestDBTSetup:
    """Test dbt setup and initialization"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_dbt_dependencies_installation(self):
        """Test dbt dependencies installation"""
        # Should install dbt packages if packages.yml exists
        assert 'if [ -f "packages.yml" ]' in self.script_content, "Should check for packages.yml"
        assert 'dbt deps' in self.script_content, "Should install dbt dependencies"
    
    def test_dbt_connection_test(self):
        """Test dbt connection testing"""
        # Should test dbt connection
        assert 'dbt debug' in self.script_content, "Should test dbt connection"
    
    def test_dbt_model_execution(self):
        """Test dbt model execution"""
        # Should run dbt models
        assert 'dbt run --target ${ENVIRONMENT}' in self.script_content, "Should run dbt models"
        
        # Should run dbt tests
        assert 'dbt test --target ${ENVIRONMENT}' in self.script_content, "Should run dbt tests"


class TestS3TablesSchemaCreation:
    """Test S3 Tables schema creation steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_athena_schema_execution(self):
        """Test Athena schema execution"""
        # Should execute schema SQL using Athena
        assert 'aws athena start-query-execution' in self.script_content, "Should execute schema via Athena"
        
        # Should read schema from file
        assert 'sql/iceberg_schemas.sql' in self.script_content, "Should read schema file"
        
        # Should specify output location
        assert 'OutputLocation=s3://peek-web-events-datalake-${AWS_ACCOUNT_ID}/athena-results/' in self.script_content
        
        # Should use correct workgroup
        assert 'peek-web-events-workgroup' in self.script_content, "Should use correct workgroup"
    
    def test_schema_creation_wait(self):
        """Test waiting for schema creation to complete"""
        # Should wait for schema setup
        assert 'sleep 30' in self.script_content, "Should wait for schema creation"


class TestMonitoringSetup:
    """Test monitoring and dashboard setup"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_cloudwatch_dashboard_creation(self):
        """Test CloudWatch dashboard creation"""
        # Should create CloudWatch dashboard
        assert 'aws cloudwatch put-dashboard' in self.script_content, "Should create dashboard"
        
        # Should specify dashboard name
        assert '--dashboard-name "WebEventsDataLakeDashboard"' in self.script_content
        
        # Should read dashboard configuration
        assert 'monitoring/cloudwatch_dashboard.json' in self.script_content


class TestValidationSteps:
    """Test validation and health check steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_s3_bucket_validation(self):
        """Test S3 bucket accessibility validation"""
        # Should test data lake bucket access
        assert 'aws s3 ls s3://peek-web-events-datalake-${AWS_ACCOUNT_ID}/' in self.script_content
        
        # Should provide success message
        assert 'S3 data lake bucket accessible' in self.script_content
    
    def test_s3_tables_validation(self):
        """Test S3 Tables validation"""  
        # Should test S3 Tables bucket
        assert 'TABLE_BUCKET_NAME="peek-web-events-tables-${AWS_ACCOUNT_ID}"' in self.script_content
        
        # Should list tables
        assert 'aws s3tables list-tables' in self.script_content, "Should validate S3 Tables"
        
        # Should handle initialization delay
        assert 'S3 Tables may still be initializing' in self.script_content
    
    def test_kinesis_stream_validation(self):
        """Test Kinesis stream validation"""
        # Should check Kinesis stream
        assert 'aws kinesis describe-stream --stream-name peek-web-events-stream' in self.script_content
        
        # Should provide success message
        assert 'Kinesis stream accessible' in self.script_content
    
    def test_glue_database_validation(self):
        """Test Glue database validation"""
        # Should check Glue database
        assert 'aws glue get-database --name peek_web_events' in self.script_content
        
        # Should provide success message
        assert 'Glue database accessible' in self.script_content


class TestUserGuidance:
    """Test user guidance and next steps"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_completion_message(self):
        """Test deployment completion message"""
        # Should provide success message
        assert 'Deployment completed successfully!' in self.script_content
        
        # Should mention S3 Tables/Iceberg
        assert 'S3 Tables/Iceberg' in self.script_content
    
    def test_next_steps_guidance(self):
        """Test next steps guidance"""
        # Should provide next steps
        assert 'Next Steps (S3 Tables/Iceberg):' in self.script_content
        
        # Should mention key actions
        next_steps = [
            'Configure your web application',
            'Trigger S3 Tables historical migration',
            'Start real-time processing',
            'Monitor the pipeline',
            'Query S3 Tables using Athena'
        ]
        
        for step in next_steps:
            assert any(keyword in self.script_content for keyword in step.split()[:2])
    
    def test_resource_information(self):
        """Test resource information section"""
        # Should provide resource information
        assert 'Resources (S3 Tables/Iceberg):' in self.script_content
        
        # Should list key resources
        resources = [
            'Data Lake Bucket:',
            'S3 Tables Bucket:',
            'Kinesis Stream:',
            'Athena Database:',
            'S3 Tables Namespace:',
            'Iceberg Time Travel:'
        ]
        
        for resource in resources:
            assert resource in self.script_content, f"Should mention resource: {resource}"


class TestTestEventFunctionality:
    """Test optional test event functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_test_event_prompt(self):
        """Test test event user prompt"""
        # Should ask user if they want to send test event
        assert 'Would you like to send a test event' in self.script_content
        
        # Should read user response
        assert 'read -r response' in self.script_content
    
    def test_test_event_creation(self):
        """Test test event creation and sending"""
        # Should create test event from sample
        assert 'TEST_EVENT=$(cat web-event-sample.json | base64)' in self.script_content
        
        # Should send to Kinesis
        assert 'aws kinesis put-record' in self.script_content
        assert '--stream-name peek-web-events-stream' in self.script_content
        assert '--data "${TEST_EVENT}"' in self.script_content
    
    def test_test_event_confirmation(self):
        """Test test event confirmation message"""
        # Should confirm test event sent
        assert 'Test event sent!' in self.script_content
        assert 'Check CloudWatch logs' in self.script_content


class TestErrorHandlingLogic:
    """Test error handling throughout the script"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_exit_on_error_configuration(self):
        """Test exit on error configuration"""
        # Should exit on any error
        assert 'set -e' in self.script_content, "Should exit on error"
    
    def test_prerequisite_error_handling(self):
        """Test prerequisite check error handling"""
        # Should exit with error code for missing prerequisites
        assert 'exit 1' in self.script_content, "Should exit with error code"
        
        # Should provide specific error messages
        error_messages = [
            'AWS CLI not found',
            'Node.js not found',
            'Python 3 not found'
        ]
        
        for message in error_messages:
            assert message in self.script_content, f"Should provide error message: {message}"
    
    def test_deployment_failure_handling(self):
        """Test deployment failure handling"""
        # Should check CDK deployment success
        assert 'if [ $? -eq 0 ]' in self.script_content, "Should check deployment exit code"
        
        # Should provide failure message and exit
        assert 'Infrastructure deployment failed' in self.script_content
    
    def test_validation_error_handling(self):
        """Test validation step error handling"""
        # Should handle validation failures gracefully
        # Most validation steps should continue even if some checks fail
        assert 'print_warning' in self.script_content, "Should use warning messages for non-critical failures"


class TestScriptParameterization:
    """Test script parameterization and configuration"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_content, 'r') as f:
            self.script_content = f.read()
    
    def test_environment_variable_defaults(self):
        """Test environment variable defaults"""
        # Should have default values
        defaults = [
            'AWS_REGION=${AWS_REGION:-us-east-1}',
            'ENVIRONMENT=${ENVIRONMENT:-dev}',
            'SOURCE_BUCKET=${SOURCE_BUCKET:-your-source-s3-bucket}'
        ]
        
        for default in defaults:
            assert default in self.script_content, f"Should have default: {default}"
    
    def test_configuration_display(self):
        """Test configuration display to user"""
        # Should display configuration to user
        config_displays = [
            'Environment: ${ENVIRONMENT}',
            'AWS Region: ${AWS_REGION}',
            'Source Bucket: ${SOURCE_BUCKET}'
        ]
        
        for display in config_displays:
            assert display in self.script_content, f"Should display config: {display}"
    
    def test_dynamic_resource_naming(self):
        """Test dynamic resource naming using variables"""
        # Should use dynamic naming
        dynamic_names = [
            '${AWS_ACCOUNT_ID}',
            '${ENVIRONMENT}',
            '${AWS_REGION}'
        ]
        
        for name in dynamic_names:
            assert name in self.script_content, f"Should use dynamic naming: {name}"


class TestScriptMaintainability:
    """Test script maintainability and best practices"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_script_organization(self):
        """Test script is well-organized with clear sections"""
        # Should have clear section headers
        sections = [
            '# Configuration',
            '# Function to print status',
            '# Check prerequisites',
            '# Step 1:',
            '# Step 2:',
            '# Get AWS Account ID'
        ]
        
        section_count = 0
        for section in sections:
            if section in self.script_content:
                section_count += 1
        
        assert section_count >= 4, "Should have clear section organization"
    
    def test_script_comments(self):
        """Test script has adequate comments"""
        # Count comment lines
        lines = self.script_content.split('\n')
        comment_lines = [line for line in lines if line.strip().startswith('#')]
        total_lines = len([line for line in lines if line.strip()])
        
        # Should have reasonable comment ratio
        comment_ratio = len(comment_lines) / total_lines if total_lines > 0 else 0
        assert comment_ratio >= 0.15, "Should have adequate comments (15% minimum)"
    
    def test_consistent_messaging(self):
        """Test consistent use of messaging functions"""
        # Should use print_status, print_warning, print_error consistently
        # Rather than raw echo statements
        print_functions = ['print_status', 'print_warning', 'print_error']
        raw_echo_count = self.script_content.count('echo -e "${')
        
        # Most output should use the helper functions
        for func in print_functions:
            assert func in self.script_content, f"Should define messaging function: {func}"


class TestS3TablesSpecificFeatures:
    """Test S3 Tables specific deployment features"""
    
    def setup_method(self):
        """Setup test environment"""
        self.script_path = Path("/Users/james/workspace-peek/load-webevents/deploy.sh")
        with open(self.script_path, 'r') as f:
            self.script_content = f.read()
    
    def test_s3_tables_naming_convention(self):
        """Test S3 Tables naming follows convention"""
        # Should use consistent naming
        assert 'peek-web-events-tables-${AWS_ACCOUNT_ID}' in self.script_content
        
        # Should reference S3 Tables namespace
        assert 's3_tables.analytics' in self.script_content
    
    def test_iceberg_specific_references(self):
        """Test Iceberg-specific references throughout script"""
        # Should mention Iceberg in multiple places
        iceberg_references = self.script_content.count('Iceberg')
        assert iceberg_references >= 3, "Should reference Iceberg multiple times"
        
        # Should mention S3 Tables
        s3_tables_references = self.script_content.count('S3 Tables')
        assert s3_tables_references >= 3, "Should reference S3 Tables multiple times"
    
    def test_time_travel_feature_mention(self):
        """Test time travel feature is mentioned"""
        # Should mention Iceberg time travel capability
        assert 'time travel' in self.script_content.lower() or 'FOR TIMESTAMP AS OF' in self.script_content


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=15"])