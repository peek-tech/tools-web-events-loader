"""
Unit tests for AWS CDK Infrastructure Stack (S3 Tables/Iceberg)
Tests the WebEventsDataLakeStack for proper resource creation, configurations, and security.
"""

import pytest
import json
from unittest.mock import Mock, patch
import aws_cdk as cdk
from aws_cdk import assertions
from cdk.lib.web_events_data_lake_stack import WebEventsDataLakeStack


class TestWebEventsDataLakeStack:
    """Comprehensive tests for the S3 Tables/Iceberg infrastructure stack"""
    
    def setup_method(self):
        """Setup test environment for each test"""
        self.app = cdk.App()
        self.account_id = "123456789012"
        self.region = "us-east-1"
        self.env = cdk.Environment(account=self.account_id, region=self.region)
        
        # Create stack with test parameters
        self.stack = WebEventsDataLakeStack(
            self.app, 
            "TestWebEventsStack",
            env=self.env,
            sourceS3Bucket="test-source-bucket"
        )
        
        self.template = assertions.Template.from_stack(self.stack)
    
    def test_s3_table_bucket_creation(self):
        """Test S3 Table Bucket is created with correct configuration"""
        # Verify S3 Table Bucket exists
        self.template.has_resource_properties("AWS::S3Tables::TableBucket", {
            "TableBucketName": f"peek-web-events-tables-{self.account_id}"
        })
        
        # Check that only one table bucket is created
        table_buckets = self.template.find_resources("AWS::S3Tables::TableBucket")
        assert len(table_buckets) == 1, "Should create exactly one S3 Table Bucket"
    
    def test_s3_tables_namespace_creation(self):
        """Test S3 Tables namespace is created correctly"""
        # Verify namespace creation
        self.template.has_resource_properties("AWS::S3Tables::Namespace", {
            "Namespace": "analytics"
        })
        
        # Ensure namespace references the correct table bucket
        namespaces = self.template.find_resources("AWS::S3Tables::Namespace")
        assert len(namespaces) == 1, "Should create exactly one namespace"
    
    def test_iceberg_tables_creation(self):
        """Test that Iceberg tables are created with proper configuration"""
        # Test web events table
        self.template.has_resource_properties("AWS::S3Tables::Table", {
            "Namespace": "analytics",
            "Name": "web_events",
            "Format": "ICEBERG"
        })
        
        # Test session metrics table
        self.template.has_resource_properties("AWS::S3Tables::Table", {
            "Namespace": "analytics", 
            "Name": "session_metrics",
            "Format": "ICEBERG"
        })
        
        # Verify exactly 2 tables are created
        tables = self.template.find_resources("AWS::S3Tables::Table")
        assert len(tables) == 2, "Should create exactly 2 S3 Tables"
    
    def test_kinesis_stream_configuration(self):
        """Test Kinesis stream for real-time events is properly configured"""
        self.template.has_resource_properties("AWS::Kinesis::Stream", {
            "Name": "peek-web-events-stream",
            "ShardCount": 5,
            "RetentionPeriod": 168,  # 7 days in hours
            "StreamEncryption": {
                "EncryptionType": "KMS"
            }
        })
        
        # Verify only one Kinesis stream
        streams = self.template.find_resources("AWS::Kinesis::Stream")
        assert len(streams) == 1, "Should create exactly one Kinesis stream"
    
    def test_glue_database_creation(self):
        """Test Glue database is created with Iceberg support"""
        self.template.has_resource_properties("AWS::Glue::Database", {
            "DatabaseInput": {
                "Name": "peek_web_events",
                "Description": "Database for web events clickstream data (Iceberg format)",
                "Parameters": {
                    "classification": "iceberg",
                    "table_format": "ICEBERG"
                }
            }
        })
    
    def test_glue_jobs_configuration(self):
        """Test Glue jobs are configured for Iceberg processing"""
        # Test Kinesis processor job
        self.template.has_resource_properties("AWS::Glue::Job", {
            "Name": "peek-web-events-kinesis-processor",
            "Command": {
                "Name": "gluestreaming",
                "PythonVersion": "3"
            },
            "GlueVersion": "4.0",
            "MaxRetries": 0,
            "Timeout": 2880  # 48 hours
        })
        
        # Test S3 processor job
        self.template.has_resource_properties("AWS::Glue::Job", {
            "Name": "peek-web-events-s3-processor", 
            "Command": {
                "Name": "glueetl",
                "PythonVersion": "3"
            },
            "GlueVersion": "4.0",
            "MaxRetries": 1,
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10
        })
        
        # Verify exactly 2 Glue jobs
        jobs = self.template.find_resources("AWS::Glue::Job")
        assert len(jobs) == 2, "Should create exactly 2 Glue jobs"
    
    def test_lambda_function_configuration(self):
        """Test Lambda function for Kinesis processing"""
        self.template.has_resource_properties("AWS::Lambda::Function", {
            "Runtime": "python3.9",
            "Handler": "index.lambda_handler",
            "Timeout": 300,  # 5 minutes
            "MemorySize": 512,
            "ReservedConcurrentExecutions": 100
        })
        
        # Test environment variables contain S3 Tables references
        lambda_functions = self.template.find_resources("AWS::Lambda::Function")
        assert len(lambda_functions) == 1, "Should create exactly one Lambda function"
        
        # Check environment variables are set
        lambda_props = list(lambda_functions.values())[0]['Properties']
        env_vars = lambda_props['Environment']['Variables']
        
        assert 'TABLE_BUCKET_ARN' in env_vars, "Should have TABLE_BUCKET_ARN environment variable"
        assert 'WEB_EVENTS_TABLE' in env_vars, "Should have WEB_EVENTS_TABLE environment variable"
        assert 'SESSION_METRICS_TABLE' in env_vars, "Should have SESSION_METRICS_TABLE environment variable"
    
    def test_iam_permissions_glue_role(self):
        """Test IAM permissions for Glue jobs include S3 Tables access"""
        # Find the Glue role
        roles = self.template.find_resources("AWS::IAM::Role")
        glue_roles = {k: v for k, v in roles.items() 
                     if 'GlueJobRole' in k or 
                     v['Properties'].get('AssumedBy', {}).get('Service') == 'glue.amazonaws.com'}
        
        assert len(glue_roles) >= 1, "Should have at least one Glue IAM role"
        
        # Check for managed policy attachment
        self.template.has_resource_properties("AWS::IAM::Role", {
            "AssumedBy": {"Service": "glue.amazonaws.com"},
            "ManagedPolicyArns": [
                {"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, 
                               ":iam::aws:policy/service-role/AWSGlueServiceRole"]]}
            ]
        })
    
    def test_s3_buckets_encryption(self):
        """Test that S3 buckets are properly encrypted"""
        # Test data lake bucket encryption
        self.template.has_resource_properties("AWS::S3::Bucket", {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [{
                    "ServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "aws:kms"
                    }
                }]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True
            }
        })
    
    def test_vpc_configuration(self):
        """Test VPC configuration for Redshift and secure networking"""
        # Test VPC creation
        self.template.has_resource_properties("AWS::EC2::VPC", {
            "CidrBlock": "10.0.0.0/16",
            "EnableDnsHostnames": True,
            "EnableDnsSupport": True
        })
        
        # Test subnet configuration
        subnets = self.template.find_resources("AWS::EC2::Subnet")
        
        # Should have both public and private subnets across 2 AZs (4 total)
        assert len(subnets) == 4, f"Expected 4 subnets, found {len(subnets)}"
    
    def test_redshift_cluster_configuration(self):
        """Test Redshift cluster for structured analytics"""
        self.template.has_resource_properties("AWS::Redshift::Cluster", {
            "DBName": "webevents",
            "NodeType": "dc2.large",
            "ClusterType": "multi-node",
            "NumberOfNodes": 2,
            "Encrypted": True
        })
        
        # Test subnet group
        self.template.has_resource("AWS::Redshift::ClusterSubnetGroup")
    
    def test_athena_workgroup_configuration(self):
        """Test Athena workgroup for S3 Tables querying"""
        self.template.has_resource_properties("AWS::Athena::WorkGroup", {
            "Name": "peek-web-events-workgroup",
            "Description": "Workgroup for web events analytics",
            "State": "ENABLED",
            "WorkGroupConfiguration": {
                "ResultConfiguration": {
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_KMS"
                    }
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetrics": True,
                "BytesScannedCutoffPerQuery": 10000000000,  # 10GB
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 3"
                }
            }
        })
    
    def test_cloudwatch_dashboard_creation(self):
        """Test CloudWatch dashboard for monitoring"""
        dashboards = self.template.find_resources("AWS::CloudWatch::Dashboard")
        assert len(dashboards) == 1, "Should create exactly one CloudWatch dashboard"
        
        dashboard_props = list(dashboards.values())[0]['Properties']
        assert dashboard_props['DashboardName'] == "WebEventsDataLake"
    
    def test_stack_outputs(self):
        """Test that essential stack outputs are defined"""
        outputs = self.template.find_outputs("*")
        
        expected_outputs = [
            "DataLakeBucketName",
            "GlueDatabaseName", 
            "KinesisJobName",
            "S3JobName",
            "AthenaWorkgroupName"
        ]
        
        output_keys = list(outputs.keys())
        for expected_output in expected_outputs:
            assert any(expected_output in key for key in output_keys), \
                f"Missing expected output: {expected_output}"
    
    def test_s3_lifecycle_policies(self):
        """Test S3 lifecycle policies for cost optimization"""
        # Find data lake bucket with lifecycle rules
        buckets = self.template.find_resources("AWS::S3::Bucket")
        
        data_lake_buckets = {k: v for k, v in buckets.items() 
                           if 'DataLake' in k or 'peek-web-events-datalake' in str(v)}
        
        assert len(data_lake_buckets) >= 1, "Should have data lake bucket"
        
        # Check for lifecycle configuration
        bucket_props = list(data_lake_buckets.values())[0]['Properties']
        if 'LifecycleConfiguration' in bucket_props:
            lifecycle_rules = bucket_props['LifecycleConfiguration']['Rules']
            assert len(lifecycle_rules) >= 1, "Should have lifecycle rules"
    
    def test_resource_naming_consistency(self):
        """Test that resource names follow consistent naming patterns"""
        # Get all resources
        all_resources = self.template.find_resources("*")  
        
        # Check for consistent naming patterns
        resource_names = []
        for resource_type, resources in all_resources.items():
            for logical_id, resource in resources.items():
                if 'Properties' in resource:
                    props = resource['Properties']
                    
                    # Extract name properties based on resource type
                    name_prop = None
                    if resource_type == "AWS::S3::Bucket" and 'BucketName' in props:
                        name_prop = props['BucketName']
                    elif resource_type == "AWS::Kinesis::Stream" and 'Name' in props:
                        name_prop = props['Name']
                    elif resource_type == "AWS::Glue::Job" and 'Name' in props:
                        name_prop = props['Name']
                    
                    if name_prop:
                        resource_names.append(name_prop)
        
        # Check that all names contain expected prefixes
        for name in resource_names:
            if isinstance(name, str):
                assert any(prefix in name for prefix in ['peek', 'web-events']), \
                    f"Resource name '{name}' doesn't follow naming convention"
    
    def test_security_configurations(self):
        """Test security configurations across all resources"""
        # Test S3 bucket public access is blocked
        buckets = self.template.find_resources("AWS::S3::Bucket")
        for bucket_id, bucket in buckets.items():
            props = bucket['Properties']
            if 'PublicAccessBlockConfiguration' in props:
                pab = props['PublicAccessBlockConfiguration']
                assert pab.get('BlockPublicAcls') == True
                assert pab.get('BlockPublicPolicy') == True
                assert pab.get('IgnorePublicAcls') == True
                assert pab.get('RestrictPublicBuckets') == True
        
        # Test Kinesis stream encryption
        streams = self.template.find_resources("AWS::Kinesis::Stream")
        for stream_id, stream in streams.items():
            props = stream['Properties']
            assert 'StreamEncryption' in props
            assert props['StreamEncryption']['EncryptionType'] == 'KMS'
        
        # Test Redshift encryption
        clusters = self.template.find_resources("AWS::Redshift::Cluster")
        for cluster_id, cluster in clusters.items():
            props = cluster['Properties']
            assert props.get('Encrypted') == True


class TestS3TablesSpecificFeatures:
    """Test S3 Tables specific configurations and integrations"""
    
    def setup_method(self):
        """Setup for S3 Tables specific tests"""
        self.app = cdk.App()
        self.account_id = "123456789012"
        self.region = "us-east-1"
        self.env = cdk.Environment(account=self.account_id, region=self.region)
        
        self.stack = WebEventsDataLakeStack(
            self.app,
            "TestS3TablesStack", 
            env=self.env,
            sourceS3Bucket="test-source-bucket"
        )
        
        self.template = assertions.Template.from_stack(self.stack)
    
    def test_s3_tables_integration_environment_variables(self):
        """Test Lambda environment variables contain correct S3 Tables ARNs"""
        lambda_functions = self.template.find_resources("AWS::Lambda::Function")
        lambda_props = list(lambda_functions.values())[0]['Properties']
        env_vars = lambda_props['Environment']['Variables']
        
        # Test table ARN format
        table_bucket_arn = env_vars['TABLE_BUCKET_ARN']
        web_events_table = env_vars['WEB_EVENTS_TABLE']
        session_metrics_table = env_vars['SESSION_METRICS_TABLE']
        
        # Validate ARN structure (using Fn::GetAtt)
        assert 'Fn::GetAtt' in str(table_bucket_arn) or 'Ref' in str(table_bucket_arn)
        assert 'namespaces/analytics/tables/web_events' in str(web_events_table)
        assert 'namespaces/analytics/tables/session_metrics' in str(session_metrics_table)
    
    def test_glue_job_s3_tables_arguments(self):
        """Test Glue jobs have correct S3 Tables parameters"""
        glue_jobs = self.template.find_resources("AWS::Glue::Job")
        
        for job_id, job in glue_jobs.items():
            default_args = job['Properties']['DefaultArguments']
            
            # Check for S3 Tables specific arguments
            assert '--TABLE_BUCKET_ARN' in str(default_args) or \
                   'TABLE_BUCKET_ARN' in str(default_args)
            assert '--WEB_EVENTS_TABLE' in str(default_args) or \
                   'WEB_EVENTS_TABLE' in str(default_args)
            assert '--SESSION_METRICS_TABLE' in str(default_args) or \
                   'SESSION_METRICS_TABLE' in str(default_args)
    
    def test_iceberg_format_specification(self):
        """Test that all tables are explicitly set to ICEBERG format"""
        tables = self.template.find_resources("AWS::S3Tables::Table")
        
        for table_id, table in tables.items():
            props = table['Properties']
            assert props['Format'] == 'ICEBERG', \
                f"Table {table_id} should use ICEBERG format"
    
    def test_namespace_table_relationships(self):
        """Test proper relationships between namespace and tables"""
        namespace = self.template.find_resources("AWS::S3Tables::Namespace")
        tables = self.template.find_resources("AWS::S3Tables::Table")
        
        assert len(namespace) == 1, "Should have exactly one namespace"
        assert len(tables) == 2, "Should have exactly two tables"
        
        # All tables should reference the same namespace
        namespace_name = list(namespace.values())[0]['Properties']['Namespace']
        
        for table_id, table in tables.items():
            table_namespace = table['Properties']['Namespace']
            assert table_namespace == namespace_name, \
                f"Table {table_id} should use namespace {namespace_name}"


class TestStackParameterValidation:
    """Test stack behavior with different parameter configurations"""
    
    def test_with_custom_source_bucket(self):
        """Test stack creation with custom source bucket parameter"""
        app = cdk.App()
        custom_bucket = "my-custom-source-bucket"
        
        stack = WebEventsDataLakeStack(
            app,
            "TestCustomBucketStack",
            sourceS3Bucket=custom_bucket
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Check that the custom source bucket is used in Glue job arguments
        glue_jobs = template.find_resources("AWS::Glue::Job")
        s3_processor_jobs = {k: v for k, v in glue_jobs.items() 
                           if 's3-processor' in v['Properties']['Name']}
        
        assert len(s3_processor_jobs) == 1, "Should have S3 processor job"
        
        job_props = list(s3_processor_jobs.values())[0]['Properties']
        default_args = job_props['DefaultArguments']
        
        # The source bucket should be in the arguments
        source_bucket_arg = default_args.get('--SOURCE_BUCKET')
        assert source_bucket_arg == custom_bucket, \
            f"Expected source bucket {custom_bucket}, got {source_bucket_arg}"
    
    def test_without_source_bucket_parameter(self):
        """Test stack creation without source bucket parameter (uses default)"""
        app = cdk.App()
        
        stack = WebEventsDataLakeStack(app, "TestDefaultBucketStack")
        template = assertions.Template.from_stack(stack)
        
        # Should still create successfully and use default
        glue_jobs = template.find_resources("AWS::Glue::Job")
        assert len(glue_jobs) == 2, "Should create both Glue jobs with default parameters"


class TestErrorHandlingAndEdgeCases:
    """Test error conditions and edge cases in infrastructure"""
    
    def test_resource_dependencies(self):
        """Test that resources have proper dependencies"""
        app = cdk.App()
        stack = WebEventsDataLakeStack(app, "TestDependenciesStack")
        template = assertions.Template.from_stack(stack)
        
        # Tables should depend on namespace
        tables = template.find_resources("AWS::S3Tables::Table")
        namespace = template.find_resources("AWS::S3Tables::Namespace")
        
        assert len(tables) == 2, "Should have tables"
        assert len(namespace) == 1, "Should have namespace"
        
        # In CDK, dependencies are handled automatically through references
        # We can verify by checking that table properties reference namespace
        for table_id, table in tables.items():
            assert table['Properties']['Namespace'] == 'analytics'
    
    def test_resource_limits_and_quotas(self):
        """Test that resource configurations stay within AWS limits"""
        app = cdk.App()
        stack = WebEventsDataLakeStack(app, "TestLimitsStack")
        template = assertions.Template.from_stack(stack)
        
        # Test Kinesis shard count is within limits
        streams = template.find_resources("AWS::Kinesis::Stream")
        for stream_id, stream in streams.items():
            shard_count = stream['Properties']['ShardCount']
            assert 1 <= shard_count <= 1000, f"Shard count {shard_count} outside valid range"
        
        # Test Lambda memory is within limits
        functions = template.find_resources("AWS::Lambda::Function")
        for func_id, func in functions.items():
            memory = func['Properties']['MemorySize']
            assert 128 <= memory <= 10240, f"Lambda memory {memory} outside valid range"
        
        # Test Redshift node count is reasonable
        clusters = template.find_resources("AWS::Redshift::Cluster")
        for cluster_id, cluster in clusters.items():
            node_count = cluster['Properties']['NumberOfNodes']
            assert 1 <= node_count <= 128, f"Redshift nodes {node_count} outside reasonable range"


# Edge case and error condition tests
class TestInfrastructureEdgeCases:
    """Test edge cases and potential failure scenarios"""
    
    def test_stack_creation_with_minimal_permissions(self):
        """Test stack validation without assuming all permissions are available"""
        app = cdk.App()
        stack = WebEventsDataLakeStack(app, "TestMinimalPermsStack")
        
        # This tests template synthesis, not actual deployment
        template = assertions.Template.from_stack(stack)
        
        # Should successfully synthesize even with minimal test environment
        assert template is not None
        
        # Basic resource count validation
        resources = template.find_resources("*")
        assert len(resources) > 0, "Should create some resources"
    
    def test_stack_with_long_account_id(self):
        """Test stack with edge case account ID"""
        app = cdk.App()
        # Test with maximum length account ID
        long_account_id = "999999999999"
        env = cdk.Environment(account=long_account_id, region="us-east-1")
        
        stack = WebEventsDataLakeStack(
            app, 
            "TestLongAccountStack",
            env=env
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Should handle long account IDs correctly in resource names
        buckets = template.find_resources("AWS::S3Tables::TableBucket")
        bucket_props = list(buckets.values())[0]['Properties']
        bucket_name = bucket_props['TableBucketName']
        
        assert long_account_id in bucket_name, "Account ID should be in bucket name"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])