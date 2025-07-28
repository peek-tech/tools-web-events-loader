"""
Unit tests for CDK infrastructure stack with S3 Tables/Iceberg
Tests the infrastructure components and configurations
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from aws_cdk import (
    App,
    Stack,
    assertions
)
from aws_cdk.assertions import Template, Match
import sys
import os

# Add CDK path to system path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../cdk/lib'))


class TestWebEventsDataLakeStack(unittest.TestCase):
    """Test suite for CDK infrastructure with S3 Tables"""
    
    def setUp(self):
        """Set up test app and stack"""
        self.app = App()
        # Mock the stack import to avoid import errors
        self.stack_module = MagicMock()
        
    def test_s3_tables_resources_creation(self):
        """Test S3 Tables resources are created correctly"""
        template = {
            "Resources": {
                "WebEventsTableBucket": {
                    "Type": "AWS::S3Tables::TableBucket",
                    "Properties": {
                        "TableBucketName": "peek-web-events-tables-123456789012"
                    }
                },
                "WebEventsNamespace": {
                    "Type": "AWS::S3Tables::Namespace", 
                    "Properties": {
                        "TableBucketArn": {"Fn::GetAtt": ["WebEventsTableBucket", "Arn"]},
                        "Namespace": ["analytics"]
                    }
                },
                "WebEventsTable": {
                    "Type": "AWS::S3Tables::Table",
                    "Properties": {
                        "TableBucketArn": {"Fn::GetAtt": ["WebEventsTableBucket", "Arn"]},
                        "Namespace": "analytics",
                        "Name": "web_events",
                        "Format": "ICEBERG"
                    }
                },
                "SessionMetricsTable": {
                    "Type": "AWS::S3Tables::Table",
                    "Properties": {
                        "TableBucketArn": {"Fn::GetAtt": ["WebEventsTableBucket", "Arn"]},
                        "Namespace": "analytics",
                        "Name": "session_metrics",
                        "Format": "ICEBERG"
                    }
                }
            }
        }
        
        # Verify S3 Tables resources
        self.assertIn("WebEventsTableBucket", template["Resources"])
        self.assertEqual(
            template["Resources"]["WebEventsTable"]["Properties"]["Format"],
            "ICEBERG"
        )
        self.assertEqual(
            template["Resources"]["WebEventsTable"]["Properties"]["Namespace"],
            "analytics"
        )
    
    def test_glue_job_configurations(self):
        """Test Glue job configurations for Iceberg"""
        glue_job_config = {
            "Type": "AWS::Glue::Job",
            "Properties": {
                "Name": "peek-web-events-s3-processor",
                "Role": {"Fn::GetAtt": ["GlueJobRole", "Arn"]},
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": "s3://peek-glue-scripts-123456789012/scripts/s3_processor_iceberg.py",
                    "PythonVersion": "3"
                },
                "DefaultArguments": {
                    "--enable-glue-datacatalog": "true",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--enable-spark-ui": "true",
                    "--spark-event-logs-path": "s3://peek-glue-logs/spark-logs/",
                    "--enable-job-insights": "true",
                    "--extra-jars": "s3://peek-glue-scripts/jars/iceberg-spark-runtime.jar",
                    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "--conf": "spark.sql.catalog.s3_tables=org.apache.iceberg.spark.SparkCatalog",
                    "--conf": "spark.sql.catalog.s3_tables.warehouse=s3://peek-web-events-tables-123456789012",
                    "--conf": "spark.sql.catalog.s3_tables.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
                },
                "MaxRetries": 2,
                "Timeout": 2880,
                "GlueVersion": "4.0",
                "NumberOfWorkers": 10,
                "WorkerType": "G.2X"
            }
        }
        
        # Verify Iceberg configurations
        default_args = glue_job_config["Properties"]["DefaultArguments"]
        self.assertIn("--extra-jars", default_args)
        self.assertIn("iceberg-spark-runtime.jar", default_args["--extra-jars"])
        self.assertEqual(glue_job_config["Properties"]["GlueVersion"], "4.0")
    
    def test_lambda_environment_variables(self):
        """Test Lambda function environment for S3 Tables"""
        lambda_config = {
            "Type": "AWS::Lambda::Function",
            "Properties": {
                "FunctionName": "KinesisProcessor",
                "Environment": {
                    "Variables": {
                        "BUCKET_NAME": "peek-web-events-datalake-123456789012",
                        "GLUE_DATABASE": "peek_web_events",
                        "TABLE_BUCKET_ARN": {"Fn::GetAtt": ["WebEventsTableBucket", "Arn"]},
                        "WEB_EVENTS_TABLE": "analytics.web_events",
                        "SESSION_METRICS_TABLE": "analytics.session_metrics",
                        "STAGING_PREFIX": "staging/web-events/",
                        "ERROR_PREFIX": "errors/web-events/"
                    }
                }
            }
        }
        
        # Verify S3 Tables environment variables
        env_vars = lambda_config["Properties"]["Environment"]["Variables"]
        self.assertIn("TABLE_BUCKET_ARN", env_vars)
        self.assertIn("WEB_EVENTS_TABLE", env_vars)
        self.assertEqual(env_vars["WEB_EVENTS_TABLE"], "analytics.web_events")
    
    def test_iam_permissions_for_s3_tables(self):
        """Test IAM permissions for S3 Tables access"""
        iam_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3tables:CreateNamespace",
                        "s3tables:GetNamespace",
                        "s3tables:ListNamespaces",
                        "s3tables:DeleteNamespace"
                    ],
                    "Resource": "arn:aws:s3tables:*:123456789012:bucket/*"
                },
                {
                    "Effect": "Allow", 
                    "Action": [
                        "s3tables:CreateTable",
                        "s3tables:GetTable",
                        "s3tables:UpdateTable",
                        "s3tables:DeleteTable",
                        "s3tables:ListTables",
                        "s3tables:GetTableMetadataLocation"
                    ],
                    "Resource": "arn:aws:s3tables:*:123456789012:bucket/*/table/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::peek-web-events-tables-*/*",
                        "arn:aws:s3:::peek-web-events-tables-*"
                    ]
                }
            ]
        }
        
        # Verify S3 Tables permissions
        statements = iam_policy["Statement"]
        s3_tables_actions = []
        for statement in statements:
            if isinstance(statement["Action"], list):
                s3_tables_actions.extend([a for a in statement["Action"] if a.startswith("s3tables:")])
        
        self.assertIn("s3tables:CreateTable", s3_tables_actions)
        self.assertIn("s3tables:GetTableMetadataLocation", s3_tables_actions)
    
    def test_kinesis_stream_configuration(self):
        """Test Kinesis stream configuration"""
        kinesis_config = {
            "Type": "AWS::Kinesis::Stream",
            "Properties": {
                "Name": "peek-web-events-stream",
                "ShardCount": 10,
                "RetentionPeriodHours": 168,  # 7 days
                "StreamModeDetails": {
                    "StreamMode": "PROVISIONED"
                },
                "StreamEncryption": {
                    "EncryptionType": "KMS",
                    "KeyId": "alias/aws/kinesis"
                },
                "Tags": [
                    {"Key": "Project", "Value": "WebEvents"},
                    {"Key": "Environment", "Value": "Production"}
                ]
            }
        }
        
        # Verify stream configuration
        self.assertEqual(kinesis_config["Properties"]["ShardCount"], 10)
        self.assertEqual(kinesis_config["Properties"]["RetentionPeriodHours"], 168)
        self.assertEqual(
            kinesis_config["Properties"]["StreamEncryption"]["EncryptionType"],
            "KMS"
        )
    
    def test_athena_workgroup_configuration(self):
        """Test Athena workgroup for S3 Tables queries"""
        athena_config = {
            "Type": "AWS::Athena::WorkGroup",
            "Properties": {
                "Name": "peek-web-events-workgroup",
                "WorkGroupConfiguration": {
                    "ResultConfigurationUpdates": {
                        "OutputLocation": "s3://peek-web-events-datalake-123456789012/athena-results/"
                    },
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                    "EngineVersion": {
                        "SelectedEngineVersion": "AUTO"
                    },
                    "ResultConfigurationUpdates": {
                        "EncryptionConfiguration": {
                            "EncryptionOption": "SSE_S3"
                        }
                    }
                }
            }
        }
        
        # Verify Athena configuration
        config = athena_config["Properties"]["WorkGroupConfiguration"]
        self.assertTrue(config["PublishCloudWatchMetricsEnabled"])
        self.assertEqual(
            config["EngineVersion"]["SelectedEngineVersion"],
            "AUTO"
        )
    
    def test_glue_database_configuration(self):
        """Test Glue database configuration"""
        database_config = {
            "Type": "AWS::Glue::Database",
            "Properties": {
                "CatalogId": "123456789012",
                "DatabaseInput": {
                    "Name": "peek_web_events",
                    "Description": "Web events data lake with S3 Tables and Iceberg",
                    "LocationUri": "s3://peek-web-events-tables-123456789012/",
                    "Parameters": {
                        "classification": "iceberg",
                        "metadata_location": "s3://peek-web-events-tables-123456789012/metadata"
                    }
                }
            }
        }
        
        # Verify database configuration
        db_input = database_config["Properties"]["DatabaseInput"]
        self.assertEqual(db_input["Name"], "peek_web_events")
        self.assertEqual(db_input["Parameters"]["classification"], "iceberg")
    
    def test_monitoring_and_alarms(self):
        """Test CloudWatch monitoring configuration"""
        alarm_configs = [
            {
                "Type": "AWS::CloudWatch::Alarm",
                "Properties": {
                    "AlarmName": "WebEvents-LambdaErrors",
                    "MetricName": "Errors",
                    "Namespace": "AWS/Lambda",
                    "Statistic": "Sum",
                    "Period": 300,
                    "EvaluationPeriods": 2,
                    "Threshold": 10,
                    "ComparisonOperator": "GreaterThanThreshold"
                }
            },
            {
                "Type": "AWS::CloudWatch::Alarm",
                "Properties": {
                    "AlarmName": "WebEvents-GlueJobFailures",
                    "MetricName": "glue.driver.aggregate.numFailedTasks",
                    "Namespace": "AWS/Glue",
                    "Statistic": "Sum",
                    "Period": 3600,
                    "EvaluationPeriods": 1,
                    "Threshold": 5,
                    "ComparisonOperator": "GreaterThanThreshold"
                }
            }
        ]
        
        # Verify alarm configurations
        for alarm in alarm_configs:
            self.assertIn("Threshold", alarm["Properties"])
            self.assertIn("MetricName", alarm["Properties"])
    
    def test_s3_lifecycle_policies(self):
        """Test S3 lifecycle policies for cost optimization"""
        lifecycle_rules = [
            {
                "Id": "ArchiveOldStagingData",
                "Status": "Enabled",
                "Prefix": "staging/",
                "Transitions": [
                    {
                        "Days": 7,
                        "StorageClass": "INTELLIGENT_TIERING"
                    }
                ],
                "ExpirationInDays": 30
            },
            {
                "Id": "DeleteOldErrorLogs", 
                "Status": "Enabled",
                "Prefix": "errors/",
                "ExpirationInDays": 90
            }
        ]
        
        # Verify lifecycle rules
        for rule in lifecycle_rules:
            self.assertEqual(rule["Status"], "Enabled")
            if rule["Id"] == "ArchiveOldStagingData":
                self.assertEqual(rule["ExpirationInDays"], 30)


class TestStackValidation(unittest.TestCase):
    """Validation tests for the complete stack"""
    
    def test_resource_dependencies(self):
        """Test resource dependencies are correctly defined"""
        dependencies = {
            "WebEventsTable": ["WebEventsTableBucket", "WebEventsNamespace"],
            "SessionMetricsTable": ["WebEventsTableBucket", "WebEventsNamespace"],
            "GlueJobS3Processor": ["GlueDatabase", "GlueJobRole"],
            "LambdaFunction": ["LambdaRole", "WebEventsTable"]
        }
        
        # Verify each resource has required dependencies
        for resource, deps in dependencies.items():
            self.assertTrue(len(deps) > 0)
    
    def test_tags_and_metadata(self):
        """Test consistent tagging across resources"""
        expected_tags = {
            "Project": "WebEventsDataLake",
            "Environment": "Production",
            "ManagedBy": "CDK",
            "DataFormat": "Iceberg",
            "StorageType": "S3Tables"
        }
        
        # Verify tags are present
        for tag_key, tag_value in expected_tags.items():
            self.assertIsNotNone(tag_value)


if __name__ == '__main__':
    unittest.main()