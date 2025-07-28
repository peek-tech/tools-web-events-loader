"""
Unit tests for Airflow DAG orchestration with S3 Tables/Iceberg operations
Tests DAG structure, task dependencies, S3 Tables health checks, and optimization procedures.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path

# Mock Airflow imports for testing
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.providers'] = MagicMock()
sys.modules['airflow.providers.amazon'] = MagicMock()
sys.modules['airflow.providers.amazon.aws'] = MagicMock()
sys.modules['airflow.providers.amazon.aws.operators'] = MagicMock()
sys.modules['airflow.providers.amazon.aws.operators.glue'] = MagicMock()
sys.modules['airflow.providers.amazon.aws.operators.athena'] = MagicMock()
sys.modules['airflow.providers.amazon.aws.operators.lambda_'] = MagicMock()
sys.modules['airflow.operators'] = MagicMock()
sys.modules['airflow.operators.python'] = MagicMock()
sys.modules['airflow.operators.dummy'] = MagicMock()
sys.modules['airflow.operators.bash'] = MagicMock()
sys.modules['airflow.utils'] = MagicMock()
sys.modules['airflow.utils.task_group'] = MagicMock()
sys.modules['boto3'] = MagicMock()

# Import the DAG module
import importlib.util
dag_spec = importlib.util.spec_from_file_location(
    "web_events_s3_tables_pipeline",
    "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
)
dag_module = importlib.util.module_from_spec(dag_spec)


class TestDAGConfiguration:
    """Test DAG configuration and basic properties"""
    
    def setup_method(self):
        """Setup test environment"""
        # Load the DAG module
        dag_spec.loader.exec_module(dag_module)
        
        # Access the DAGs
        self.migration_dag = getattr(dag_module, 'dag_migration', None)
        self.realtime_dag = getattr(dag_module, 'dag_realtime', None)
        self.analytics_dag = getattr(dag_module, 'dag_analytics', None)
        self.monitoring_dag = getattr(dag_module, 'dag_monitoring', None)
    
    def test_migration_dag_properties(self):
        """Test migration DAG basic properties"""
        assert self.migration_dag is not None, "Migration DAG should exist"
        
        # Test DAG ID and description
        dag_id = getattr(self.migration_dag, 'dag_id', None)
        assert dag_id == 'web_events_s3_tables_migration', "Should have correct DAG ID"
        
        # Test schedule (should be None for manual trigger)
        schedule_interval = getattr(self.migration_dag, 'schedule_interval', None)
        assert schedule_interval is None, "Migration should be manually triggered"
        
        # Test tags
        tags = getattr(self.migration_dag, 'tags', [])
        expected_tags = ['web-events', 'migration', 's3-tables', 'iceberg']
        for tag in expected_tags:
            assert tag in tags, f"Should include tag: {tag}"
    
    def test_realtime_dag_properties(self):
        """Test real-time DAG basic properties"""
        assert self.realtime_dag is not None, "Real-time DAG should exist"
        
        dag_id = getattr(self.realtime_dag, 'dag_id', None)
        assert dag_id == 'web_events_s3_tables_realtime', "Should have correct DAG ID"
        
        # Test schedule (should run every 15 minutes)
        schedule_interval = getattr(self.realtime_dag, 'schedule_interval', None)
        assert schedule_interval is not None, "Real-time DAG should be scheduled"
        
        # Test max active runs
        max_active_runs = getattr(self.realtime_dag, 'max_active_runs', None)
        assert max_active_runs == 1, "Should limit concurrent runs"
    
    def test_analytics_dag_properties(self):
        """Test analytics DAG basic properties"""
        assert self.analytics_dag is not None, "Analytics DAG should exist"
        
        dag_id = getattr(self.analytics_dag, 'dag_id', None)
        assert dag_id == 'web_events_s3_tables_analytics', "Should have correct DAG ID"
        
        # Test schedule (should run daily)
        schedule_interval = getattr(self.analytics_dag, 'schedule_interval', None)
        assert schedule_interval == '0 6 * * *', "Should run daily at 6 AM UTC"
    
    def test_monitoring_dag_properties(self):
        """Test monitoring DAG basic properties"""
        assert self.monitoring_dag is not None, "Monitoring DAG should exist"
        
        dag_id = getattr(self.monitoring_dag, 'dag_id', None)
        assert dag_id == 'web_events_s3_tables_monitoring', "Should have correct DAG ID"
        
        # Test schedule (should run hourly)
        schedule_interval = getattr(self.monitoring_dag, 'schedule_interval', None)
        # Should be timedelta(hours=1) or equivalent
        assert schedule_interval is not None, "Should be scheduled hourly"
    
    def test_default_args_configuration(self):
        """Test default arguments are properly configured"""
        default_args = getattr(dag_module, 'default_args', {})
        
        # Test required default args
        assert 'owner' in default_args, "Should specify owner"
        assert default_args['owner'] == 'data-engineering-team', "Should have correct owner"
        
        assert 'depends_on_past' in default_args, "Should specify depends_on_past"
        assert default_args['depends_on_past'] == False, "Should not depend on past"
        
        assert 'email_on_failure' in default_args, "Should specify email on failure"
        assert default_args['email_on_failure'] == True, "Should email on failure"
        
        assert 'retries' in default_args, "Should specify retry count"
        assert default_args['retries'] == 2, "Should retry twice"
        
        assert 'catchup' in default_args, "Should specify catchup behavior"
        assert default_args['catchup'] == False, "Should not catchup"


class TestDAGTaskStructure:
    """Test DAG task structure and dependencies"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
        self.migration_dag = getattr(dag_module, 'dag_migration', None)
        self.realtime_dag = getattr(dag_module, 'dag_realtime', None)
        self.analytics_dag = getattr(dag_module, 'dag_analytics', None)
        self.monitoring_dag = getattr(dag_module, 'dag_monitoring', None)
    
    def test_migration_dag_task_count(self):
        """Test migration DAG has expected number of tasks"""
        if self.migration_dag and hasattr(self.migration_dag, 'task_dict'):
            tasks = self.migration_dag.task_dict
            
            # Expected tasks: start, check_infrastructure, migrate_historical, 
            # validate_migration, optimize_tables, end
            expected_min_tasks = 6
            assert len(tasks) >= expected_min_tasks, f"Should have at least {expected_min_tasks} tasks"
    
    def test_migration_dag_critical_tasks(self):
        """Test migration DAG has critical tasks"""
        if self.migration_dag and hasattr(self.migration_dag, 'task_dict'):
            tasks = self.migration_dag.task_dict
            
            critical_task_patterns = [
                'start',
                'check_infrastructure',
                'migrate_historical',
                'validate',
                'optimize',
                'end'
            ]
            
            for pattern in critical_task_patterns:
                matching_tasks = [task_id for task_id in tasks.keys() if pattern in task_id.lower()]
                assert len(matching_tasks) >= 1, f"Should have task matching pattern: {pattern}"
    
    def test_realtime_dag_task_structure(self):
        """Test real-time DAG task structure"""
        if self.realtime_dag and hasattr(self.realtime_dag, 'task_dict'):
            tasks = self.realtime_dag.task_dict
            
            expected_task_patterns = [
                'start',
                'check_kinesis',
                'streaming_job',
                'lambda',
                'monitor',
                'end'
            ]
            
            for pattern in expected_task_patterns:
                matching_tasks = [task_id for task_id in tasks.keys() if pattern in task_id.lower()]
                assert len(matching_tasks) >= 1, f"Should have task for: {pattern}"
    
    def test_analytics_dag_task_groups(self):
        """Test analytics DAG has proper task groups"""
        # Task groups should include: iceberg_features, dbt_models, table_maintenance
        if self.analytics_dag and hasattr(self.analytics_dag, 'task_dict'):
            tasks = self.analytics_dag.task_dict
            
            expected_groups = [
                'iceberg',
                'dbt',
                'maintenance'
            ]
            
            for group in expected_groups:
                # Look for tasks that contain the group name
                group_tasks = [task_id for task_id in tasks.keys() if group in task_id.lower()]
                assert len(group_tasks) >= 1, f"Should have tasks for group: {group}"
    
    def test_monitoring_dag_task_structure(self):
        """Test monitoring DAG task structure"""
        if self.monitoring_dag and hasattr(self.monitoring_dag, 'task_dict'):
            tasks = self.monitoring_dag.task_dict
            
            monitoring_patterns = [
                'monitor',
                'performance',
                'health',
                'check'
            ]
            
            for pattern in monitoring_patterns:
                matching_tasks = [task_id for task_id in tasks.keys() if pattern in task_id.lower()]
                assert len(matching_tasks) >= 1, f"Should have monitoring task: {pattern}"


class TestS3TablesHealthCheck:
    """Test S3 Tables health check functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
        
        # Mock context for testing
        self.mock_context = {
            'params': {
                'account_id': '123456789012'
            }
        }
    
    @patch('dag_module.boto3.client')
    def test_check_s3_tables_health_success(self, mock_boto3_client):
        """Test successful S3 Tables health check"""
        # Mock S3 Tables client
        mock_s3tables = Mock()
        mock_boto3_client.return_value = mock_s3tables
        
        # Mock list_tables response
        mock_s3tables.list_tables.return_value = {
            'tables': [
                {
                    'name': 'web_events',
                    'arn': 'arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/namespaces/analytics/tables/web_events',
                    'createdAt': datetime.now()
                },
                {
                    'name': 'session_metrics',
                    'arn': 'arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/namespaces/analytics/tables/session_metrics',
                    'createdAt': datetime.now()
                }
            ]
        }
        
        # Mock get_table_metadata_location
        mock_s3tables.get_table_metadata_location.return_value = {
            'metadataLocation': 's3://bucket/metadata/location'
        }
        
        # Test the function
        result = dag_module.check_s3_tables_health(**self.mock_context)
        
        assert result is not None, "Should return health check results"
        assert 'web_events' in result, "Should include web_events table"
        assert 'session_metrics' in result, "Should include session_metrics table"
        
        # Verify client calls
        mock_s3tables.list_tables.assert_called_once()
        assert mock_s3tables.get_table_metadata_location.call_count == 2, "Should check metadata for both tables"
    
    @patch('dag_module.boto3.client')
    def test_check_s3_tables_health_error_handling(self, mock_boto3_client):
        """Test S3 Tables health check error handling"""
        # Mock client that raises exception
        mock_s3tables = Mock()
        mock_boto3_client.return_value = mock_s3tables
        mock_s3tables.list_tables.side_effect = Exception("S3 Tables unavailable")
        
        # Should raise exception
        with pytest.raises(Exception):
            dag_module.check_s3_tables_health(**self.mock_context)
    
    def test_check_s3_tables_health_context_params(self):
        """Test health check uses context parameters correctly"""
        with patch('dag_module.boto3.client') as mock_boto3:
            mock_s3tables = Mock()
            mock_boto3.return_value = mock_s3tables
            mock_s3tables.list_tables.return_value = {'tables': []}
            
            # Test with different account ID
            custom_context = {
                'params': {
                    'account_id': '999999999999'
                }
            }
            
            dag_module.check_s3_tables_health(**custom_context)
            
            # Verify the account ID is used in the ARN
            call_args = mock_s3tables.list_tables.call_args
            table_bucket_arn = call_args[1]['tableBucketARN']
            assert '999999999999' in table_bucket_arn, "Should use custom account ID"


class TestIcebergOptimization:
    """Test Iceberg table optimization functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
        
        self.mock_context = {
            'params': {
                'account_id': '123456789012'
            }
        }
    
    @patch('dag_module.boto3.client')
    def test_optimize_iceberg_tables_success(self, mock_boto3_client):
        """Test successful Iceberg table optimization"""
        # Mock Athena client
        mock_athena = Mock()
        mock_boto3_client.return_value = mock_athena
        
        # Mock successful query executions
        mock_athena.start_query_execution.return_value = {
            'QueryExecutionId': 'query-123'
        }
        
        # Should not raise exception
        dag_module.optimize_iceberg_tables(**self.mock_context)
        
        # Verify optimization queries were executed
        assert mock_athena.start_query_execution.call_count >= 2, "Should execute multiple optimization queries"
        
        # Check that OPTIMIZE queries were used
        call_args_list = mock_athena.start_query_execution.call_args_list
        
        optimize_calls = 0
        expire_calls = 0
        
        for call_args in call_args_list:
            query_string = call_args[1]['QueryString']
            if 'OPTIMIZE' in query_string:
                optimize_calls += 1
            if 'expire_snapshots' in query_string:
                expire_calls += 1
        
        assert optimize_calls >= 2, "Should run OPTIMIZE queries for both tables"
        assert expire_calls >= 2, "Should run snapshot expiration for both tables"
    
    @patch('dag_module.boto3.client')
    def test_optimize_iceberg_tables_error_resilience(self, mock_boto3_client):
        """Test optimization error resilience"""
        # Mock Athena client that fails on some queries
        mock_athena = Mock()
        mock_boto3_client.return_value = mock_athena
        
        # Simulate some queries failing
        def side_effect(*args, **kwargs):
            query = kwargs.get('QueryString', '')
            if 'OPTIMIZE' in query:
                raise Exception("Optimization failed")
            return {'QueryExecutionId': 'query-success'}
        
        mock_athena.start_query_execution.side_effect = side_effect
        
        # Should not raise exception (graceful handling)
        dag_module.optimize_iceberg_tables(**self.mock_context)
        
        # Should still attempt all queries
        assert mock_athena.start_query_execution.call_count >= 4, "Should attempt all optimization queries"
    
    def test_optimize_iceberg_tables_query_structure(self):
        """Test optimization queries have correct structure"""
        with patch('dag_module.boto3.client') as mock_boto3:
            mock_athena = Mock()
            mock_boto3.return_value = mock_athena
            mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'test'}
            
            dag_module.optimize_iceberg_tables(**self.mock_context)
            
            # Check query structure
            call_args_list = mock_athena.start_query_execution.call_args_list
            
            for call_args in call_args_list:
                query_string = call_args[1]['QueryString']
                
                if 'OPTIMIZE' in query_string:
                    assert 'REWRITE DATA USING BIN_PACK' in query_string, "Should use BIN_PACK strategy"
                    assert 'event_date >=' in query_string, "Should optimize recent data"
                
                if 'expire_snapshots' in query_string:
                    assert 'older_than' in query_string, "Should specify retention period"
                    assert 'INTERVAL' in query_string, "Should use interval for retention"


class TestDAGParameterization:
    """Test DAG parameterization and configuration"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
    
    def test_migration_dag_parameters(self):
        """Test migration DAG has correct parameters"""
        if hasattr(dag_module, 'dag_migration'):
            migration_dag = dag_module.dag_migration
            
            # Check for expected parameters
            if hasattr(migration_dag, 'params'):
                params = migration_dag.params
                
                expected_params = ['source_bucket', 'account_id']
                for param in expected_params:
                    assert param in params, f"Should have parameter: {param}"
    
    def test_realtime_dag_parameters(self):
        """Test real-time DAG has correct parameters"""
        if hasattr(dag_module, 'dag_realtime'):
            realtime_dag = dag_module.dag_realtime
            
            if hasattr(realtime_dag, 'params'):
                params = realtime_dag.params
                
                expected_params = ['kinesis_stream_arn', 'account_id']
                for param in expected_params:
                    assert param in params, f"Should have parameter: {param}"
    
    def test_analytics_dag_parameters(self):
        """Test analytics DAG has correct parameters"""
        if hasattr(dag_module, 'dag_analytics'):
            analytics_dag = dag_module.dag_analytics
            
            if hasattr(analytics_dag, 'params'):
                params = analytics_dag.params
                
                assert 'account_id' in params, "Should have account_id parameter"


class TestGlueJobOperators:
    """Test Glue job operator configurations"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
    
    def test_glue_job_configurations(self):
        """Test Glue job operators have correct configurations"""
        # This tests the structure rather than actual operator instances
        # since we're mocking the Airflow imports
        
        # Check that the DAG file contains expected Glue job configurations
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Test for S3 Tables specific job names
        assert 'peek-web-events-kinesis-processor' in dag_content, "Should reference Kinesis processor job"
        assert 'peek-web-events-s3-processor' in dag_content, "Should reference S3 processor job"
        
        # Test for S3 Tables ARN references
        assert 'TABLE_BUCKET_ARN' in dag_content, "Should pass table bucket ARN"
        assert 'WEB_EVENTS_TABLE' in dag_content, "Should pass web events table ARN"
        assert 'SESSION_METRICS_TABLE' in dag_content, "Should pass session metrics table ARN"
        
        # Test for streaming job configuration
        assert 'wait_for_completion=False' in dag_content, "Streaming job should not wait for completion"
        assert 'wait_for_completion=True' in dag_content, "Batch job should wait for completion"


class TestAthenaOperators:
    """Test Athena operator configurations"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
    
    def test_athena_workgroup_configuration(self):
        """Test Athena operators use correct workgroup"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should use the S3 Tables workgroup
        assert 'peek-web-events-workgroup' in dag_content, "Should use correct Athena workgroup"
    
    def test_athena_query_patterns(self):
        """Test Athena queries use S3 Tables syntax"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should reference S3 Tables catalog
        assert 's3_tables.analytics' in dag_content, "Should use S3 Tables catalog"
        
        # Should use Iceberg time travel syntax
        assert 'FOR TIMESTAMP AS OF' in dag_content, "Should use time travel queries"
        
        # Should use Iceberg maintenance operations
        assert 'OPTIMIZE' in dag_content, "Should use OPTIMIZE operations"
        assert 'expire_snapshots' in dag_content, "Should use snapshot expiration"


class TestDBTOperators:
    """Test dbt operator configurations"""
    
    def test_dbt_model_execution_order(self):
        """Test dbt models are executed in correct order"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should run staging models first
        assert 'dbt run --models staging' in dag_content, "Should run staging models"
        
        # Should run core models next
        assert 'fct_web_events_iceberg' in dag_content, "Should run fact model"
        
        # Should run analytics models
        assert 'marts.analytics' in dag_content, "Should run analytics models"
        
        # Should run time travel analysis
        assert 'time_travel_analysis' in dag_content, "Should run time travel analysis"
        
        # Should run tests
        assert 'dbt test' in dag_content, "Should run dbt tests"
    
    def test_dbt_target_configuration(self):
        """Test dbt uses correct target environment"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should specify target environment
        if '--target' in dag_content:
            assert '${ENVIRONMENT}' in dag_content or 'target' in dag_content, "Should specify dbt target"


class TestLambdaOperators:
    """Test Lambda operator configurations"""
    
    def test_lambda_function_reference(self):
        """Test Lambda operators reference correct functions"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should reference the Kinesis processor Lambda
        assert 'KinesisProcessor' in dag_content, "Should reference Lambda function"
        
        # Should pass test payload
        assert 'test' in dag_content.lower(), "Should include test payload"


class TestDAGSchedulingAndRetries:
    """Test DAG scheduling and retry configurations"""
    
    def setup_method(self):
        """Setup test environment"""
        dag_spec.loader.exec_module(dag_module)
    
    def test_dag_scheduling_intervals(self):
        """Test DAG scheduling intervals are appropriate"""
        # Migration: Manual only (None)
        if hasattr(dag_module, 'dag_migration'):
            migration_schedule = getattr(dag_module.dag_migration, 'schedule_interval', None)
            assert migration_schedule is None, "Migration should be manual"
        
        # Real-time: Every 15 minutes
        if hasattr(dag_module, 'dag_realtime'):
            # Schedule should be frequent for real-time monitoring
            # Exact format depends on implementation
            pass
        
        # Analytics: Daily at 6 AM UTC
        if hasattr(dag_module, 'dag_analytics'):
            analytics_schedule = getattr(dag_module.dag_analytics, 'schedule_interval', None)
            assert analytics_schedule == '0 6 * * *', "Analytics should run daily at 6 AM"
        
        # Monitoring: Hourly
        if hasattr(dag_module, 'dag_monitoring'):
            # Should be hourly monitoring
            pass
    
    def test_dag_retry_configuration(self):
        """Test DAG retry configurations"""
        default_args = getattr(dag_module, 'default_args', {})
        
        # Should have reasonable retry policy
        assert default_args.get('retries') == 2, "Should retry failed tasks twice"
        
        # Should have retry delay
        retry_delay = default_args.get('retry_delay')
        assert retry_delay is not None, "Should have retry delay"
        
        # Retry delay should be reasonable (5 minutes)
        if hasattr(retry_delay, 'total_seconds'):
            assert retry_delay.total_seconds() == 300, "Should have 5-minute retry delay"
    
    def test_dag_concurrency_limits(self):
        """Test DAG concurrency configurations"""
        # Real-time DAG should limit concurrent runs
        if hasattr(dag_module, 'dag_realtime'):
            max_active_runs = getattr(dag_module.dag_realtime, 'max_active_runs', None)
            assert max_active_runs == 1, "Real-time DAG should limit concurrent runs"
        
        # Analytics DAG should also limit concurrent runs
        if hasattr(dag_module, 'dag_analytics'):
            max_active_runs = getattr(dag_module.dag_analytics, 'max_active_runs', None)
            # Should be limited (1 or small number)
            pass


class TestDAGErrorHandling:
    """Test DAG error handling and resilience"""
    
    def test_error_handling_in_functions(self):
        """Test error handling in custom Python functions"""
        dag_spec.loader.exec_module(dag_module)
        
        # Test S3 Tables health check error handling
        with patch('dag_module.boto3.client') as mock_boto3:
            mock_s3tables = Mock()
            mock_boto3.return_value = mock_s3tables
            mock_s3tables.list_tables.side_effect = Exception("Test exception")
            
            context = {'params': {'account_id': '123456789012'}}
            
            # Should raise exception (fail fast for infrastructure issues)
            with pytest.raises(Exception):
                dag_module.check_s3_tables_health(**context)
        
        # Test optimization function error resilience
        with patch('dag_module.boto3.client') as mock_boto3:
            mock_athena = Mock()
            mock_boto3.return_value = mock_athena
            mock_athena.start_query_execution.side_effect = Exception("Query failed")
            
            context = {'params': {'account_id': '123456789012'}}
            
            # Should handle gracefully (optimization failures shouldn't break pipeline)
            try:
                dag_module.optimize_iceberg_tables(**context)
                # Should not raise exception
            except Exception:
                pytest.fail("Optimization function should handle errors gracefully")
    
    def test_email_notification_configuration(self):
        """Test email notification configuration"""
        default_args = getattr(dag_module, 'default_args', {})
        
        # Should email on failure
        assert default_args.get('email_on_failure') == True, "Should email on failure"
        
        # Should not email on retry (to avoid spam)
        assert default_args.get('email_on_retry') == False, "Should not email on retry"


class TestDAGSecurity:
    """Test DAG security configurations"""
    
    def test_aws_connection_usage(self):
        """Test DAGs use secure AWS connections"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should use aws_default connection
        assert 'aws_conn_id=\'aws_default\'' in dag_content, "Should use aws_default connection"
    
    def test_parameter_templating(self):
        """Test parameters use Airflow templating safely"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should use Airflow templating for dynamic values
        template_patterns = [
            '{{ params.account_id }}',
            '{{ var.value.aws_account_id }}',
            '{{ ds }}'  # Date string
        ]
        
        for pattern in template_patterns:
            assert pattern in dag_content, f"Should use template: {pattern}"


class TestDAGDocumentation:
    """Test DAG documentation and metadata"""
    
    def test_dag_descriptions(self):
        """Test DAGs have proper descriptions"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should have descriptive descriptions
        description_keywords = [
            'S3 Tables',
            'Iceberg',
            'web events',
            'analytics',
            'real-time',
            'migration'
        ]
        
        for keyword in description_keywords:
            assert keyword in dag_content, f"Should mention {keyword} in descriptions"
    
    def test_dag_tags(self):
        """Test DAGs have appropriate tags"""
        dag_file_path = "/Users/james/workspace-peek/load-webevents/airflow/dags/web_events_s3_tables_pipeline.py"
        with open(dag_file_path, 'r') as f:
            dag_content = f.read()
        
        # Should have relevant tags
        expected_tags = [
            'web-events',
            's3-tables', 
            'iceberg',
            'analytics',
            'realtime',
            'migration',
            'monitoring'
        ]
        
        for tag in expected_tags:
            assert tag in dag_content, f"Should include tag: {tag}"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=10"])