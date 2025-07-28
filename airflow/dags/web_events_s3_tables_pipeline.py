from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.lambda_ import LambdaInvokeFunctionOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import boto3
import json

# Default arguments optimized for S3 Tables/Iceberg
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
}

def check_s3_tables_health(**context):
    """Check S3 Tables health and metadata"""
    s3tables_client = boto3.client('s3tables')
    account_id = context['params'].get('account_id', '123456789012')
    
    try:
        # Check table bucket status
        table_bucket_arn = f"arn:aws:s3tables:us-east-1:{account_id}:bucket/peek-web-events-tables-{account_id}"
        
        # Get table information
        tables_response = s3tables_client.list_tables(
            tableBucketARN=table_bucket_arn,
            namespace='analytics'
        )
        
        tables_info = {}
        for table in tables_response.get('tables', []):
            table_name = table['name']
            
            # Get table metadata
            table_metadata = s3tables_client.get_table_metadata_location(
                tableBucketARN=table_bucket_arn,
                namespace='analytics',
                name=table_name
            )
            
            tables_info[table_name] = {
                'arn': table['arn'],
                'created_at': table.get('createdAt'),
                'metadata_location': table_metadata.get('metadataLocation'),
                'status': 'healthy'
            }
        
        print(f"S3 Tables Health Check: {len(tables_info)} tables found")
        return tables_info
        
    except Exception as e:
        print(f"S3 Tables health check failed: {str(e)}")
        raise

def optimize_iceberg_tables(**context):
    """Trigger Iceberg table optimization"""
    athena_client = boto3.client('athena')
    account_id = context['params'].get('account_id', '123456789012')
    
    optimize_queries = [
        # Optimize main web events table
        f"""
        OPTIMIZE s3_tables.analytics.web_events 
        REWRITE DATA USING BIN_PACK
        WHERE event_date >= DATE(CURRENT_TIMESTAMP - INTERVAL '7' DAY)
        """,
        
        # Optimize session metrics table
        f"""
        OPTIMIZE s3_tables.analytics.session_metrics 
        REWRITE DATA USING BIN_PACK  
        WHERE event_date >= DATE(CURRENT_TIMESTAMP - INTERVAL '7' DAY)
        """,
        
        # Expire old snapshots (keep last 30 days)
        f"""
        CALL s3_tables.system.expire_snapshots(
          table => 's3_tables.analytics.web_events',
          older_than => CURRENT_TIMESTAMP - INTERVAL '30' DAY
        )
        """,
        
        f"""
        CALL s3_tables.system.expire_snapshots(
          table => 's3_tables.analytics.session_metrics', 
          older_than => CURRENT_TIMESTAMP - INTERVAL '30' DAY
        )
        """
    ]
    
    for query in optimize_queries:
        try:
            response = athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': f's3://peek-web-events-datalake-{account_id}/athena-optimization/'
                },
                WorkGroup='peek-web-events-workgroup'
            )
            print(f"Started optimization query: {response['QueryExecutionId']}")
        except Exception as e:
            print(f"Optimization query failed: {str(e)}")
            # Don't fail the DAG for optimization issues
            continue

# DAG 1: Historical Data Migration to S3 Tables (One-time)
dag_migration = DAG(
    'web_events_s3_tables_migration',
    default_args=default_args,
    description='Migrate historical web events data to S3 Tables (Iceberg)',
    schedule_interval=None,  # Manual trigger only
    tags=['web-events', 'migration', 's3-tables', 'iceberg'],
    params={
        'source_bucket': 'your-source-s3-bucket',
        'account_id': '{{ var.value.aws_account_id }}'
    }
)

with dag_migration:
    start_migration = DummyOperator(task_id='start_s3_tables_migration')
    
    # Health check S3 Tables infrastructure
    check_infrastructure = PythonOperator(
        task_id='check_s3_tables_infrastructure',
        python_callable=check_s3_tables_health,
        provide_context=True
    )
    
    # Process historical data with new Iceberg-enabled Glue job
    migrate_historical = GlueJobOperator(
        task_id='migrate_historical_to_iceberg',
        job_name='peek-web-events-s3-processor',  # Updated job
        script_args={
            '--SOURCE_BUCKET': '{{ params.source_bucket }}',
            '--OUTPUT_BUCKET': 'peek-web-events-datalake-{{ params.account_id }}',
            '--TABLE_BUCKET_ARN': 'arn:aws:s3tables:us-east-1:{{ params.account_id }}:bucket/peek-web-events-tables-{{ params.account_id }}',
            '--WEB_EVENTS_TABLE': 'arn:aws:s3tables:us-east-1:{{ params.account_id }}:bucket/peek-web-events-tables-{{ params.account_id }}/namespaces/analytics/tables/web_events',
            '--SESSION_METRICS_TABLE': 'arn:aws:s3tables:us-east-1:{{ params.account_id }}:bucket/peek-web-events-tables-{{ params.account_id }}/namespaces/analytics/tables/session_metrics',
        },
        wait_for_completion=True,
        verbose=True,
        aws_conn_id='aws_default',
    )
    
    # Validate migration results
    validate_migration = AthenaOperator(
        task_id='validate_iceberg_migration',
        query='''
        WITH validation_results AS (
          SELECT 
            'web_events' AS table_name,
            COUNT(*) AS record_count,
            COUNT(DISTINCT session_id) AS unique_sessions,
            MIN(event_date) AS earliest_date,
            MAX(event_date) AS latest_date,
            AVG(data_quality_score) AS avg_quality
          FROM s3_tables.analytics.web_events
          
          UNION ALL
          
          SELECT 
            'session_metrics' AS table_name,
            COUNT(*) AS record_count,
            COUNT(DISTINCT session_id) AS unique_sessions,
            MIN(event_date) AS earliest_date,
            MAX(event_date) AS latest_date,
            1.0 AS avg_quality
          FROM s3_tables.analytics.session_metrics
        )
        SELECT * FROM validation_results
        ''',
        database='peek_web_events',
        output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
        workgroup='peek-web-events-workgroup',
        aws_conn_id='aws_default',
    )
    
    # Initial table optimization
    optimize_tables = PythonOperator(
        task_id='optimize_iceberg_tables',
        python_callable=optimize_iceberg_tables,
        provide_context=True
    )
    
    end_migration = DummyOperator(task_id='end_s3_tables_migration')
    
    start_migration >> check_infrastructure >> migrate_historical >> validate_migration >> optimize_tables >> end_migration

# DAG 2: Real-time S3 Tables Processing
dag_realtime = DAG(
    'web_events_s3_tables_realtime',
    default_args=default_args,
    description='Real-time web events processing with S3 Tables/Iceberg',
    schedule_interval=timedelta(minutes=15),  # Check every 15 minutes
    max_active_runs=1,
    tags=['web-events', 'realtime', 's3-tables', 'streaming'],
    params={
        'kinesis_stream_arn': 'arn:aws:kinesis:us-east-1:{{ var.value.aws_account_id }}:stream/peek-web-events-stream',
        'account_id': '{{ var.value.aws_account_id }}'
    }
)

with dag_realtime:
    start_realtime = DummyOperator(task_id='start_realtime_processing')
    
    # Check Kinesis stream health
    check_kinesis = PythonOperator(
        task_id='check_kinesis_stream',
        python_callable=lambda **context: print("Kinesis stream health check"),
        provide_context=True
    )
    
    # Ensure Glue streaming job is running
    start_streaming_job = GlueJobOperator(
        task_id='ensure_streaming_job_running',
        job_name='peek-web-events-kinesis-processor',  # Updated for Iceberg
        script_args={
            '--KINESIS_STREAM_ARN': '{{ params.kinesis_stream_arn }}',
            '--OUTPUT_BUCKET': 'peek-web-events-datalake-{{ params.account_id }}',
            '--TABLE_BUCKET_ARN': 'arn:aws:s3tables:us-east-1:{{ params.account_id }}:bucket/peek-web-events-tables-{{ params.account_id }}',
            '--WEB_EVENTS_TABLE': 'arn:aws:s3tables:us-east-1:{{ params.account_id }}:bucket/peek-web-events-tables-{{ params.account_id }}/namespaces/analytics/tables/web_events',
            '--SESSION_METRICS_TABLE': 'arn:aws:s3tables:us-east-1:{{ params.account_id }}:bucket/peek-web-events-tables-{{ params.account_id }}/namespaces/analytics/tables/session_metrics',
        },
        wait_for_completion=False,  # Streaming job runs continuously
        aws_conn_id='aws_default',
    )
    
    # Check Lambda processor health
    check_lambda = LambdaInvokeFunctionOperator(
        task_id='check_lambda_processor',
        function_name='KinesisProcessor',
        payload=json.dumps({'test': True}),
        aws_conn_id='aws_default',
    )
    
    # Monitor table growth and performance
    monitor_tables = AthenaOperator(
        task_id='monitor_table_performance',
        query='''
        SELECT 
          'real_time_check' AS check_type,
          COUNT(*) AS events_last_hour,
          COUNT(DISTINCT session_id) AS sessions_last_hour,
          AVG(data_quality_score) AS avg_quality_score,
          MAX(processed_timestamp) AS latest_processed
        FROM s3_tables.analytics.web_events
        WHERE processed_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
        ''',
        database='peek_web_events',
        output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
        workgroup='peek-web-events-workgroup',
        aws_conn_id='aws_default',
    )
    
    end_realtime = DummyOperator(task_id='end_realtime_processing')
    
    start_realtime >> [check_kinesis, check_lambda] >> start_streaming_job >> monitor_tables >> end_realtime

# DAG 3: Daily S3 Tables Analytics with Iceberg Features
dag_analytics = DAG(
    'web_events_s3_tables_analytics',
    default_args=default_args,
    description='Daily analytics processing leveraging Iceberg capabilities',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    tags=['web-events', 'analytics', 's3-tables', 'iceberg'],
    params={
        'account_id': '{{ var.value.aws_account_id }}'
    }
)

with dag_analytics:
    start_analytics = DummyOperator(task_id='start_iceberg_analytics')
    
    with TaskGroup('iceberg_features') as iceberg_group:
        # Time travel data validation
        time_travel_check = AthenaOperator(
            task_id='time_travel_validation',
            query='''
            WITH current_count AS (
              SELECT COUNT(*) AS current_events
              FROM s3_tables.analytics.web_events 
              WHERE event_date = DATE('{{ ds }}')
            ),
            historical_count AS (
              SELECT COUNT(*) AS historical_events
              FROM s3_tables.analytics.web_events 
              FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
              WHERE event_date = DATE('{{ ds }}')
            )
            SELECT 
              current_events,
              historical_events,
              current_events - historical_events AS events_added,
              CASE 
                WHEN historical_events > 0 
                THEN (current_events - historical_events) * 100.0 / historical_events 
                ELSE NULL 
              END AS growth_percentage
            FROM current_count, historical_count
            ''',
            database='peek_web_events',
            output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
            workgroup='peek-web-events-workgroup',
            aws_conn_id='aws_default',
        )
        
        # Schema evolution check
        schema_evolution_check = AthenaOperator(
            task_id='check_schema_evolution',
            query='''
            SHOW PARTITIONS s3_tables.analytics.web_events
            ''',
            database='peek_web_events',
            output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
            workgroup='peek-web-events-workgroup',
            aws_conn_id='aws_default',
        )
    
    with TaskGroup('dbt_iceberg_models') as dbt_group:
        # Run dbt models optimized for Iceberg
        run_dbt_staging = BashOperator(
            task_id='run_dbt_staging_models',
            bash_command='cd /opt/airflow/dbt && dbt run --models staging --vars "{\'execution_date\': \'{{ ds }}\'}"',
        )
        
        run_dbt_core = BashOperator(
            task_id='run_dbt_core_iceberg_models',
            bash_command='cd /opt/airflow/dbt && dbt run --models marts.core.fct_web_events_iceberg --vars "{\'execution_date\': \'{{ ds }}\'}"',
        )
        
        run_dbt_analytics = BashOperator(
            task_id='run_dbt_analytics_models',
            bash_command='cd /opt/airflow/dbt && dbt run --models marts.analytics --vars "{\'execution_date\': \'{{ ds }}\'}"',
        )
        
        run_dbt_time_travel = BashOperator(
            task_id='run_dbt_time_travel_analysis',
            bash_command='cd /opt/airflow/dbt && dbt run --models marts.analytics.time_travel_analysis --vars "{\'execution_date\': \'{{ ds }}\'}"',
        )
        
        run_dbt_tests = BashOperator(
            task_id='run_dbt_tests',
            bash_command='cd /opt/airflow/dbt && dbt test --models marts --vars "{\'execution_date\': \'{{ ds }}\'}"',
        )
        
        run_dbt_staging >> run_dbt_core >> [run_dbt_analytics, run_dbt_time_travel] >> run_dbt_tests
    
    with TaskGroup('table_maintenance') as maintenance_group:
        # Compact small files
        compact_tables = PythonOperator(
            task_id='compact_iceberg_tables',
            python_callable=optimize_iceberg_tables,
            provide_context=True
        )
        
        # Update table statistics
        update_statistics = AthenaOperator(
            task_id='update_table_statistics',
            query='''
            ANALYZE TABLE s3_tables.analytics.web_events COMPUTE STATISTICS;
            ANALYZE TABLE s3_tables.analytics.session_metrics COMPUTE STATISTICS;
            ''',
            database='peek_web_events',
            output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
            workgroup='peek-web-events-workgroup',
            aws_conn_id='aws_default',
        )
        
        # Vacuum old files (if needed)
        vacuum_tables = AthenaOperator(
            task_id='vacuum_old_files',
            query='''
            CALL s3_tables.system.remove_orphan_files(
              table => 's3_tables.analytics.web_events',
              older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY
            )
            ''',
            database='peek_web_events',
            output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
            workgroup='peek-web-events-workgroup',
            aws_conn_id='aws_default',
        )
        
        compact_tables >> update_statistics >> vacuum_tables
    
    # Generate daily analytics report
    generate_report = AthenaOperator(
        task_id='generate_daily_report',
        query='''
        CREATE TABLE IF NOT EXISTS peek_web_events.daily_analytics_report_{{ ds_nodash }} AS
        SELECT 
          '{{ ds }}' AS report_date,
          journey_type,
          engagement_tier,
          device_type,
          detailed_region,
          SUM(session_count) AS total_sessions,
          SUM(unique_users) AS total_users,
          AVG(avg_session_duration_minutes) AS avg_duration,
          AVG(conversion_intent_rate) AS avg_conversion_rate,
          SUM(high_intent_sessions) AS high_intent_sessions,
          AVG(engagement_rate) AS avg_engagement_rate
        FROM {{ ref('user_journey_analysis_iceberg') }}
        WHERE analysis_date = DATE('{{ ds }}')
        GROUP BY journey_type, engagement_tier, device_type, detailed_region
        ORDER BY total_sessions DESC
        ''',
        database='peek_web_events',
        output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
        workgroup='peek-web-events-workgroup',
        aws_conn_id='aws_default',
    )
    
    end_analytics = DummyOperator(task_id='end_iceberg_analytics')
    
    start_analytics >> iceberg_group >> dbt_group >> maintenance_group >> generate_report >> end_analytics

# DAG 4: S3 Tables Monitoring and Alerting
dag_monitoring = DAG(
    'web_events_s3_tables_monitoring',
    default_args=default_args,
    description='Monitor S3 Tables performance and data quality',
    schedule_interval=timedelta(hours=1),
    tags=['web-events', 'monitoring', 's3-tables'],
    params={
        'account_id': '{{ var.value.aws_account_id }}'
    }
)

with dag_monitoring:
    start_monitoring = DummyOperator(task_id='start_s3_tables_monitoring')
    
    # Monitor table performance metrics
    monitor_performance = AthenaOperator(
        task_id='monitor_table_performance',
        query='''
        WITH table_metrics AS (
          SELECT 
            'web_events' AS table_name,
            COUNT(*) AS record_count,
            COUNT(DISTINCT event_date) AS date_partitions,
            COUNT(DISTINCT space_type) AS space_partitions,
            MAX(processed_timestamp) AS latest_update,
            AVG(data_quality_score) AS avg_quality,
            COUNT(CASE WHEN is_bot = true THEN 1 END) AS bot_events,
            CURRENT_TIMESTAMP AS check_timestamp
          FROM s3_tables.analytics.web_events
          WHERE processed_timestamp >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
        )
        SELECT 
          *,
          CASE 
            WHEN latest_update < CURRENT_TIMESTAMP - INTERVAL '30' MINUTE THEN 'STALE'
            WHEN avg_quality < 0.8 THEN 'QUALITY_ISSUE' 
            WHEN record_count = 0 THEN 'NO_DATA'
            ELSE 'HEALTHY'
          END AS health_status
        FROM table_metrics
        ''',
        database='peek_web_events',
        output_location='s3://peek-web-events-datalake-{{ params.account_id }}/athena-results/',
        workgroup='peek-web-events-workgroup',
        aws_conn_id='aws_default',
    )
    
    # Check for table locks or issues
    check_table_locks = PythonOperator(
        task_id='check_table_locks',
        python_callable=check_s3_tables_health,
        provide_context=True
    )
    
    end_monitoring = DummyOperator(task_id='end_s3_tables_monitoring')
    
    start_monitoring >> [monitor_performance, check_table_locks] >> end_monitoring