"""
Unit tests for Iceberg schema definitions and partitioning
Tests SQL schema creation, table properties, partitioning strategies, and Iceberg-specific configurations.
"""

import pytest
import re
from pathlib import Path
from typing import List, Dict, Any


class TestIcebergSchemaStructure:
    """Test Iceberg schema structure and definitions"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_main_web_events_table_creation(self):
        """Test main web events table is created with correct structure"""
        # Check for table creation statement
        assert "CREATE TABLE IF NOT EXISTS s3_tables.analytics.web_events" in self.schema_sql
        
        # Check for USING iceberg clause
        assert "USING iceberg" in self.schema_sql.lower()
        
        # Check for essential columns
        essential_columns = [
            'event_id STRING',
            'event_timestamp TIMESTAMP',
            'event_date DATE',
            'event_type STRING',
            'anonymous_id STRING',
            'session_id STRING',
            'space_id STRING',
            'space_type STRING',
            'data_quality_score DOUBLE',
            'processed_timestamp TIMESTAMP'
        ]
        
        for column in essential_columns:
            assert column in self.schema_sql, f"Should define column: {column}"
    
    def test_web_events_table_partitioning(self):
        """Test web events table partitioning strategy"""
        # Should be partitioned by event_date and space_type
        partition_pattern = r"PARTITIONED BY\s*\(\s*event_date\s*,\s*space_type\s*\)"
        assert re.search(partition_pattern, self.schema_sql), "Should partition by event_date and space_type"
    
    def test_web_events_table_properties(self):
        """Test web events table Iceberg properties"""
        # Check for TBLPROPERTIES section
        assert "TBLPROPERTIES" in self.schema_sql, "Should have table properties"
        
        # Check for essential Iceberg properties
        iceberg_properties = [
            'write.target-file-size-bytes',
            'write.format.default',
            'write.parquet.compression-codec',
            'format-version'
        ]
        
        for prop in iceberg_properties:
            assert prop in self.schema_sql, f"Should include Iceberg property: {prop}"
        
        # Check for specific values
        assert "'parquet'" in self.schema_sql, "Should use Parquet format"
        assert "'zstd'" in self.schema_sql, "Should use ZSTD compression"
        assert "'merge-on-read'" in self.schema_sql, "Should use merge-on-read for streaming"
    
    def test_web_events_table_comments(self):
        """Test web events table has proper column comments"""
        # Check for COMMENT clauses
        comment_patterns = [
            "COMMENT 'Unique event identifier",
            "COMMENT 'Event occurrence timestamp",
            "COMMENT 'Type of space",
            "COMMENT 'Data completeness score"
        ]
        
        for pattern in comment_patterns:
            assert pattern in self.schema_sql, f"Should have comment: {pattern}"
    
    def test_session_metrics_table_creation(self):
        """Test session metrics table is created correctly"""
        # Check for table creation
        assert "CREATE TABLE IF NOT EXISTS s3_tables.analytics.session_metrics" in self.schema_sql
        
        # Check for essential session-level columns
        session_columns = [
            'session_id STRING',
            'anonymous_id STRING',
            'session_start TIMESTAMP',
            'session_end TIMESTAMP',
            'session_duration_seconds BIGINT',
            'total_events BIGINT',
            'unique_spaces_viewed BIGINT',
            'bounce_session BOOLEAN',
            'space_types_viewed ARRAY<STRING>'
        ]
        
        for column in session_columns:
            assert column in self.schema_sql, f"Should define session column: {column}"
    
    def test_session_metrics_partitioning(self):
        """Test session metrics table partitioning"""
        # Should be partitioned by event_date only
        session_partition_pattern = r"session_metrics.*PARTITIONED BY\s*\(\s*event_date\s*\)"
        assert re.search(session_partition_pattern, self.schema_sql, re.DOTALL), "Should partition session metrics by event_date"
    
    def test_user_journey_analysis_table(self):
        """Test user journey analysis table definition"""
        # Check for table creation
        assert "CREATE TABLE IF NOT EXISTS s3_tables.analytics.user_journey_analysis" in self.schema_sql
        
        # Check for journey-specific columns
        journey_columns = [
            'analysis_date DATE',
            'journey_type STRING',
            'engagement_tier STRING',
            'conversion_intent_level STRING',
            'session_count BIGINT',
            'unique_users BIGINT',
            'avg_session_duration_minutes DOUBLE',
            'bounce_category STRING',
            'conversion_segment STRING'
        ]
        
        for column in journey_columns:
            assert column in self.schema_sql, f"Should define journey column: {column}"
    
    def test_time_travel_analysis_table(self):
        """Test time travel analysis table definition"""
        # Check for table creation
        assert "CREATE TABLE IF NOT EXISTS s3_tables.analytics.time_travel_analysis" in self.schema_sql
        
        # Check for time travel specific columns
        time_travel_columns = [
            'event_date DATE',
            'space_type STRING',
            'current_events BIGINT',
            'historical_events BIGINT',
            'events_change BIGINT',
            'events_change_pct DOUBLE',
            'events_variance_flag STRING',
            'data_freshness_status STRING'
        ]
        
        for column in time_travel_columns:
            assert column in self.schema_sql, f"Should define time travel column: {column}"


class TestIcebergTableProperties:
    """Test Iceberg-specific table properties and configurations"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_file_size_targeting(self):
        """Test file size targeting properties"""
        # Check for different file size targets for different tables
        file_size_patterns = [
            "'write.target-file-size-bytes'='134217728'",  # 128MB for main table
            "'write.target-file-size-bytes'='67108864'",   # 64MB for smaller tables
            "'write.target-file-size-bytes'='33554432'"    # 32MB for analysis tables
        ]
        
        found_patterns = 0
        for pattern in file_size_patterns:
            if pattern in self.schema_sql:
                found_patterns += 1
        
        assert found_patterns >= 2, "Should have different file size targets for different table types"
    
    def test_compression_configuration(self):
        """Test compression codec configuration"""
        # Should use ZSTD compression for all tables
        compression_pattern = r"'write\.parquet\.compression-codec'\s*=\s*'zstd'"
        matches = re.findall(compression_pattern, self.schema_sql)
        assert len(matches) >= 3, "Should configure ZSTD compression for multiple tables"
    
    def test_format_version_specification(self):
        """Test Iceberg format version specification"""
        # Should specify format version 2 for new features
        format_version_pattern = r"'format-version'\s*=\s*'2'"
        assert re.search(format_version_pattern, self.schema_sql), "Should use Iceberg format version 2"
    
    def test_commit_retry_configuration(self):
        """Test commit retry configuration for reliability"""
        # Should have commit retry settings
        retry_patterns = [
            "'commit.retry.num-retries'",
            "'commit.retry.min-wait-ms'"
        ]
        
        for pattern in retry_patterns:
            assert pattern in self.schema_sql, f"Should configure commit retry: {pattern}"
    
    def test_merge_mode_configuration(self):
        """Test merge mode configuration for streaming"""
        # Main events table should use merge-on-read for streaming
        merge_mode_pattern = r"'write\.merge\.mode'\s*=\s*'merge-on-read'"
        assert re.search(merge_mode_pattern, self.schema_sql), "Should configure merge-on-read for streaming table"


class TestPartitioningStrategies:
    """Test partitioning strategies for different table types"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_main_table_partitioning_strategy(self):
        """Test main web events table partitioning strategy"""
        # Should partition by event_date and space_type
        main_partition_pattern = r"web_events.*PARTITIONED BY\s*\(\s*event_date\s*,\s*space_type\s*\)"
        assert re.search(main_partition_pattern, self.schema_sql, re.DOTALL), "Main table should partition by date and space_type"
        
        # This partitioning strategy should:
        # 1. Enable efficient time-based queries (event_date)
        # 2. Enable efficient space-type filtering (space_type)
        # 3. Distribute data evenly across partitions
    
    def test_session_metrics_partitioning_strategy(self):
        """Test session metrics table partitioning strategy"""
        # Should partition by event_date only
        session_partition_pattern = r"session_metrics.*PARTITIONED BY\s*\(\s*event_date\s*\)"
        assert re.search(session_partition_pattern, self.schema_sql, re.DOTALL), "Session metrics should partition by event_date only"
        
        # Session-level data doesn't need space_type partitioning as it's aggregated
    
    def test_analytics_table_partitioning_strategy(self):
        """Test analytics tables partitioning strategy"""
        # User journey analysis should partition by analysis_date
        journey_partition_pattern = r"user_journey_analysis.*PARTITIONED BY\s*\(\s*analysis_date\s*\)"
        assert re.search(journey_partition_pattern, self.schema_sql, re.DOTALL), "Journey analysis should partition by analysis_date"
        
        # Time travel analysis should partition by event_date
        time_travel_partition_pattern = r"time_travel_analysis.*PARTITIONED BY\s*\(\s*event_date\s*\)"
        assert re.search(time_travel_partition_pattern, self.schema_sql, re.DOTALL), "Time travel analysis should partition by event_date"
    
    def test_partition_column_data_types(self):
        """Test partition columns have appropriate data types"""
        # event_date should be DATE type
        date_column_pattern = r"event_date\s+DATE"
        assert re.search(date_column_pattern, self.schema_sql), "event_date should be DATE type"
        
        # analysis_date should be DATE type
        analysis_date_pattern = r"analysis_date\s+DATE"
        assert re.search(analysis_date_pattern, self.schema_sql), "analysis_date should be DATE type"
        
        # space_type should be STRING type
        space_type_pattern = r"space_type\s+STRING"
        assert re.search(space_type_pattern, self.schema_sql), "space_type should be STRING type"


class TestDataTypes:
    """Test data type specifications and constraints"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_timestamp_data_types(self):
        """Test timestamp columns use correct data types"""
        timestamp_columns = [
            'event_timestamp TIMESTAMP',
            'session_start TIMESTAMP',
            'session_end TIMESTAMP',
            'processed_timestamp TIMESTAMP'
        ]
        
        for column in timestamp_columns:
            assert column in self.schema_sql, f"Should define timestamp column: {column}"
    
    def test_numeric_data_types(self):
        """Test numeric columns use appropriate data types"""
        # Integer types
        integer_columns = [
            'event_hour INT',
            'screen_width INT',
            'screen_height INT',
            'session_event_sequence INT'
        ]
        
        for column in integer_columns:
            assert column in self.schema_sql, f"Should define integer column: {column}"
        
        # Long integer types
        bigint_columns = [
            'total_events BIGINT',
            'session_duration_seconds BIGINT',
            'seconds_utc_ts BIGINT',
            'time_on_page_seconds BIGINT'
        ]
        
        for column in bigint_columns:
            assert column in self.schema_sql, f"Should define bigint column: {column}"
        
        # Double precision types
        double_columns = [
            'data_quality_score DOUBLE',
            'avg_session_duration_minutes DOUBLE',
            'avg_engagement_score DOUBLE'
        ]
        
        for column in double_columns:
            assert column in self.schema_sql, f"Should define double column: {column}"
    
    def test_boolean_data_types(self):
        """Test boolean columns are properly defined"""
        boolean_columns = [
            'is_first_event_in_session BOOLEAN',
            'bounce_session BOOLEAN',
            'is_virtual_tour_page BOOLEAN',
            'has_space_context BOOLEAN',
            'is_bot BOOLEAN'
        ]
        
        for column in boolean_columns:
            assert column in self.schema_sql, f"Should define boolean column: {column}"
    
    def test_array_data_types(self):
        """Test array columns are properly defined"""
        array_columns = [
            'space_types_viewed ARRAY<STRING>'
        ]
        
        for column in array_columns:
            assert column in self.schema_sql, f"Should define array column: {column}"
    
    def test_string_data_types(self):
        """Test string columns are properly defined"""
        # Key string columns that should be present
        string_columns = [
            'event_id STRING',
            'event_type STRING',
            'anonymous_id STRING',
            'session_id STRING',
            'page_url STRING',
            'user_agent STRING',
            'space_id STRING',
            'space_name STRING',
            'space_type STRING'
        ]
        
        for column in string_columns:
            assert column in self.schema_sql, f"Should define string column: {column}"


class TestExternalTableMappings:
    """Test external table mappings for Athena/Redshift compatibility"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_external_table_creation(self):
        """Test external table for Athena compatibility"""
        # Should create external table mapping
        assert "CREATE OR REPLACE EXTERNAL TABLE" in self.schema_sql, "Should create external table"
        assert "peek_web_events.web_events_external" in self.schema_sql, "Should name external table correctly"
    
    def test_external_table_format_specification(self):
        """Test external table uses Iceberg input/output formats"""
        # Should specify Iceberg formats
        iceberg_formats = [
            "INPUTFORMAT 'org.apache.iceberg.mr.hive.HiveIcebergInputFormat'",
            "OUTPUTFORMAT 'org.apache.iceberg.mr.hive.HiveIcebergOutputFormat'"
        ]
        
        for format_spec in iceberg_formats:
            assert format_spec in self.schema_sql, f"Should specify format: {format_spec}"
    
    def test_external_table_location(self):
        """Test external table location points to S3 Tables"""
        # Should point to S3 Tables location
        location_pattern = r"LOCATION\s+'s3://peek-web-events-tables-ACCOUNT_ID/namespaces/analytics/tables/web_events/'"
        assert re.search(location_pattern, self.schema_sql), "Should specify S3 Tables location"
    
    def test_external_table_properties(self):
        """Test external table has correct properties"""
        # Should specify table type
        table_properties = [
            "'table_type'='ICEBERG'",
            "'format'='iceberg'"
        ]
        
        for prop in table_properties:
            assert prop in self.schema_sql, f"Should specify property: {prop}"


class TestViewDefinitions:
    """Test view definitions for performance and monitoring"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_realtime_metrics_view(self):
        """Test real-time metrics view definition"""
        # Should create real-time metrics view
        assert "CREATE OR REPLACE VIEW s3_tables.analytics.realtime_metrics" in self.schema_sql
        
        # Should aggregate by hour
        assert "DATE_TRUNC('hour', event_timestamp)" in self.schema_sql, "Should aggregate by hour"
        
        # Should filter recent data
        assert "CURRENT_TIMESTAMP - INTERVAL '24' HOUR" in self.schema_sql, "Should filter last 24 hours"
        
        # Should exclude bots and low quality data
        assert "NOT is_bot" in self.schema_sql, "Should exclude bots"
        assert "data_quality_score >= 0.7" in self.schema_sql, "Should filter by quality"
    
    def test_data_quality_monitor_view(self):
        """Test data quality monitoring view definition"""
        # Should create data quality monitor view
        assert "CREATE OR REPLACE VIEW s3_tables.analytics.data_quality_monitor" in self.schema_sql
        
        # Should count missing values
        missing_value_checks = [
            "COUNT(CASE WHEN session_id IS NULL THEN 1 END) AS missing_session_id",
            "COUNT(CASE WHEN anonymous_id IS NULL THEN 1 END) AS missing_anonymous_id",
            "COUNT(CASE WHEN user_agent IS NULL THEN 1 END) AS missing_user_agent"
        ]
        
        for check in missing_value_checks:
            assert check in self.schema_sql, f"Should include quality check: {check}"
        
        # Should calculate quality metrics
        quality_metrics = [
            "AVG(data_quality_score) AS avg_quality_score",
            "MIN(data_quality_score) AS min_quality_score",
            "MAX(processed_timestamp) AS latest_processed"
        ]
        
        for metric in quality_metrics:
            assert metric in self.schema_sql, f"Should calculate metric: {metric}"


class TestSchemaEvolutionSupport:
    """Test schema evolution and backward compatibility support"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_nullable_columns(self):
        """Test that columns are appropriately nullable for schema evolution"""
        # Most columns should allow NULL for flexibility
        # Only critical columns like event_id, timestamps should be implicitly non-null
        
        # Check that we don't have excessive NOT NULL constraints
        not_null_count = self.schema_sql.count("NOT NULL")
        
        # Should be minimal NOT NULL constraints to allow schema evolution
        assert not_null_count <= 5, "Should minimize NOT NULL constraints for schema flexibility"
    
    def test_extensibility_columns(self):
        """Test that schema includes columns for extensibility"""
        # Should have JSON column for raw properties
        assert "properties_json STRING" in self.schema_sql, "Should include properties_json for extensibility"
        
        # Should have processing metadata for debugging
        assert "processed_timestamp TIMESTAMP" in self.schema_sql, "Should include processing metadata"
    
    def test_iceberg_format_version(self):
        """Test that tables use Iceberg format version 2 for schema evolution"""
        # Format version 2 supports better schema evolution
        format_version_pattern = r"'format-version'\s*=\s*'2'"
        assert re.search(format_version_pattern, self.schema_sql), "Should use format version 2 for schema evolution"


class TestPerformanceOptimizations:
    """Test performance optimization configurations"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_file_size_optimization(self):
        """Test file size targeting for performance"""
        # Different tables should have appropriate file sizes
        file_size_configs = [
            ("134217728", "128MB"),  # Main table - larger files for better compression
            ("67108864", "64MB"),    # Medium tables
            ("33554432", "32MB")     # Small analytical tables
        ]
        
        for file_size, description in file_size_configs:
            assert file_size in self.schema_sql, f"Should configure {description} file size"
    
    def test_compression_optimization(self):
        """Test compression codec optimization"""
        # Should use ZSTD for better compression ratio
        assert "'zstd'" in self.schema_sql, "Should use ZSTD compression"
        
        # Should use Parquet format for analytical queries
        assert "'parquet'" in self.schema_sql, "Should use Parquet format"
    
    def test_partitioning_for_query_performance(self):
        """Test partitioning strategies optimize query performance"""
        # Main table partitioned by date and space_type for common query patterns
        main_partition = r"web_events.*PARTITIONED BY\s*\(\s*event_date\s*,\s*space_type\s*\)"
        assert re.search(main_partition, self.schema_sql, re.DOTALL), "Should partition for query performance"
        
        # Analytics tables partitioned by analysis/event date for time-based queries
        date_partitions = ["analysis_date", "event_date"]
        for partition_col in date_partitions:
            partition_pattern = f"PARTITIONED BY\\s*\\(\\s*{partition_col}\\s*\\)"
            assert re.search(partition_pattern, self.schema_sql), f"Should partition by {partition_col}"


class TestBusinessLogicConstraints:
    """Test business logic constraints and data validation"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_virtual_tour_specific_columns(self):
        """Test virtual tour business logic columns"""
        # Should have space-related columns for virtual tour analytics
        tour_columns = [
            'space_id STRING',
            'space_name STRING',
            'space_type STRING',
            'space_token STRING',
            'is_virtual_tour_page BOOLEAN',
            'has_space_context BOOLEAN'
        ]
        
        for column in tour_columns:
            assert column in self.schema_sql, f"Should define tour-specific column: {column}"
    
    def test_user_journey_analysis_columns(self):
        """Test user journey analysis specific columns"""
        # Should have journey-specific metrics
        journey_columns = [
            'journey_type STRING',
            'engagement_tier STRING',
            'conversion_intent_level STRING',
            'bounce_category STRING',
            'conversion_segment STRING'
        ]
        
        for column in journey_columns:
            assert column in self.schema_sql, f"Should define journey column: {column}"
    
    def test_data_quality_columns(self):
        """Test data quality and filtering columns"""
        # Should have data quality indicators
        quality_columns = [
            'data_quality_score DOUBLE',
            'is_bot BOOLEAN'
        ]
        
        for column in quality_columns:
            assert column in self.schema_sql, f"Should define quality column: {column}"
    
    def test_session_analysis_columns(self):
        """Test session-level analysis columns"""
        # Should support session boundary analysis
        session_columns = [
            'session_event_sequence INT',
            'is_first_event_in_session BOOLEAN',
            'time_on_page_seconds BIGINT'
        ]
        
        for column in session_columns:
            assert column in self.schema_sql, f"Should define session column: {column}"


class TestSQLSyntaxValidation:
    """Test SQL syntax validation and best practices"""
    
    def setup_method(self):
        """Setup test environment"""
        schema_file_path = Path("/Users/james/workspace-peek/load-webevents/sql/iceberg_schemas.sql")
        with open(schema_file_path, 'r') as f:
            self.schema_sql = f.read()
    
    def test_sql_syntax_correctness(self):
        """Test basic SQL syntax correctness"""
        # Check for balanced parentheses
        open_parens = self.schema_sql.count('(')
        close_parens = self.schema_sql.count(')')
        assert open_parens == close_parens, "Should have balanced parentheses"
        
        # Check for proper statement termination
        create_statements = self.schema_sql.count('CREATE TABLE')
        semicolons_after_create = 0
        
        # Count semicolons that likely terminate CREATE TABLE statements
        lines = self.schema_sql.split('\n')
        in_create_statement = False
        
        for line in lines:
            if 'CREATE TABLE' in line:
                in_create_statement = True
            elif in_create_statement and ';' in line:
                semicolons_after_create += 1
                in_create_statement = False
        
        assert semicolons_after_create >= create_statements, "Should terminate CREATE statements with semicolons"
    
    def test_consistent_naming_conventions(self):
        """Test consistent naming conventions"""
        # Table names should use snake_case
        table_names = re.findall(r'CREATE TABLE.*?\.(\w+)', self.schema_sql)
        
        for table_name in table_names:
            # Should be snake_case (lowercase with underscores)
            assert table_name.islower(), f"Table name should be lowercase: {table_name}"
            assert not re.search(r'[A-Z]', table_name), f"Table name should not have uppercase: {table_name}"
        
        # Column names should use snake_case
        column_patterns = re.findall(r'(\w+)\s+(?:STRING|INT|BIGINT|DOUBLE|BOOLEAN|TIMESTAMP|DATE|ARRAY)', self.schema_sql)
        
        for column_name in column_patterns:
            assert column_name.islower() or '_' in column_name, f"Column should use snake_case: {column_name}"
    
    def test_proper_comment_formatting(self):
        """Test proper comment formatting"""
        # Comments should be properly formatted
        comment_pattern = r"COMMENT\s+'[^']+'"
        comments = re.findall(comment_pattern, self.schema_sql)
        
        assert len(comments) >= 10, "Should have comments for key columns"
        
        # Comments should be descriptive (more than just column name)
        for comment in comments[:5]:  # Check first 5 comments
            comment_text = comment.split("'")[1]
            assert len(comment_text) > 10, f"Comment should be descriptive: {comment_text}"
    
    def test_proper_data_type_usage(self):
        """Test proper data type usage"""
        # Check for appropriate data type choices
        
        # ID fields should be STRING
        id_columns = re.findall(r'(\w*id\w*)\s+(\w+)', self.schema_sql, re.IGNORECASE)
        for col_name, col_type in id_columns:
            if 'id' in col_name.lower():
                assert col_type == 'STRING', f"ID column {col_name} should be STRING, not {col_type}"
        
        # Timestamp columns should be TIMESTAMP
        timestamp_columns = re.findall(r'(\w*timestamp\w*)\s+(\w+)', self.schema_sql, re.IGNORECASE)
        for col_name, col_type in timestamp_columns:
            if 'timestamp' in col_name.lower():
                assert col_type == 'TIMESTAMP', f"Timestamp column {col_name} should be TIMESTAMP, not {col_type}"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=15"])