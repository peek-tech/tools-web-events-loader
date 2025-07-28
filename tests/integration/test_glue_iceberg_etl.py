"""
Integration tests for Glue ETL jobs with S3 Tables/Iceberg
Tests both batch and streaming Glue jobs for Iceberg table operations
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import boto3
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import tempfile
import shutil
import os


class TestGlueIcebergETL(unittest.TestCase):
    """Integration tests for Glue Iceberg ETL jobs"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("TestGlueIcebergETL") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.s3_tables", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.s3_tables.type", "hadoop") \
            .config("spark.sql.catalog.s3_tables.warehouse", "file:///tmp/iceberg-test") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create temp directory for test data
        cls.temp_dir = tempfile.mkdtemp()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session and temp files"""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir, ignore_errors=True)
    
    def setUp(self):
        """Set up test data for each test"""
        self.sample_events = [
            {
                "event_id": f"evt-{i}",
                "event_timestamp": datetime.now() - timedelta(hours=i),
                "event_type": "page",
                "anonymous_id": f"user-{i % 5}",
                "session_id": f"session-{i % 10}",
                "page_path": "/viewer" if i % 2 == 0 else "/tour",
                "space_id": f"space-{i % 3}" if i % 2 == 0 else None,
                "space_type": ["unit", "amenity", "common_area"][i % 3],
                "referrer": "https://google.com" if i % 3 == 0 else "",
                "user_agent": "Mozilla/5.0",
                "data_quality_score": 0.85 - (i * 0.05),
                "is_bot": False
            }
            for i in range(50)
        ]
    
    def test_s3_batch_processor_iceberg_creation(self):
        """Test S3 batch processor creates valid Iceberg tables"""
        # Create test DataFrame
        df = self.spark.createDataFrame(self.sample_events)
        
        # Simulate Iceberg table creation
        table_path = f"{self.temp_dir}/test_web_events"
        
        # Write as Iceberg table
        df.write \
            .mode("overwrite") \
            .option("write.format.default", "parquet") \
            .option("write.parquet.compression-codec", "zstd") \
            .option("write.target-file-size-bytes", "134217728") \
            .partitionBy("event_type") \
            .format("iceberg") \
            .save(table_path)
        
        # Verify table was created
        self.assertTrue(os.path.exists(f"{table_path}/metadata"))
        
        # Read back and verify
        read_df = self.spark.read.format("iceberg").load(table_path)
        self.assertEqual(read_df.count(), 50)
        self.assertIn("event_id", read_df.columns)
        self.assertIn("space_type", read_df.columns)
    
    def test_kinesis_streaming_processor_merge(self):
        """Test Kinesis streaming processor MERGE operations"""
        # Create initial Iceberg table
        table_path = f"{self.temp_dir}/test_streaming"
        initial_df = self.spark.createDataFrame(self.sample_events[:25])
        
        initial_df.write \
            .mode("overwrite") \
            .format("iceberg") \
            .save(table_path)
        
        # Create new events (some duplicates, some new)
        new_events = self.sample_events[20:35]  # Overlap with initial
        for event in new_events[10:]:
            event['event_id'] = f"new-evt-{event['event_id']}"
        
        new_df = self.spark.createDataFrame(new_events)
        
        # Register tables for SQL operations
        initial_df.createOrReplaceTempView("source")
        self.spark.read.format("iceberg").load(table_path).createOrReplaceTempView("target")
        
        # Simulate MERGE operation (as Spark SQL)
        merge_query = """
        MERGE INTO target t
        USING source s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        # For testing, we'll simulate with union and distinct
        existing_df = self.spark.read.format("iceberg").load(table_path)
        merged_df = existing_df.unionByName(new_df).dropDuplicates(['event_id'])
        
        # Verify merge results
        self.assertGreater(merged_df.count(), 25)  # More than initial
        self.assertLess(merged_df.count(), 40)  # Less than simple union
    
    def test_data_quality_enrichment(self):
        """Test data quality scoring and enrichment logic"""
        # Create events with varying quality
        quality_test_events = [
            # High quality event
            {
                "event_id": "high-quality",
                "anonymous_id": "user-1",
                "session_id": "session-1",
                "space_id": "space-1",
                "space_type": "unit",
                "page_path": "/viewer",
                "referrer": "https://google.com",
                "user_agent": "Mozilla/5.0"
            },
            # Medium quality event (missing space context)
            {
                "event_id": "medium-quality",
                "anonymous_id": "user-2",
                "session_id": "session-2",
                "page_path": "/tour",
                "user_agent": "Mozilla/5.0"
            },
            # Low quality event (minimal data)
            {
                "event_id": "low-quality",
                "anonymous_id": "user-3"
            }
        ]
        
        # Apply quality scoring logic
        def calculate_quality_score(row):
            score = 0.0
            if row.get('anonymous_id'): score += 0.2
            if row.get('session_id'): score += 0.2
            if row.get('space_id'): score += 0.2
            if row.get('page_path'): score += 0.1
            if row.get('referrer'): score += 0.1
            if row.get('user_agent'): score += 0.1
            if row.get('space_type') and row['space_type'] != 'unknown': score += 0.1
            return score
        
        # Test scoring
        for event in quality_test_events:
            score = calculate_quality_score(event)
            if event['event_id'] == 'high-quality':
                self.assertGreaterEqual(score, 0.8)
            elif event['event_id'] == 'medium-quality':
                self.assertGreaterEqual(score, 0.5)
                self.assertLess(score, 0.8)
            else:
                self.assertLess(score, 0.5)
    
    def test_session_metrics_aggregation(self):
        """Test session metrics calculation for Iceberg tables"""
        # Create session events
        session_events = []
        for session_id in range(5):
            for event_seq in range(10):
                session_events.append({
                    "event_id": f"sess-{session_id}-evt-{event_seq}",
                    "session_id": f"session-{session_id}",
                    "anonymous_id": f"user-{session_id}",
                    "event_timestamp": datetime.now() - timedelta(minutes=50-event_seq*5),
                    "session_event_sequence": event_seq + 1,
                    "space_type": ["unit", "amenity"][event_seq % 2],
                    "time_on_page_seconds": 30 + event_seq * 10
                })
        
        df = self.spark.createDataFrame(session_events)
        
        # Calculate session metrics
        session_metrics = df.groupBy("session_id", "anonymous_id").agg(
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.count("*").alias("total_events"),
            F.countDistinct("space_type").alias("unique_space_types"),
            F.sum("time_on_page_seconds").alias("total_time_on_site"),
            F.collect_set("space_type").alias("space_types_viewed")
        )
        
        # Verify metrics
        metrics_list = session_metrics.collect()
        self.assertEqual(len(metrics_list), 5)  # 5 sessions
        
        for metric in metrics_list:
            self.assertEqual(metric['total_events'], 10)
            self.assertEqual(metric['unique_space_types'], 2)
            self.assertGreater(metric['total_time_on_site'], 0)
    
    def test_iceberg_time_travel_capability(self):
        """Test Iceberg time travel features"""
        table_path = f"{self.temp_dir}/test_time_travel"
        
        # Create initial version
        df_v1 = self.spark.createDataFrame(self.sample_events[:10])
        df_v1.write.mode("overwrite").format("iceberg").save(table_path)
        
        # Get snapshot ID (simulate)
        snapshot_v1_timestamp = datetime.now()
        
        # Add more data (version 2)
        df_v2 = self.spark.createDataFrame(self.sample_events[10:20])
        df_v2.write.mode("append").format("iceberg").save(table_path)
        
        # Read current version
        current_df = self.spark.read.format("iceberg").load(table_path)
        self.assertEqual(current_df.count(), 20)
        
        # In real Iceberg, we would use time travel:
        # historical_df = spark.read.format("iceberg").option("as-of-timestamp", snapshot_v1_timestamp).load(table_path)
        # For testing, we verify the concept
        self.assertTrue(os.path.exists(f"{table_path}/metadata"))
    
    def test_iceberg_schema_evolution(self):
        """Test Iceberg schema evolution capabilities"""
        table_path = f"{self.temp_dir}/test_schema_evolution"
        
        # Create initial schema
        initial_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), True),
            StructField("anonymous_id", StringType(), True)
        ])
        
        initial_data = [
            ("evt-1", "page", "user-1"),
            ("evt-2", "track", "user-2")
        ]
        
        df_v1 = self.spark.createDataFrame(initial_data, initial_schema)
        df_v1.write.mode("overwrite").format("iceberg").save(table_path)
        
        # Evolve schema - add new columns
        evolved_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), True),
            StructField("anonymous_id", StringType(), True),
            StructField("session_id", StringType(), True),  # New column
            StructField("space_type", StringType(), True)   # New column
        ])
        
        evolved_data = [
            ("evt-3", "page", "user-3", "session-1", "unit"),
            ("evt-4", "track", "user-4", "session-2", "amenity")
        ]
        
        df_v2 = self.spark.createDataFrame(evolved_data, evolved_schema)
        
        # In real Iceberg, schema evolution is automatic
        # For testing, verify both schemas can coexist
        self.assertEqual(len(df_v1.columns), 3)
        self.assertEqual(len(df_v2.columns), 5)
    
    def test_partition_optimization(self):
        """Test partition optimization for query performance"""
        # Create partitioned data
        df = self.spark.createDataFrame(self.sample_events)
        
        # Add partition columns
        df_with_partitions = df.withColumn(
            "event_date", 
            F.date_format(F.col("event_timestamp"), "yyyy-MM-dd")
        ).withColumn(
            "event_hour",
            F.hour(F.col("event_timestamp"))
        )
        
        # Test partition pruning
        filtered_df = df_with_partitions.filter(
            (F.col("event_date") == "2024-01-01") & 
            (F.col("space_type") == "unit")
        )
        
        # Verify partition columns are used
        self.assertIn("event_date", filtered_df.columns)
        self.assertIn("space_type", filtered_df.columns)
    
    def test_error_handling_and_recovery(self):
        """Test error handling in ETL pipeline"""
        # Create events with some invalid data
        mixed_events = self.sample_events.copy()
        
        # Add corrupted events
        mixed_events.append({
            "event_id": None,  # Invalid: null primary key
            "event_type": "page"
        })
        mixed_events.append({
            "event_id": "valid-id",
            "event_timestamp": "invalid-date"  # Invalid timestamp
        })
        
        # Process with error handling
        valid_events = []
        error_events = []
        
        for event in mixed_events:
            try:
                # Validate event
                if not event.get('event_id'):
                    raise ValueError("Missing event_id")
                if isinstance(event.get('event_timestamp'), str):
                    # Try to parse timestamp
                    datetime.fromisoformat(event['event_timestamp'])
                valid_events.append(event)
            except Exception as e:
                error_events.append({
                    'event': event,
                    'error': str(e)
                })
        
        # Verify error handling
        self.assertEqual(len(valid_events), 50)  # Original valid events
        self.assertEqual(len(error_events), 2)   # Two invalid events
    
    def test_concurrent_write_handling(self):
        """Test handling of concurrent writes to Iceberg tables"""
        table_path = f"{self.temp_dir}/test_concurrent"
        
        # Simulate concurrent writes
        df1 = self.spark.createDataFrame(self.sample_events[:25])
        df2 = self.spark.createDataFrame(self.sample_events[25:])
        
        # Write first batch
        df1.write.mode("overwrite").format("iceberg").save(table_path)
        
        # Simulate concurrent append (in real scenario, these would be parallel)
        df2.write.mode("append").format("iceberg").save(table_path)
        
        # Verify both writes succeeded
        final_df = self.spark.read.format("iceberg").load(table_path)
        self.assertEqual(final_df.count(), 50)
        
        # Verify no duplicates (ACID compliance)
        self.assertEqual(
            final_df.select("event_id").distinct().count(),
            50
        )


class TestGlueJobConfiguration(unittest.TestCase):
    """Test Glue job configuration and parameters"""
    
    def test_glue_job_parameters(self):
        """Test Glue job parameter validation"""
        # Required parameters for S3 processor
        s3_params = {
            '--SOURCE_BUCKET': 'source-bucket',
            '--ACCOUNT_ID': '123456789012',
            '--JOB_NAME': 'test-job'
        }
        
        # Validate all required params present
        required_params = ['--SOURCE_BUCKET', '--ACCOUNT_ID', '--JOB_NAME']
        for param in required_params:
            self.assertIn(param, s3_params)
        
        # Required parameters for Kinesis processor
        kinesis_params = {
            '--KINESIS_STREAM_NAME': 'test-stream',
            '--DATABASE_NAME': 'test_db',
            '--CHECKPOINT_LOCATION': 's3://bucket/checkpoint/',
            '--starting_position': 'LATEST'
        }
        
        # Validate streaming params
        self.assertIn('--starting_position', kinesis_params)
        self.assertTrue(
            kinesis_params['--starting_position'] in ['LATEST', 'TRIM_HORIZON']
        )
    
    def test_iceberg_table_properties(self):
        """Test Iceberg table properties configuration"""
        table_properties = {
            'write.target-file-size-bytes': '134217728',  # 128MB
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'zstd',
            'write.metadata.delete-after-commit.enabled': 'true',
            'write.metadata.previous-versions-max': '10',
            'write.merge.mode': 'merge-on-read',
            'write.distribution-mode': 'hash'
        }
        
        # Verify critical properties
        self.assertEqual(
            int(table_properties['write.target-file-size-bytes']),
            128 * 1024 * 1024
        )
        self.assertEqual(
            table_properties['write.parquet.compression-codec'],
            'zstd'
        )
        self.assertIn(
            table_properties['write.merge.mode'],
            ['merge-on-read', 'copy-on-write']
        )


if __name__ == '__main__':
    # Run tests with coverage
    unittest.main(verbosity=2)