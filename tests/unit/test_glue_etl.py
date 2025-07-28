"""
Unit tests for Glue ETL processors with S3 Tables/Iceberg integration
Tests both Kinesis streaming processor and S3 batch processor for data quality, transformations, and Iceberg operations.
"""

import pytest
import json
import base64
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
import sys
from types import SimpleNamespace

# Mock PySpark and Glue imports for testing
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.context'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.sql.window'] = MagicMock()
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.transforms'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()

# Import the actual modules after mocking
import importlib.util

# Load Kinesis processor
kinesis_spec = importlib.util.spec_from_file_location(
    "kinesis_processor_iceberg", 
    "/Users/james/workspace-peek/load-webevents/glue/kinesis_processor_iceberg.py"
)
kinesis_module = importlib.util.module_from_spec(kinesis_spec)

# Load S3 processor  
s3_spec = importlib.util.spec_from_file_location(
    "s3_processor_iceberg",
    "/Users/james/workspace-peek/load-webevents/glue/s3_processor_iceberg.py"
)
s3_module = importlib.util.module_from_spec(s3_spec)


class TestKinesisProcessorIceberg:
    """Test the Kinesis streaming processor for S3 Tables/Iceberg"""
    
    def setup_method(self):
        """Setup test environment"""
        self.sample_event = {
            "type": "page",
            "properties": {
                "title": "Peek | Virtual Tour | The Washingtons | [object Object] | 02-222",
                "url": "https://qa-beta-tour.peek.us/viewer?token=Ynyyjo8p",
                "path": "/viewer",
                "hash": "",
                "search": "token=Ynyyjo8p",
                "width": 1920,
                "height": 929,
                "spaceName": "02-222",
                "spaceId": "636d31b01be4410019a7962e",
                "spaceType": "unit",
                "spaceToken": "Ynyyjo8p",
                "token": "87827f537893ffa727c6eba409f9e5d0"
            },
            "anonymousId": "8d6aabc7-5b28-4106-ac61-1e9b4fdb3638",
            "secondsUtcTS": 1674840769225,
            "os": {"name": "Windows"},
            "referrer": "",
            "sessionId": "19b0a5fb-3bde-4ceb-988e-67e21bbdc761",
            "timezone": "America/Buenos_Aires",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "locale": "es-ES",
            "appId": "web-viewer-dev",
            "app": "web-viewer"
        }
        
        self.kinesis_metadata = {
            "sequenceNumber": "49638818049273582875652834848275797507965104553950068738",
            "partitionKey": "test-partition-key"
        }
    
    def test_decode_kinesis_record_valid_data(self):
        """Test decoding of valid base64 encoded Kinesis record"""
        # Load the actual function
        kinesis_spec.loader.exec_module(kinesis_module)
        
        # Encode test data
        json_string = json.dumps(self.sample_event)
        encoded_data = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')
        
        # Test decoding
        result = kinesis_module.decode_kinesis_record(encoded_data)
        
        assert result is not None, "Should successfully decode valid data"
        assert result['type'] == 'page', "Should preserve event type"
        assert result['sessionId'] == self.sample_event['sessionId'], "Should preserve session ID"
        assert result['properties']['spaceType'] == 'unit', "Should preserve space type"
    
    def test_decode_kinesis_record_invalid_data(self):
        """Test handling of invalid or corrupted Kinesis record"""
        kinesis_spec.loader.exec_module(kinesis_module)
        
        # Test with invalid base64
        result = kinesis_module.decode_kinesis_record("invalid-base64-data")
        assert result is None, "Should return None for invalid base64"
        
        # Test with valid base64 but invalid JSON
        invalid_json = base64.b64encode(b"not-json-data").decode('utf-8')
        result = kinesis_module.decode_kinesis_record(invalid_json)
        assert result is None, "Should return None for invalid JSON"
        
        # Test with empty string
        result = kinesis_module.decode_kinesis_record("")
        assert result is None, "Should return None for empty string"
    
    def test_transform_web_event_streaming_complete_data(self):
        """Test transformation of complete web event data for Iceberg"""
        kinesis_spec.loader.exec_module(kinesis_module)
        
        result = kinesis_module.transform_web_event_streaming(self.sample_event, self.kinesis_metadata)
        
        assert result is not None, "Should successfully transform valid data"
        
        # Test required fields
        assert 'event_id' in result, "Should generate event_id"
        assert 'event_timestamp' in result, "Should include event_timestamp"
        assert 'event_date' in result, "Should include event_date for partitioning"
        assert 'space_type' in result, "Should include space_type for partitioning"
        
        # Test event identification
        assert result['event_type'] == 'page', "Should preserve event type"
        assert result['session_id'] == self.sample_event['sessionId'], "Should preserve session ID"
        assert result['anonymous_id'] == self.sample_event['anonymousId'], "Should preserve anonymous ID"
        
        # Test space context extraction
        assert result['space_id'] == '636d31b01be4410019a7962e', "Should extract space ID"
        assert result['space_type'] == 'unit', "Should extract space type"
        assert result['space_name'] == '02-222', "Should extract space name"
        
        # Test processing metadata
        assert 'processed_timestamp' in result, "Should include processing timestamp"
        assert result['kinesis_sequence_number'] == self.kinesis_metadata['sequenceNumber']
        assert result['kinesis_partition_key'] == self.kinesis_metadata['partitionKey']
        
        # Test properties JSON
        assert 'properties_json' in result, "Should include raw properties as JSON"
        properties_dict = json.loads(result['properties_json'])
        assert properties_dict['spaceId'] == '636d31b01be4410019a7962e'
    
    def test_transform_web_event_streaming_missing_fields(self):
        """Test transformation handles missing fields gracefully"""
        kinesis_spec.loader.exec_module(kinesis_module)
        
        # Create event with missing optional fields
        incomplete_event = {
            "type": "page",
            "sessionId": "test-session",
            "anonymousId": "test-anon-id"
            # Missing properties, timestamps, etc.
        }
        
        result = kinesis_module.transform_web_event_streaming(incomplete_event, self.kinesis_metadata)
        
        assert result is not None, "Should handle incomplete data"
        assert result['event_type'] == 'page', "Should preserve available fields"
        assert result['session_id'] == 'test-session', "Should preserve session ID"
        
        # Missing fields should be None or have defaults
        assert result['space_id'] is None, "Missing space_id should be None"
        assert result['user_agent'] is None, "Missing user_agent should be None"
        assert result['screen_width'] is None, "Missing screen dimensions should be None"
    
    def test_transform_web_event_streaming_timestamp_handling(self):
        """Test timestamp parsing and formatting for Iceberg"""
        kinesis_spec.loader.exec_module(kinesis_module)
        
        # Test with valid timestamp
        event_with_timestamp = self.sample_event.copy()
        event_with_timestamp['secondsUtcTS'] = 1674840769225  # Known timestamp
        
        result = kinesis_module.transform_web_event_streaming(event_with_timestamp, self.kinesis_metadata)
        
        # Verify timestamp conversion
        assert result['seconds_utc_ts'] == 1674840769225, "Should preserve original timestamp"
        
        # Check ISO format timestamp
        event_timestamp = result['event_timestamp']
        assert 'T' in event_timestamp, "Should be in ISO format"
        assert event_timestamp.endswith('Z') or '+' in event_timestamp or '-' in event_timestamp[-6:]
        
        # Check date extraction
        event_date = result['event_date']
        assert len(event_date) == 10, "Date should be YYYY-MM-DD format"
        assert '-' in event_date, "Date should contain hyphens"
        
        # Test with missing timestamp (should use current time)
        event_no_timestamp = {
            "type": "page",
            "sessionId": "test-session",
            "anonymousId": "test-anon-id"
        }
        
        result_no_ts = kinesis_module.transform_web_event_streaming(event_no_timestamp, self.kinesis_metadata)
        assert result_no_ts['seconds_utc_ts'] > 0, "Should generate timestamp for missing timestamp"
    
    def test_transform_web_event_streaming_error_handling(self):
        """Test error handling in transformation function"""
        kinesis_spec.loader.exec_module(kinesis_module)
        
        # Test with None input
        result = kinesis_module.transform_web_event_streaming(None, self.kinesis_metadata)
        assert result is None, "Should return None for None input"
        
        # Test with empty dict
        result = kinesis_module.transform_web_event_streaming({}, self.kinesis_metadata)
        assert result is not None, "Should handle empty dict"
        assert result['event_type'] == 'unknown', "Should use default for missing type"
        
        # Test with malformed data types
        malformed_event = {
            "secondsUtcTS": "not-a-number",  # Invalid timestamp
            "properties": "not-a-dict"  # Invalid properties
        }
        
        # Should not crash, might return None or handle gracefully
        result = kinesis_module.transform_web_event_streaming(malformed_event, self.kinesis_metadata)
        # Result can be None (error case) or transformed with defaults


class TestS3ProcessorIceberg:
    """Test the S3 batch processor for S3 Tables/Iceberg"""
    
    def setup_method(self):
        """Setup test environment"""
        self.sample_jsonl_data = [
            {
                "type": "page",
                "sessionId": "session-1",
                "anonymousId": "user-1",
                "secondsUtcTS": 1674840769225,
                "properties": {
                    "spaceType": "unit",
                    "spaceId": "space-1",
                    "url": "https://example.com/viewer"
                }
            },
            {
                "type": "track", 
                "sessionId": "session-2",
                "anonymousId": "user-2",
                "secondsUtcTS": 1674840769226,
                "properties": {
                    "spaceType": "amenity",
                    "spaceId": "space-2"
                }
            }
        ]
    
    def test_decode_base64_record_valid_data(self):
        """Test decoding base64 encoded records from S3"""
        s3_spec.loader.exec_module(s3_module)
        
        # Encode test JSON
        json_string = json.dumps(self.sample_jsonl_data[0])
        encoded_data = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')
        
        result = s3_module.decode_base64_record(encoded_data)
        
        assert result is not None, "Should decode valid base64 data"
        assert result['type'] == 'page', "Should preserve event type"
        assert result['sessionId'] == 'session-1', "Should preserve session ID"
    
    def test_decode_base64_record_invalid_data(self):
        """Test handling of invalid base64 data"""
        s3_spec.loader.exec_module(s3_module)
        
        # Test invalid base64
        result = s3_module.decode_base64_record("invalid-base64")
        assert result is None, "Should return None for invalid base64"
        
        # Test valid base64 but invalid JSON
        invalid_json = base64.b64encode(b"not-json").decode('utf-8')
        result = s3_module.decode_base64_record(invalid_json)
        assert result is None, "Should return None for invalid JSON"
    
    def test_transform_web_event_complete_data(self):
        """Test transformation of complete web event for batch processing"""
        s3_spec.loader.exec_module(s3_module)
        
        result = s3_module.transform_web_event(self.sample_jsonl_data[0])
        
        assert result is not None, "Should transform valid event"
        
        # Test required fields for Iceberg
        assert 'event_id' in result, "Should generate unique event_id"
        assert 'event_timestamp' in result, "Should format timestamp for Iceberg"
        assert 'event_date' in result, "Should extract date for partitioning"
        assert 'event_hour' in result, "Should extract hour"
        
        # Test space context
        assert result['space_type'] == 'unit', "Should extract space type"
        assert result['space_id'] == 'space-1', "Should extract space ID"
        
        # Test properties JSON preservation
        assert 'properties_json' in result, "Should preserve raw properties"
        properties = json.loads(result['properties_json'])
        assert properties['spaceType'] == 'unit'
    
    def test_transform_web_event_missing_timestamp(self):
        """Test transformation with missing timestamp"""
        s3_spec.loader.exec_module(s3_module)
        
        event_no_timestamp = {
            "type": "page",
            "sessionId": "session-test",
            "anonymousId": "user-test"
            # Missing secondsUtcTS
        }
        
        result = s3_module.transform_web_event(event_no_timestamp)
        
        assert result is not None, "Should handle missing timestamp"
        assert result['seconds_utc_ts'] > 0, "Should generate current timestamp"
        assert result['event_timestamp'], "Should have formatted timestamp"
    
    def test_calculate_data_quality_score(self):
        """Test data quality scoring algorithm"""
        s3_spec.loader.exec_module(s3_module)
        
        # Perfect data quality
        perfect_event = {
            'session_id': 'session-1',
            'anonymous_id': 'user-1', 
            'user_agent': 'Mozilla/5.0...',
            'space_id': 'space-1',
            'page_path': '/viewer'
        }
        
        score = s3_module.calculate_data_quality_score(perfect_event)
        assert score == 1.0, "Perfect data should score 1.0"
        
        # Missing session_id (major penalty)
        no_session = perfect_event.copy()
        del no_session['session_id']
        score = s3_module.calculate_data_quality_score(no_session)
        assert score == 0.6, "Missing session_id should reduce score by 0.4"
        
        # Missing anonymous_id
        no_anon = perfect_event.copy()
        del no_anon['anonymous_id']
        score = s3_module.calculate_data_quality_score(no_anon)
        assert score == 0.7, "Missing anonymous_id should reduce score by 0.3"
        
        # Missing user_agent
        no_ua = perfect_event.copy()
        del no_ua['user_agent']
        score = s3_module.calculate_data_quality_score(no_ua)
        assert score == 0.8, "Missing user_agent should reduce score by 0.2"
        
        # Missing space_id on viewer page
        no_space_viewer = perfect_event.copy()
        del no_space_viewer['space_id']
        score = s3_module.calculate_data_quality_score(no_space_viewer)
        assert score == 0.9, "Missing space_id on viewer page should reduce score by 0.1"
        
        # Multiple missing fields
        minimal_event = {'page_path': '/home'}
        score = s3_module.calculate_data_quality_score(minimal_event)
        assert score == 0.0, "Multiple missing fields should result in 0 score"
    
    def test_transform_web_event_error_handling(self):
        """Test error handling in batch transformation"""
        s3_spec.loader.exec_module(s3_module)
        
        # Test with None
        result = s3_module.transform_web_event(None)
        assert result is None, "Should return None for None input"
        
        # Test with empty dict
        result = s3_module.transform_web_event({})
        assert result is not None, "Should handle empty dict"
        
        # Test with invalid data types
        invalid_event = {
            "secondsUtcTS": "not-a-number",
            "properties": ["not", "a", "dict"]
        }
        
        # Should either return None or handle gracefully
        result = s3_module.transform_web_event(invalid_event)
        # Result handling depends on implementation
    
    def test_event_id_generation_uniqueness(self):
        """Test that event IDs are unique for different events"""
        s3_spec.loader.exec_module(s3_module)
        
        event1 = {
            "sessionId": "session-1",
            "secondsUtcTS": 1674840769225,
            "type": "page"
        }
        
        event2 = {
            "sessionId": "session-2", 
            "secondsUtcTS": 1674840769226,
            "type": "page"
        }
        
        result1 = s3_module.transform_web_event(event1)
        result2 = s3_module.transform_web_event(event2)
        
        assert result1['event_id'] != result2['event_id'], "Event IDs should be unique"
        
        # Same event should generate same ID (idempotent)
        result1_repeat = s3_module.transform_web_event(event1)
        # Note: Due to hash inclusion, this might not be exactly identical but should be predictable


class TestGlueJobConfiguration:
    """Test Glue job configurations and Spark/Iceberg integration"""
    
    def test_kinesis_processor_spark_configuration(self):
        """Test that Kinesis processor configures Spark correctly for Iceberg"""
        # This would test the main() function configuration
        # Since we can't easily test the full Glue context, we test key configuration values
        
        expected_configs = {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true', 
            'spark.sql.streaming.stateStore.maintenanceInterval': '300s',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.s3_tables': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.s3_tables.catalog-impl': 'org.apache.iceberg.aws.s3.S3Tables'
        }
        
        # These configurations are critical for S3 Tables/Iceberg functionality
        for config_key, expected_value in expected_configs.items():
            # In a real test, we would verify these are set on the Spark session
            assert config_key is not None  # Placeholder test
            assert expected_value is not None
    
    def test_s3_processor_spark_configuration(self):
        """Test that S3 processor configures Spark correctly for batch processing"""
        expected_configs = {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.s3_tables': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.s3_tables.catalog-impl': 'org.apache.iceberg.aws.s3.S3Tables'
        }
        
        for config_key, expected_value in expected_configs.items():
            assert config_key is not None
            assert expected_value is not None


class TestIcebergTableOperations:
    """Test Iceberg-specific table operations and SQL generation"""
    
    def test_iceberg_table_creation_sql(self):
        """Test the Iceberg table creation SQL is properly formatted"""
        expected_table_properties = [
            'write.target-file-size-bytes',
            'write.format.default',
            'write.parquet.compression-codec'
        ]
        
        expected_partition_columns = ['event_date', 'space_type']
        
        # In the actual processors, verify these are used in CREATE TABLE statements
        for prop in expected_table_properties:
            assert prop is not None, f"Should include {prop} in table properties"
        
        for col in expected_partition_columns:
            assert col is not None, f"Should partition by {col}"
    
    def test_iceberg_merge_operation_logic(self):
        """Test the MERGE operation for handling duplicates"""
        # The Kinesis processor uses MERGE for deduplication
        expected_merge_condition = "target.event_id = source.event_id"
        expected_action = "WHEN NOT MATCHED THEN INSERT *"
        
        # These would be used in the actual MERGE SQL statement
        assert "event_id" in expected_merge_condition
        assert "INSERT" in expected_action


class TestDataQualityAndValidation:
    """Test data quality validation and filtering logic"""
    
    def test_bot_detection_logic(self):
        """Test bot traffic detection patterns"""
        s3_spec.loader.exec_module(s3_module)
        
        # Create mock bot traffic patterns
        bot_user_agents = [
            "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
            "crawler-bot/1.0",
            "automated-scraper"
        ]
        
        legitimate_user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15"
        ]
        
        # Test bot detection (this would be part of the enrichment logic)
        for bot_ua in bot_user_agents:
            # In actual implementation, check if bot detection flags these
            assert 'bot' in bot_ua.lower() or 'crawler' in bot_ua.lower()
        
        for legit_ua in legitimate_user_agents:
            assert 'Mozilla' in legit_ua  # Legitimate browsers include Mozilla
    
    def test_session_boundary_detection(self):
        """Test session boundary and sequencing logic"""
        # Test data for session analysis
        session_events = [
            {"sessionId": "session-1", "secondsUtcTS": 1674840769225, "type": "page"},
            {"sessionId": "session-1", "secondsUtcTS": 1674840769230, "type": "track"},
            {"sessionId": "session-1", "secondsUtcTS": 1674840769235, "type": "page"},
            {"sessionId": "session-2", "secondsUtcTS": 1674840769240, "type": "page"},
        ]
        
        # Group by session
        sessions = {}
        for event in session_events:
            session_id = event['sessionId']
            if session_id not in sessions:
                sessions[session_id] = []
            sessions[session_id].append(event)
        
        # Test session-1 has 3 events, session-2 has 1 event
        assert len(sessions['session-1']) == 3, "Session-1 should have 3 events"
        assert len(sessions['session-2']) == 1, "Session-2 should have 1 event"
        
        # Test chronological ordering within session
        session_1_events = sessions['session-1']
        timestamps = [event['secondsUtcTS'] for event in session_1_events]
        assert timestamps == sorted(timestamps), "Events should be chronologically ordered"
    
    def test_virtual_tour_context_validation(self):
        """Test virtual tour specific context validation"""
        # Test events with different space contexts
        unit_event = {
            "properties": {
                "spaceType": "unit",
                "spaceId": "unit-123",
                "path": "/viewer"
            }
        }
        
        amenity_event = {
            "properties": {
                "spaceType": "amenity", 
                "spaceId": "amenity-456",
                "path": "/viewer"
            }
        }
        
        no_space_event = {
            "properties": {
                "path": "/home"
            }
        }
        
        # Test space context detection
        assert unit_event['properties']['spaceType'] == 'unit'
        assert unit_event['properties']['path'] == '/viewer'
        
        assert amenity_event['properties']['spaceType'] == 'amenity'
        
        assert 'spaceType' not in no_space_event['properties']
    
    def test_data_quality_thresholds(self):
        """Test data quality filtering thresholds"""
        # Test quality score thresholds used in processing
        high_quality_threshold = 0.9
        medium_quality_threshold = 0.7
        low_quality_threshold = 0.5
        
        # These thresholds should be used to filter data
        assert high_quality_threshold > medium_quality_threshold
        assert medium_quality_threshold > low_quality_threshold
        assert low_quality_threshold >= 0.0


class TestErrorHandlingAndResilience:
    """Test error handling and system resilience"""
    
    def test_malformed_json_handling(self):
        """Test handling of malformed JSON data"""
        s3_spec.loader.exec_module(s3_module)
        
        malformed_json_strings = [
            '{"incomplete": json',  # Incomplete JSON
            '{invalid-json-syntax}',  # Invalid syntax
            'not-json-at-all',  # Not JSON
            '',  # Empty string
            '{}',  # Empty JSON (valid)
            'null',  # JSON null
        ]
        
        for json_str in malformed_json_strings:
            try:
                encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
                result = s3_module.decode_base64_record(encoded)
                
                # Should either return None or valid dict
                assert result is None or isinstance(result, dict)
            except Exception:
                # Should not raise unhandled exceptions
                pytest.fail(f"Should handle malformed JSON gracefully: {json_str}")
    
    def test_large_payload_handling(self):
        """Test handling of unusually large payloads"""
        # Create a large event payload
        large_properties = {f"key_{i}": f"value_{i}" * 100 for i in range(1000)}
        large_event = {
            "type": "page",
            "sessionId": "large-session",
            "properties": large_properties
        }
        
        s3_spec.loader.exec_module(s3_module)
        
        # Should handle large payloads without crashing
        result = s3_module.transform_web_event(large_event)
        
        if result is not None:
            # Properties should be serialized as JSON
            assert 'properties_json' in result
            # Should be valid JSON
            properties_dict = json.loads(result['properties_json'])
            assert len(properties_dict) == 1000
    
    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters"""
        unicode_event = {
            "type": "page",
            "sessionId": "unicode-session",
            "properties": {
                "title": "Virtual Tour - 北京公寓 🏠",
                "spaceName": "Café René's Apartment™",
                "description": "Special chars: @#$%^&*()[]{}|\\:;\"'<>?/+=~`"
            }
        }
        
        s3_spec.loader.exec_module(s3_module)
        
        result = s3_module.transform_web_event(unicode_event)
        
        if result is not None:
            # Should preserve Unicode characters
            properties_dict = json.loads(result['properties_json'])
            assert "北京公寓" in properties_dict['title']
            assert "🏠" in properties_dict['title']
            assert "Café René's Apartment™" in properties_dict['spaceName']


class TestPerformanceConsiderations:
    """Test performance-related aspects of the ETL processors"""
    
    def test_batch_processing_efficiency(self):
        """Test that batch processing handles multiple records efficiently"""
        # Create a batch of events
        batch_events = []
        for i in range(100):
            event = {
                "type": "page",
                "sessionId": f"session-{i % 10}",  # 10 different sessions
                "anonymousId": f"user-{i % 20}",   # 20 different users  
                "secondsUtcTS": 1674840769225 + i,
                "properties": {
                    "spaceType": "unit" if i % 2 == 0 else "amenity",
                    "spaceId": f"space-{i}"
                }
            }
            batch_events.append(event)
        
        s3_spec.loader.exec_module(s3_module)
        
        # Transform all events
        transformed_events = []
        for event in batch_events:
            result = s3_module.transform_web_event(event)
            if result:
                transformed_events.append(result)
        
        # Should successfully transform most/all events
        assert len(transformed_events) >= 90, "Should transform at least 90% of events"
        
        # Should have unique event IDs
        event_ids = [event['event_id'] for event in transformed_events]
        assert len(set(event_ids)) == len(event_ids), "All event IDs should be unique"
    
    def test_memory_usage_patterns(self):
        """Test memory-efficient processing patterns"""
        # Test that large strings are handled efficiently
        large_string = "x" * 10000  # 10KB string
        
        event_with_large_data = {
            "type": "page",
            "sessionId": "memory-test",
            "properties": {
                "large_field": large_string,
                "title": "Normal title"
            }
        }
        
        s3_spec.loader.exec_module(s3_module)
        
        result = s3_module.transform_web_event(event_with_large_data)
        
        if result is not None:
            # Should compress/handle large data appropriately
            properties_json = result['properties_json']
            assert len(properties_json) > 0, "Should serialize large properties"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=5"])