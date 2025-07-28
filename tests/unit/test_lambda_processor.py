"""
Unit tests for Lambda Kinesis processor with S3 Tables integration
Tests real-time event processing, S3 Tables staging, data enrichment, and error handling.
"""

import pytest
import json
import base64
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import os
import boto3
from moto import mock_s3, mock_lambda, mock_glue
import importlib.util

# Load the Lambda function module
lambda_spec = importlib.util.spec_from_file_location(
    "lambda_processor", 
    "/Users/james/workspace-peek/load-webevents/lambda/kinesis-processor/index_iceberg.py"
)
lambda_module = importlib.util.module_from_spec(lambda_spec)
lambda_spec.loader.exec_module(lambda_module)


class TestLambdaKinesisProcessor:
    """Test the main Lambda handler function"""
    
    def setup_method(self):
        """Setup test environment"""
        # Sample Kinesis event structure
        self.sample_kinesis_event = {
            "Records": [
                {
                    "eventID": "shardId-000000000000:49638818049273582875652834848275797507965104553950068738",
                    "eventName": "aws:kinesis:record",
                    "eventVersion": "1.0",
                    "eventSource": "aws:kinesis",
                    "awsRegion": "us-east-1",
                    "kinesis": {
                        "kinesisSchemaVersion": "1.0",
                        "partitionKey": "user-123",
                        "sequenceNumber": "49638818049273582875652834848275797507965104553950068738",
                        "data": self._encode_sample_event(),
                        "approximateArrivalTimestamp": 1674840769.225
                    }
                }
            ]
        }
        
        # Sample web event
        self.sample_web_event = {
            "type": "page",
            "properties": {
                "title": "Peek | Virtual Tour | Test Space",
                "url": "https://qa-beta-tour.peek.us/viewer?token=testtoken",
                "path": "/viewer",
                "width": 1920,
                "height": 1080,
                "spaceName": "Test Space",
                "spaceId": "test-space-id-123",
                "spaceType": "unit",
                "spaceToken": "testtoken"
            },
            "anonymousId": "test-anonymous-id",
            "secondsUtcTS": 1674840769225,
            "os": {"name": "Windows"},
            "referrer": "https://example.com",
            "sessionId": "test-session-id",
            "timezone": "America/New_York",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "locale": "en-US",
            "appId": "web-viewer-prod",
            "app": "web-viewer"
        }
        
        # Mock environment variables
        os.environ['BUCKET_NAME'] = 'test-bucket'
        os.environ['GLUE_DATABASE'] = 'test_database'
        os.environ['TABLE_BUCKET_ARN'] = 'arn:aws:s3tables:us-east-1:123456789012:bucket/test-table-bucket'
        os.environ['WEB_EVENTS_TABLE'] = 'arn:aws:s3tables:us-east-1:123456789012:bucket/test-table-bucket/namespaces/analytics/tables/web_events'
        os.environ['SESSION_METRICS_TABLE'] = 'arn:aws:s3tables:us-east-1:123456789012:bucket/test-table-bucket/namespaces/analytics/tables/session_metrics'
    
    def _encode_sample_event(self):
        """Helper to encode sample event as base64"""
        json_string = json.dumps(self.sample_web_event)
        return base64.b64encode(json_string.encode('utf-8')).decode('utf-8')
    
    @mock_s3
    def test_lambda_handler_success(self):
        """Test successful processing of Kinesis records"""
        # Create mock S3 bucket
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # Mock context
        context = Mock()
        context.aws_request_id = 'test-request-id'
        
        with patch('lambda_processor.s3_client', s3_client):
            response = lambda_module.lambda_handler(self.sample_kinesis_event, context)
        
        # Verify successful response
        assert response['statusCode'] == 200
        
        # Parse response body
        response_body = json.loads(response['body'])
        assert response_body['processed'] == 1
        assert response_body['failed'] == 0
    
    def test_lambda_handler_invalid_records(self):
        """Test handling of invalid Kinesis records"""
        # Create event with invalid base64 data
        invalid_event = {
            "Records": [
                {
                    "eventID": "test-id",
                    "kinesis": {
                        "data": "invalid-base64-data!!!",
                        "sequenceNumber": "123",
                        "partitionKey": "test"
                    }
                }
            ]
        }
        
        context = Mock()
        
        with patch('lambda_processor.s3_client'):
            response = lambda_module.lambda_handler(invalid_event, context)
        
        # Should handle errors gracefully
        assert response['statusCode'] == 200
        response_body = json.loads(response['body'])
        assert response_body['failed'] >= 1
    
    def test_lambda_handler_empty_records(self):
        """Test handling of empty record list"""
        empty_event = {"Records": []}
        context = Mock()
        
        with patch('lambda_processor.s3_client'):
            response = lambda_module.lambda_handler(empty_event, context)
        
        assert response['statusCode'] == 200
        response_body = json.loads(response['body'])
        assert response_body['processed'] == 0
        assert response_body['failed'] == 0
    
    def test_lambda_handler_exception_handling(self):
        """Test overall exception handling in Lambda handler"""
        context = Mock()
        
        # Simulate exception in processing
        with patch('lambda_processor.enrich_event_for_iceberg', side_effect=Exception("Test exception")):
            response = lambda_module.lambda_handler(self.sample_kinesis_event, context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'error' in response_body


class TestEventEnrichmentForIceberg:
    """Test event enrichment functions for S3 Tables/Iceberg"""
    
    def setup_method(self):
        """Setup test data"""
        self.sample_event_data = {
            "type": "page",
            "properties": {
                "title": "Test Page",
                "url": "https://example.com/viewer?token=abc123",
                "path": "/viewer",
                "width": 1920,
                "height": 1080,
                "spaceId": "space-123",
                "spaceName": "Test Space",
                "spaceType": "unit",
                "spaceToken": "abc123"
            },
            "anonymousId": "anon-123",
            "sessionId": "session-123",
            "secondsUtcTS": 1674840769225,
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "timezone": "America/New_York",
            "appId": "web-viewer",
            "app": "web-viewer"
        }
        
        self.kinesis_data = {
            "sequenceNumber": "49638818049273582875652834848275797507965104553950068738",
            "partitionKey": "test-partition"
        }
    
    def test_enrich_event_for_iceberg_complete_data(self):
        """Test enrichment of complete event data"""
        result = lambda_module.enrich_event_for_iceberg(self.sample_event_data, self.kinesis_data)
        
        assert result is not None, "Should successfully enrich complete data"
        
        # Test required Iceberg fields
        assert 'event_id' in result, "Should generate event_id"
        assert 'event_timestamp' in result, "Should format timestamp for Iceberg"
        assert 'event_date' in result, "Should extract date for partitioning"
        assert 'event_hour' in result, "Should extract hour"
        
        # Test core event fields
        assert result['event_type'] == 'page', "Should preserve event type"
        assert result['anonymous_id'] == 'anon-123', "Should preserve anonymous ID"
        assert result['session_id'] == 'session-123', "Should preserve session ID"
        
        # Test space context extraction
        assert result['space_id'] == 'space-123', "Should extract space ID"
        assert result['space_type'] == 'unit', "Should extract space type"
        assert result['space_name'] == 'Test Space', "Should extract space name"
        
        # Test derived fields
        assert result['device_type'] == 'desktop', "Should classify device type correctly"
        assert result['is_virtual_tour_page'] == True, "Should detect virtual tour page"
        assert result['has_space_context'] == True, "Should detect space context"
        
        # Test processing metadata
        assert 'processed_timestamp' in result, "Should include processing timestamp"
        assert result['kinesis_sequence_number'] == self.kinesis_data['sequenceNumber']
        assert result['kinesis_partition_key'] == self.kinesis_data['partitionKey']
        
        # Test data quality
        assert 'data_quality_score' in result, "Should calculate data quality score"
        assert result['data_quality_score'] > 0.5, "Complete data should have good quality score"
        
        # Test bot detection
        assert 'is_bot' in result, "Should include bot detection result"
        assert result['is_bot'] == False, "Normal user agent should not be flagged as bot"
    
    def test_enrich_event_missing_optional_fields(self):
        """Test enrichment with missing optional fields"""
        minimal_event = {
            "type": "page",
            "sessionId": "session-minimal",
            "anonymousId": "anon-minimal"
            # Missing properties, timestamps, etc.
        }
        
        result = lambda_module.enrich_event_for_iceberg(minimal_event, self.kinesis_data)
        
        assert result is not None, "Should handle minimal data"
        assert result['event_type'] == 'page', "Should preserve available fields"
        assert result['session_id'] == 'session-minimal', "Should preserve session ID"
        
        # Missing fields should have None or default values
        assert result['space_id'] is None, "Missing space_id should be None"
        assert result['page_url'] is None, "Missing URL should be None"
        assert result['device_type'] == 'unknown', "Missing screen width should result in unknown device"
        
        # Should still generate required fields
        assert 'event_timestamp' in result, "Should generate timestamp for missing timestamp"
        assert result['data_quality_score'] < 1.0, "Missing fields should reduce quality score"
    
    def test_enrich_event_timestamp_handling(self):
        """Test timestamp parsing and ISO formatting"""
        # Test with valid timestamp
        event_with_timestamp = self.sample_event_data.copy()
        event_with_timestamp['secondsUtcTS'] = 1674840769225
        
        result = lambda_module.enrich_event_for_iceberg(event_with_timestamp, self.kinesis_data)
        
        # Check original timestamp preservation
        assert result['seconds_utc_ts'] == 1674840769225
        
        # Check ISO format
        iso_timestamp = result['event_timestamp']
        assert 'T' in iso_timestamp, "Should be in ISO format"
        
        # Check date extraction
        event_date = result['event_date']
        assert len(event_date) == 10, "Date should be YYYY-MM-DD format"
        
        # Check hour extraction
        assert isinstance(result['event_hour'], int), "Hour should be integer"
        assert 0 <= result['event_hour'] <= 23, "Hour should be valid (0-23)"
        
        # Test with missing timestamp
        event_no_timestamp = {"type": "page", "sessionId": "test", "anonymousId": "test"}
        result_no_ts = lambda_module.enrich_event_for_iceberg(event_no_timestamp, self.kinesis_data)
        
        assert result_no_ts['seconds_utc_ts'] > 0, "Should generate current timestamp"
    
    def test_enrich_event_error_handling(self):
        """Test error handling in enrichment function"""
        # Test with None input
        result = lambda_module.enrich_event_for_iceberg(None, self.kinesis_data)
        assert result is None, "Should return None for None input"
        
        # Test with invalid data types
        invalid_event = {
            "secondsUtcTS": "not-a-number",
            "properties": "not-a-dict"
        }
        
        # Should handle gracefully (might return None or partial data)
        result = lambda_module.enrich_event_for_iceberg(invalid_event, self.kinesis_data)
        # Result depends on implementation - should not crash


class TestDeviceClassification:
    """Test device type classification logic"""
    
    def test_classify_device_type_desktop(self):
        """Test desktop device classification"""
        desktop_widths = [1920, 1366, 1440, 1600, 2560]
        
        for width in desktop_widths:
            result = lambda_module.classify_device_type(width)
            assert result == 'desktop', f"Width {width} should be classified as desktop"
    
    def test_classify_device_type_tablet(self):
        """Test tablet device classification"""
        tablet_widths = [768, 800, 1024, 834, 1000]
        
        for width in tablet_widths:
            result = lambda_module.classify_device_type(width)
            assert result == 'tablet', f"Width {width} should be classified as tablet"
    
    def test_classify_device_type_mobile(self):
        """Test mobile device classification"""
        mobile_widths = [320, 375, 414, 428, 767]
        
        for width in mobile_widths:
            result = lambda_module.classify_device_type(width)
            assert result == 'mobile', f"Width {width} should be classified as mobile"
    
    def test_classify_device_type_edge_cases(self):
        """Test edge cases in device classification"""
        # Boundary cases
        assert lambda_module.classify_device_type(767) == 'mobile', "767px should be mobile"
        assert lambda_module.classify_device_type(768) == 'tablet', "768px should be tablet"
        assert lambda_module.classify_device_type(1023) == 'tablet', "1023px should be tablet"
        assert lambda_module.classify_device_type(1024) == 'desktop', "1024px should be desktop"
        
        # Invalid inputs
        assert lambda_module.classify_device_type(None) == 'unknown', "None should be unknown"
        assert lambda_module.classify_device_type('') == 'unknown', "Empty string should be unknown"
        assert lambda_module.classify_device_type('not-a-number') == 'unknown', "String should be unknown"
        assert lambda_module.classify_device_type(-100) == 'unknown', "Negative should be unknown"


class TestDataQualityScoring:
    """Test data quality scoring algorithm"""
    
    def test_calculate_data_quality_score_perfect_data(self):
        """Test data quality scoring with perfect data"""
        perfect_event = {
            "sessionId": "session-123",
            "anonymousId": "anon-123", 
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        perfect_properties = {
            "spaceId": "space-123",
            "path": "/viewer"
        }
        
        score = lambda_module.calculate_data_quality_score(perfect_event, perfect_properties)
        assert score == 1.0, "Perfect data should score 1.0"
    
    def test_calculate_data_quality_score_missing_session(self):
        """Test quality score with missing session ID"""
        event_no_session = {
            "anonymousId": "anon-123",
            "userAgent": "Mozilla/5.0..."
        }
        
        score = lambda_module.calculate_data_quality_score(event_no_session, {})
        assert score == 0.6, "Missing session ID should reduce score by 0.4"
    
    def test_calculate_data_quality_score_missing_anonymous_id(self):
        """Test quality score with missing anonymous ID"""
        event_no_anon = {
            "sessionId": "session-123",
            "userAgent": "Mozilla/5.0..."
        }
        
        score = lambda_module.calculate_data_quality_score(event_no_anon, {})
        assert score == 0.7, "Missing anonymous ID should reduce score by 0.3"
    
    def test_calculate_data_quality_score_missing_user_agent(self):
        """Test quality score with missing user agent"""
        event_no_ua = {
            "sessionId": "session-123",
            "anonymousId": "anon-123"
        }
        
        score = lambda_module.calculate_data_quality_score(event_no_ua, {})
        assert score == 0.8, "Missing user agent should reduce score by 0.2"
    
    def test_calculate_data_quality_score_missing_space_on_viewer(self):
        """Test quality score with missing space ID on viewer page"""
        event = {
            "sessionId": "session-123",
            "anonymousId": "anon-123",
            "userAgent": "Mozilla/5.0..."
        }
        
        viewer_properties = {"path": "/viewer"}  # Missing spaceId
        
        score = lambda_module.calculate_data_quality_score(event, viewer_properties)
        assert score == 0.9, "Missing space ID on viewer page should reduce score by 0.1"
    
    def test_calculate_data_quality_score_multiple_issues(self):
        """Test quality score with multiple missing fields"""
        minimal_event = {}
        minimal_properties = {"path": "/viewer"}
        
        score = lambda_module.calculate_data_quality_score(minimal_event, minimal_properties)
        assert score == 0.0, "Multiple missing fields should result in minimum score"


class TestBotDetection:
    """Test bot traffic detection logic"""
    
    def test_detect_bot_traffic_known_bots(self):
        """Test detection of known bot user agents"""
        bot_user_agents = [
            "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
            "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
            "Twitterbot/1.0",
            "crawler-test-bot",
            "automated-scraper-tool",
            "spider-indexer"
        ]
        
        for bot_ua in bot_user_agents:
            result = lambda_module.detect_bot_traffic(bot_ua)
            assert result == True, f"Should detect bot: {bot_ua}"
    
    def test_detect_bot_traffic_legitimate_browsers(self):
        """Test that legitimate browsers are not flagged as bots"""
        legitimate_user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/91.0.4472.80 Mobile/15E148 Safari/604.1"
        ]
        
        for legit_ua in legitimate_user_agents:
            result = lambda_module.detect_bot_traffic(legit_ua)
            assert result == False, f"Should not detect legitimate browser as bot: {legit_ua[:50]}..."
    
    def test_detect_bot_traffic_edge_cases(self):
        """Test bot detection edge cases"""
        # Empty or None user agent
        assert lambda_module.detect_bot_traffic(None) == True, "None user agent should be flagged"
        assert lambda_module.detect_bot_traffic("") == True, "Empty user agent should be flagged"
        
        # Case sensitivity
        assert lambda_module.detect_bot_traffic("GOOGLEBOT/2.1") == True, "Should be case insensitive"
        assert lambda_module.detect_bot_traffic("Bot-Scanner") == True, "Should detect 'Bot' anywhere in string"


class TestS3TablesStaging:
    """Test S3 Tables staging functionality"""
    
    def setup_method(self):
        """Setup test data for staging tests"""
        self.sample_events = [
            {
                'event_id': 'event-1',
                'event_date': '2024-01-27',
                'space_type': 'unit',
                'session_id': 'session-1',
                'event_type': 'page'
            },
            {
                'event_id': 'event-2', 
                'event_date': '2024-01-27',
                'space_type': 'amenity',
                'session_id': 'session-2',
                'event_type': 'track'
            },
            {
                'event_id': 'event-3',
                'event_date': '2024-01-28',
                'space_type': 'unit',
                'session_id': 'session-3',
                'event_type': 'page'
            }
        ]
    
    @mock_s3
    def test_write_to_s3_tables_staging_success(self):
        """Test successful write to S3 Tables staging"""
        # Setup mock S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        with patch('lambda_processor.s3_client', s3_client):
            with patch('lambda_processor.BUCKET_NAME', 'test-bucket'):
                # Should not raise exception
                lambda_module.write_to_s3_tables_staging(self.sample_events)
        
        # Verify files were created
        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='streaming-staging/')
        assert 'Contents' in objects, "Should create staging files"
        assert len(objects['Contents']) >= 1, "Should create at least one staging file"
    
    @mock_s3
    def test_write_to_s3_tables_staging_partitioning(self):
        """Test that events are partitioned correctly by date and space_type"""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        with patch('lambda_processor.s3_client', s3_client):
            with patch('lambda_processor.BUCKET_NAME', 'test-bucket'):
                lambda_module.write_to_s3_tables_staging(self.sample_events)
        
        # Check partition structure in S3 keys
        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='streaming-staging/')
        
        keys = [obj['Key'] for obj in objects['Contents']]
        
        # Should have different partitions for different date/space_type combinations
        # Expected partitions: 2024-01-27_unit, 2024-01-27_amenity, 2024-01-28_unit
        partition_patterns = ['2024-01-27_unit', '2024-01-27_amenity', '2024-01-28_unit']
        
        for pattern in partition_patterns:
            matching_keys = [key for key in keys if pattern in key]
            assert len(matching_keys) >= 1, f"Should create file for partition {pattern}"
    
    @mock_s3  
    def test_write_to_s3_tables_staging_jsonl_format(self):
        """Test that data is written in JSON Lines format"""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        with patch('lambda_processor.s3_client', s3_client):
            with patch('lambda_processor.BUCKET_NAME', 'test-bucket'):
                lambda_module.write_to_s3_tables_staging(self.sample_events)
        
        # Get one of the created objects
        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='streaming-staging/')
        first_key = objects['Contents'][0]['Key']
        
        # Read the content
        obj_response = s3_client.get_object(Bucket='test-bucket', Key=first_key)
        content = obj_response['Body'].read().decode('utf-8')
        
        # Should be JSON Lines format (one JSON object per line)
        lines = content.strip().split('\n')
        
        for line in lines:
            # Each line should be valid JSON
            event_data = json.loads(line)
            assert 'event_id' in event_data, "Each line should be a valid event"
            assert 'event_date' in event_data, "Should include partitioning fields"
    
    @mock_s3
    def test_write_to_s3_tables_staging_metadata(self):
        """Test that S3 objects have correct metadata"""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        with patch('lambda_processor.s3_client', s3_client):
            with patch('lambda_processor.BUCKET_NAME', 'test-bucket'):
                lambda_module.write_to_s3_tables_staging(self.sample_events)
        
        # Get object metadata
        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='streaming-staging/')
        first_key = objects['Contents'][0]['Key']
        
        obj_metadata = s3_client.head_object(Bucket='test-bucket', Key=first_key)
        
        # Check content type
        assert obj_metadata['ContentType'] == 'application/jsonl'
        
        # Check server-side encryption
        assert 'ServerSideEncryption' in obj_metadata
        
        # Check custom metadata
        metadata = obj_metadata.get('Metadata', {})
        assert 'table-target' in metadata, "Should include table target metadata"
        assert 'source' in metadata, "Should include source metadata"
        assert metadata['source'] == 'lambda-kinesis-processor'
    
    def test_write_to_s3_tables_staging_empty_events(self):
        """Test handling of empty events list"""
        with patch('lambda_processor.s3_client') as mock_s3:
            lambda_module.write_to_s3_tables_staging([])
            
            # Should not attempt to write anything
            mock_s3.put_object.assert_not_called()
    
    def test_write_to_s3_tables_staging_error_handling(self):
        """Test error handling in S3 staging write"""
        with patch('lambda_processor.s3_client') as mock_s3:
            # Simulate S3 error
            mock_s3.put_object.side_effect = Exception("S3 write error")
            
            # Should raise exception
            with pytest.raises(Exception):
                lambda_module.write_to_s3_tables_staging(self.sample_events)


class TestGlueJobIntegration:
    """Test integration with Glue streaming job"""
    
    @mock_glue
    def test_glue_job_health_check(self):
        """Test checking Glue streaming job status"""
        glue_client = boto3.client('glue', region_name='us-east-1')
        
        # Create mock job
        glue_client.create_job(
            Name='peek-web-events-kinesis-processor',
            Role='arn:aws:iam::123456789012:role/GlueRole',
            Command={
                'Name': 'gluestreaming',
                'ScriptLocation': 's3://test-bucket/script.py'
            }
        )
        
        with patch('lambda_processor.glue_client', glue_client):
            # The actual function would check job runs
            response = glue_client.get_job_runs(
                JobName='peek-web-events-kinesis-processor',
                MaxResults=1
            )
            
            # Should not error (job exists)
            assert 'JobRuns' in response


class TestLambdaEnvironmentConfiguration:
    """Test Lambda environment configuration"""
    
    def test_environment_variables_loaded(self):
        """Test that required environment variables are loaded"""
        # These should be set in setup_method
        assert os.environ.get('BUCKET_NAME') == 'test-bucket'
        assert os.environ.get('GLUE_DATABASE') == 'test_database'
        assert 'TABLE_BUCKET_ARN' in os.environ
        assert 'WEB_EVENTS_TABLE' in os.environ
        assert 'SESSION_METRICS_TABLE' in os.environ
    
    def test_s3_tables_arn_format(self):
        """Test S3 Tables ARN format validation"""
        table_bucket_arn = os.environ.get('TABLE_BUCKET_ARN')
        web_events_table = os.environ.get('WEB_EVENTS_TABLE')
        
        # Validate ARN format
        assert table_bucket_arn.startswith('arn:aws:s3tables:')
        assert 'bucket/' in table_bucket_arn
        
        assert web_events_table.startswith('arn:aws:s3tables:')
        assert 'namespaces/analytics/tables/web_events' in web_events_table


class TestEventIdGeneration:
    """Test event ID generation for deduplication"""
    
    def test_event_id_uniqueness(self):
        """Test that event IDs are unique for different events"""
        event1 = {
            "sessionId": "session-1",
            "secondsUtcTS": 1674840769225
        }
        
        event2 = {
            "sessionId": "session-2", 
            "secondsUtcTS": 1674840769226
        }
        
        kinesis_data1 = {"sequenceNumber": "seq-1", "partitionKey": "key-1"}
        kinesis_data2 = {"sequenceNumber": "seq-2", "partitionKey": "key-2"}
        
        result1 = lambda_module.enrich_event_for_iceberg(event1, kinesis_data1)
        result2 = lambda_module.enrich_event_for_iceberg(event2, kinesis_data2)
        
        assert result1['event_id'] != result2['event_id'], "Event IDs should be unique"
    
    def test_event_id_format(self):
        """Test event ID format contains expected components"""
        event = {
            "sessionId": "test-session",
            "secondsUtcTS": 1674840769225
        }
        
        kinesis_data = {"sequenceNumber": "test-seq", "partitionKey": "test-key"}
        
        result = lambda_module.enrich_event_for_iceberg(event, kinesis_data)
        event_id = result['event_id']
        
        # Should contain session ID, sequence number, and timestamp
        assert 'test-session' in event_id
        assert 'test-seq' in event_id
        assert '1674840769225' in event_id


class TestErrorHandlingAndResilience:
    """Test error handling and system resilience"""
    
    def test_malformed_kinesis_record_handling(self):
        """Test handling of malformed Kinesis records"""
        malformed_event = {
            "Records": [
                {
                    "eventID": "test-id",
                    "kinesis": {
                        "data": "not-base64-data!!!",
                        # Missing required fields
                    }
                }
            ]
        }
        
        context = Mock()
        
        with patch('lambda_processor.s3_client'):
            response = lambda_module.lambda_handler(malformed_event, context)
        
        # Should handle gracefully
        assert response['statusCode'] == 200
        response_body = json.loads(response['body'])
        assert response_body['failed'] >= 1
    
    def test_s3_write_failure_handling(self):
        """Test handling of S3 write failures"""
        with patch('lambda_processor.s3_client') as mock_s3:
            mock_s3.put_object.side_effect = Exception("S3 unavailable")
            
            context = Mock()
            
            # Should propagate S3 errors
            with pytest.raises(Exception):
                lambda_module.write_to_s3_tables_staging([{"event_id": "test"}])
    
    def test_partial_record_processing(self):
        """Test processing when some records succeed and others fail"""
        mixed_event = {
            "Records": [
                {
                    "eventID": "good-record",
                    "kinesis": {
                        "data": self._encode_valid_event(),
                        "sequenceNumber": "seq-1",
                        "partitionKey": "key-1"
                    }
                },
                {
                    "eventID": "bad-record", 
                    "kinesis": {
                        "data": "invalid-base64!!!",
                        "sequenceNumber": "seq-2",
                        "partitionKey": "key-2"
                    }
                }
            ]
        }
        
        context = Mock()
        
        with patch('lambda_processor.s3_client'):
            response = lambda_module.lambda_handler(mixed_event, context)
        
        response_body = json.loads(response['body'])
        assert response_body['processed'] >= 1, "Should process valid records"
        assert response_body['failed'] >= 1, "Should track failed records"
    
    def _encode_valid_event(self):
        """Helper to encode a valid event"""
        event = {
            "type": "page",
            "sessionId": "test-session",
            "anonymousId": "test-anon"
        }
        return base64.b64encode(json.dumps(event).encode('utf-8')).decode('utf-8')


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=5"])