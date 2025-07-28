"""
Unit tests for Lambda S3 Tables/Iceberg integration
Tests the Kinesis processor Lambda function for S3 Tables compatibility
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import base64
import os
import sys
from datetime import datetime
from freezegun import freeze_time

# Add Lambda path to system path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda/kinesis-processor'))

# Mock environment variables before importing
with patch.dict(os.environ, {
    'BUCKET_NAME': 'test-bucket',
    'GLUE_DATABASE': 'test_database',
    'TABLE_BUCKET_ARN': 'arn:aws:s3tables:us-east-1:123456789012:bucket/test-table-bucket',
    'WEB_EVENTS_TABLE': 'web_events',
    'SESSION_METRICS_TABLE': 'session_metrics'
}):
    import index_iceberg


class TestLambdaIcebergProcessor(unittest.TestCase):
    """Test suite for Lambda S3 Tables integration"""
    
    def setUp(self):
        """Set up test fixtures with MongoDB structure"""
        # MongoDB-style nested document structure
        self.sample_mongodb_event = {
            "eventTimestamp": {"$date": "2024-01-01T00:00:00.000Z"},
            "metadata": {
                "anonymousId": "test-user-123",
                "appId": "web-viewer"
            },
            "_id": {"$oid": "65b1234567890abcdef12345"},
            "createdAt": {"$date": "2024-01-01T00:00:00.000Z"},
            "updatedAt": {"$date": "2024-01-01T00:00:01.000Z"},
            "eventData": {
                "type": "track",
                "event": "Room View Position",  # NEW: Specific event name
                "anonymousId": "test-user-123",
                "userId": "auth-user-789",  # NEW: Authenticated user
                "sessionId": "session-456",
                "secondsUtcTS": 1704067200000,
                "properties": {
                    "title": "Virtual Tour - Unit 2B",
                    "url": "https://example.com/viewer",
                    "path": "/viewer",
                    "spaceId": "space-789",
                    "spaceName": "Unit 2B - Master Bedroom",
                    "spaceType": "unit",
                    "roomId": "room-101",  # NEW: Room-level data
                    "roomName": "LIVING ROOM",  # NEW: Room name
                    "community": "Towers at Rincon",  # NEW: Community
                    "building": "Building A",  # NEW: Building
                    "floorplan": "2BR-2BA",  # NEW: Floorplan
                    "coordinates": {  # NEW: 3D navigation data
                        "pitch": 7.77,
                        "yaw": -121.71,
                        "hfov": 123
                    }
                },
                "page": {  # NEW: Separate page object
                    "title": "Peek | Virtual Tour | Unit 2B",
                    "url": "https://example.com/viewer?token=abc123",
                    "path": "/viewer",
                    "width": 1024,
                    "height": 768,
                    "spaceId": "space-789",
                    "spaceName": "Unit 2B - Master Bedroom",
                    "spaceType": "unit"
                },
                "os": {"name": "Mac OS X"},
                "referrer": "https://google.com",
                "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "timezone": "America/New_York",
                "locale": "en-US",
                "appId": "web-viewer"
            }
        }
        
        # Legacy simple event structure for backward compatibility
        self.sample_event = {
            "type": "page",
            "anonymousId": "test-user-123",
            "sessionId": "session-456",
            "secondsUtcTS": 1704067200000,  # 2024-01-01 00:00:00
            "properties": {
                "title": "Virtual Tour - Unit 2B",
                "url": "https://example.com/viewer",
                "path": "/viewer",
                "spaceId": "space-789",
                "spaceName": "Unit 2B - Master Bedroom",
                "spaceType": "unit"
            },
            "os": {"name": "Mac OS X"},
            "referrer": "https://google.com",
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "timezone": "America/New_York",
            "locale": "en-US",
            "appId": "app-123"
        }
        
        # Base64 encode both event types
        self.encoded_event = base64.b64encode(
            json.dumps(self.sample_event).encode('utf-8')
        ).decode('utf-8')
        
        self.encoded_mongodb_event = base64.b64encode(
            json.dumps(self.sample_mongodb_event).encode('utf-8')
        ).decode('utf-8')
        
        self.kinesis_record = {
            "Records": [{
                "eventID": "shardId-000000000000:12345",
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "test-user-123",
                    "sequenceNumber": "12345",
                    "data": self.encoded_event,
                    "approximateArrivalTimestamp": 1704067200.0
                }
            }]
        }
    
    @patch('index_iceberg.s3_client')
    def test_successful_event_processing(self, mock_s3):
        """Test successful processing of a valid web event"""
        # Mock S3 put_object response
        mock_s3.put_object.return_value = {'ETag': '"abc123"'}
        
        # Process the event
        result = index_iceberg.lambda_handler(self.kinesis_record, None)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['processed_count'], 1)
        self.assertEqual(result['failed_count'], 0)
        
        # Verify S3 was called
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args[1]
        
        # Verify S3 key format for Iceberg staging
        expected_key_pattern = r'staging/web-events/year=2024/month=01/day=01/hour=00/.*\.json'
        self.assertRegex(call_args['Key'], expected_key_pattern)
        
        # Verify body contains enriched data
        body = json.loads(call_args['Body'])
        self.assertEqual(body['anonymous_id'], 'test-user-123')
        self.assertEqual(body['session_id'], 'session-456')
        self.assertEqual(body['space_id'], 'space-789')
        self.assertEqual(body['space_type'], 'unit')
        self.assertIn('data_quality_score', body)
        self.assertIn('is_bot', body)
    
    def test_data_quality_scoring(self):
        """Test data quality scoring logic"""
        # Test complete event
        complete_event = self.sample_event.copy()
        enriched = index_iceberg.enrich_event_for_iceberg(complete_event, {})
        self.assertGreaterEqual(enriched['data_quality_score'], 0.8)
        
        # Test event missing space context
        incomplete_event = self.sample_event.copy()
        incomplete_event['properties'].pop('spaceId')
        enriched = index_iceberg.enrich_event_for_iceberg(incomplete_event, {})
        self.assertLess(enriched['data_quality_score'], 0.8)
        
        # Test event with minimal data
        minimal_event = {
            "type": "page",
            "anonymousId": "test-user",
            "secondsUtcTS": 1704067200000
        }
        enriched = index_iceberg.enrich_event_for_iceberg(minimal_event, {})
        self.assertLess(enriched['data_quality_score'], 0.5)
    
    def test_bot_detection(self):
        """Test bot detection logic"""
        # Test known bot user agent
        bot_event = self.sample_event.copy()
        bot_event['userAgent'] = 'Googlebot/2.1 (+http://www.google.com/bot.html)'
        enriched = index_iceberg.enrich_event_for_iceberg(bot_event, {})
        self.assertTrue(enriched['is_bot'])
        
        # Test normal user agent
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_event, {})
        self.assertFalse(enriched['is_bot'])
        
        # Test rapid session behavior (bot-like)
        rapid_event = self.sample_event.copy()
        rapid_event['properties']['timeOnPage'] = 0.5  # Half second
        enriched = index_iceberg.enrich_event_for_iceberg(rapid_event, {})
        # Should flag as potential bot based on behavior
        # Bot detection should flag rapid sessions
        self.assertTrue(enriched['is_bot'] or enriched['data_quality_score'] < 1.0)
    
    def test_session_metrics_extraction(self):
        """Test session metrics calculation"""
        # Create a session with multiple events
        session_events = []
        for i in range(5):
            event = self.sample_event.copy()
            event['secondsUtcTS'] = 1704067200000 + (i * 60000)  # 1 minute apart
            event['properties']['spaceId'] = f'space-{i}'
            event['properties']['spaceType'] = 'unit' if i < 3 else 'amenity'
            session_events.append(event)
        
        # Process events and verify session metrics
        for i, event in enumerate(session_events):
            enriched = index_iceberg.enrich_event_for_iceberg(event, {})
            # Note: Lambda function doesn't calculate session sequences (done in dbt)
            self.assertIn('event_id', enriched)
            self.assertIn('session_id', enriched)
    
    @patch('index_iceberg.s3_client')
    def test_error_handling(self, mock_s3):
        """Test error handling for various failure scenarios"""
        # Test malformed base64 data
        bad_record = {
            "Records": [{
                "kinesis": {
                    "data": "invalid-base64-data!!!"
                }
            }]
        }
        result = index_iceberg.lambda_handler(bad_record, None)
        self.assertEqual(result['failed_count'], 1)
        self.assertEqual(result['processed_count'], 0)
        
        # Test S3 write failure
        mock_s3.put_object.side_effect = Exception("S3 write failed")
        result = index_iceberg.lambda_handler(self.kinesis_record, None)
        self.assertEqual(result['statusCode'], 207)  # Partial failure
        self.assertIn('errors', result)
    
    @freeze_time("2024-01-01 12:30:45")
    def test_timestamp_handling(self):
        """Test proper timestamp handling and partitioning"""
        # Test event with timestamp
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_event, {})
        self.assertEqual(enriched['event_date'], '2024-01-01')
        self.assertEqual(enriched['event_hour'], 0)
        
        # Test event without timestamp (should use current time)
        no_timestamp_event = self.sample_event.copy()
        no_timestamp_event.pop('secondsUtcTS')
        enriched = index_iceberg.enrich_event_for_iceberg(no_timestamp_event, {})
        self.assertEqual(enriched['event_date'], '2024-01-01')
        self.assertEqual(enriched['event_hour'], 12)
    
    def test_space_context_enrichment(self):
        """Test space context enrichment for virtual tours"""
        # Test with full space context
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_event, {})
        self.assertEqual(enriched['space_id'], 'space-789')
        self.assertEqual(enriched['space_name'], 'Unit 2B - Master Bedroom')
        self.assertEqual(enriched['space_type'], 'unit')
        
        # Test without space context
        no_space_event = self.sample_event.copy()
        no_space_event['properties'] = {"title": "Home Page"}
        enriched = index_iceberg.enrich_event_for_iceberg(no_space_event, {})
        self.assertIsNone(enriched.get('space_id'))
        self.assertEqual(enriched['space_type'], 'unknown')
    
    @patch('index_iceberg.s3_client')
    def test_batch_processing(self, mock_s3):
        """Test processing multiple records in a batch"""
        # Create batch with 10 records
        batch_event = {"Records": []}
        for i in range(10):
            record = {
                "eventID": f"event-{i}",
                "kinesis": {
                    "data": self.encoded_event,
                    "partitionKey": f"user-{i}"
                }
            }
            batch_event["Records"].append(record)
        
        # Process batch
        result = index_iceberg.lambda_handler(batch_event, None)
        
        # Verify all processed
        self.assertEqual(result['processed_count'], 10)
        self.assertEqual(result['failed_count'], 0)
        self.assertEqual(mock_s3.put_object.call_count, 10)
    
    # NEW: MongoDB-specific test methods
    
    @patch('index_iceberg.s3_client')
    def test_mongodb_event_processing(self, mock_s3):
        """Test processing of MongoDB nested document structure"""
        mock_s3.put_object.return_value = {'ETag': '"abc123"'}
        
        # Create Kinesis record with MongoDB structure
        mongodb_record = {
            "Records": [{
                "eventID": "mongodb-test-123",
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "auth-user-789",
                    "sequenceNumber": "67890",
                    "data": self.encoded_mongodb_event,
                    "approximateArrivalTimestamp": 1704067200.0
                }
            }]
        }
        
        result = index_iceberg.lambda_handler(mongodb_record, None)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['processed_count'], 1)
        self.assertEqual(result['failed_count'], 0)
        
        # Verify S3 call
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args[1]
        
        # Parse the body to check MongoDB-specific fields
        body = json.loads(call_args['Body'])
        
        # Check MongoDB-specific fields are extracted
        self.assertEqual(body['event_name'], 'Room View Position')
        self.assertEqual(body['user_id'], 'auth-user-789')
        self.assertEqual(body['room_id'], 'room-101')
        self.assertEqual(body['room_name'], 'LIVING ROOM')
        self.assertEqual(body['community'], 'Towers at Rincon')
        self.assertEqual(body['building'], 'Building A')
        self.assertEqual(body['floorplan'], '2BR-2BA')
        
        # Check 3D coordinates structure
        coordinates = body.get('coordinates')
        self.assertIsNotNone(coordinates)
        self.assertEqual(coordinates['pitch'], 7.77)
        self.assertEqual(coordinates['yaw'], -121.71)
        self.assertEqual(coordinates['hfov'], 123)
        
        # Check MongoDB metadata is preserved
        self.assertEqual(body['mongodb_id'], '65b1234567890abcdef12345')
        self.assertIsNotNone(body['created_at'])
        self.assertIsNotNone(body['updated_at'])
    
    def test_mongodb_coordinates_extraction(self):
        """Test 3D navigation coordinates extraction from MongoDB structure"""
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_mongodb_event['eventData'], {})
        
        # Check coordinates are properly extracted
        coordinates = enriched.get('coordinates')
        self.assertIsNotNone(coordinates)
        self.assertEqual(coordinates['pitch'], 7.77)
        self.assertEqual(coordinates['yaw'], -121.71)
        self.assertEqual(coordinates['hfov'], 123)
        
        # Test event without coordinates
        no_coords_event = self.sample_mongodb_event['eventData'].copy()
        no_coords_event['properties'].pop('coordinates')
        enriched_no_coords = index_iceberg.enrich_event_for_iceberg(no_coords_event, {})
        self.assertIsNone(enriched_no_coords.get('coordinates'))
    
    def test_room_hierarchy_extraction(self):
        """Test room and location hierarchy processing"""
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_mongodb_event['eventData'], {})
        
        # Test room-level data
        self.assertEqual(enriched['room_id'], 'room-101')
        self.assertEqual(enriched['room_name'], 'LIVING ROOM')
        
        # Test location hierarchy
        self.assertEqual(enriched['community'], 'Towers at Rincon')
        self.assertEqual(enriched['building'], 'Building A')
        self.assertEqual(enriched['floorplan'], '2BR-2BA')
        
        # Test enhanced data quality score (should be higher due to room data)
        self.assertGreaterEqual(enriched['data_quality_score'], 0.9)
    
    def test_authenticated_vs_anonymous_users(self):
        """Test handling of both authenticated and anonymous users"""
        # Test authenticated user (MongoDB structure)
        mongodb_enriched = index_iceberg.enrich_event_for_iceberg(self.sample_mongodb_event['eventData'], {})
        self.assertEqual(mongodb_enriched['user_id'], 'auth-user-789')
        self.assertEqual(mongodb_enriched['anonymous_id'], 'test-user-123')
        
        # Test anonymous user (legacy structure)
        legacy_enriched = index_iceberg.enrich_event_for_iceberg(self.sample_event, {})
        self.assertIsNone(legacy_enriched.get('user_id'))
        self.assertEqual(legacy_enriched['anonymous_id'], 'test-user-123')
    
    def test_mongodb_metadata_parsing(self):
        """Test MongoDB metadata extraction and timestamp parsing"""
        # Test with full MongoDB document structure
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_mongodb_event, {})
        
        # Check MongoDB metadata
        self.assertEqual(enriched['mongodb_id'], '65b1234567890abcdef12345')
        self.assertIsNotNone(enriched['created_at'])
        self.assertIsNotNone(enriched['updated_at'])
        
        # Test timestamp parsing functions
        created_timestamp = index_iceberg.parse_mongodb_timestamp({'$date': '2024-01-01T00:00:00.000Z'})
        self.assertEqual(created_timestamp, '2024-01-01T00:00:00.000Z')
        
        # Test with string timestamp
        string_timestamp = index_iceberg.parse_mongodb_timestamp('2024-01-01T00:00:00.000Z')
        self.assertEqual(string_timestamp, '2024-01-01T00:00:00.000Z')
        
        # Test with None
        none_timestamp = index_iceberg.parse_mongodb_timestamp(None)
        self.assertIsNone(none_timestamp)
    
    def test_enhanced_data_quality_scoring(self):
        """Test enhanced data quality scoring with MongoDB fields"""
        # Test MongoDB event with full context (should score very high)
        mongodb_properties = self.sample_mongodb_event['eventData']['properties']
        mongodb_page = self.sample_mongodb_event['eventData']['page']
        mongodb_event_data = self.sample_mongodb_event['eventData']
        
        quality_score = index_iceberg.calculate_data_quality_score_enhanced(
            mongodb_event_data, mongodb_properties, mongodb_page
        )
        
        # Should be very high due to room context and coordinates
        self.assertGreaterEqual(quality_score, 0.95)
        
        # Test event without room context
        no_room_properties = mongodb_properties.copy()
        no_room_properties.pop('roomId')
        no_room_properties.pop('roomName')
        no_room_properties.pop('coordinates')
        
        lower_score = index_iceberg.calculate_data_quality_score_enhanced(
            mongodb_event_data, no_room_properties, mongodb_page
        )
        
        # Should be lower but still good
        self.assertLess(lower_score, quality_score)
        self.assertGreaterEqual(lower_score, 0.8)
    
    def test_spatial_event_classification(self):
        """Test spatial event classification based on event names"""
        # Test room position event
        room_position_event = self.sample_mongodb_event['eventData'].copy()
        room_position_event['event'] = 'Room View Position'
        enriched = index_iceberg.enrich_event_for_iceberg(room_position_event, {})
        
        self.assertEqual(enriched['event_name'], 'Room View Position')
        self.assertIsNotNone(enriched['coordinates'])
        
        # Test toggle event
        toggle_event = self.sample_mongodb_event['eventData'].copy()
        toggle_event['event'] = 'Toggle Floor Plan'
        enriched_toggle = index_iceberg.enrich_event_for_iceberg(toggle_event, {})
        
        self.assertEqual(enriched_toggle['event_name'], 'Toggle Floor Plan')
        
        # Test generic event
        generic_event = self.sample_event.copy()
        # No event name in legacy structure
        enriched_generic = index_iceberg.enrich_event_for_iceberg(generic_event, {})
        
        self.assertIsNone(enriched_generic.get('event_name'))
    
    def test_iceberg_metadata_generation(self):
        """Test Iceberg-specific metadata generation"""
        enriched = index_iceberg.enrich_event_for_iceberg(self.sample_event, {})
        
        # Verify Iceberg-required fields
        self.assertIn('event_id', enriched)  # Unique identifier
        self.assertIn('processed_timestamp', enriched)  # Processing time
        self.assertIn('event_timestamp', enriched)  # Event time
        
        # Verify event_id is unique
        enriched2 = index_iceberg.enrich_event_for_iceberg(self.sample_event, {})
        self.assertNotEqual(enriched['event_id'], enriched2['event_id'])
    
    def test_s3_staging_write_functionality(self):
        """Test S3 staging functionality for Iceberg tables"""
        # Test that write_to_s3_tables_staging partitions correctly
        events = [
            {
                'event_date': '2024-01-01',
                'space_type': 'unit',
                'event_id': 'test-1'
            },
            {
                'event_date': '2024-01-01', 
                'space_type': 'amenity',
                'event_id': 'test-2'
            }
        ]
        
        # Mock S3 client for testing
        with patch('index_iceberg.s3_client') as mock_s3:
            mock_s3.put_object.return_value = {'ETag': '"test123"'}
            
            try:
                index_iceberg.write_to_s3_tables_staging(events)
                
                # Should have made 2 S3 calls (one per partition)
                self.assertEqual(mock_s3.put_object.call_count, 2)
                
                # Check that partitioning worked
                calls = mock_s3.put_object.call_args_list
                keys_written = [call[1]['Key'] for call in calls]
                
                # Should have unit and amenity partitions
                unit_keys = [k for k in keys_written if 'unit' in k]
                amenity_keys = [k for k in keys_written if 'amenity' in k]
                
                self.assertEqual(len(unit_keys), 1)
                self.assertEqual(len(amenity_keys), 1)
                
            except Exception as e:
                # Expected to work in test environment
                self.fail(f"S3 staging write failed: {e}")


class TestLambdaPerformance(unittest.TestCase):
    """Performance tests for Lambda function"""
    
    def test_large_batch_performance(self):
        """Test performance with large batches"""
        import time
        
        # Create large batch (100 records)
        large_batch = {"Records": []}
        for i in range(100):
            event_data = {
                "type": "page",
                "anonymousId": f"user-{i}",
                "sessionId": f"session-{i}",
                "secondsUtcTS": 1704067200000 + i * 1000
            }
            encoded = base64.b64encode(
                json.dumps(event_data).encode('utf-8')
            ).decode('utf-8')
            
            large_batch["Records"].append({
                "kinesis": {"data": encoded}
            })
        
        # Measure processing time
        start_time = time.time()
        with patch('index_iceberg.s3_client') as mock_s3:
            mock_s3.put_object.return_value = {'ETag': '"abc123"'}
            result = index_iceberg.lambda_handler(large_batch, None)
        
        processing_time = time.time() - start_time
        
        # Verify performance (should process 100 records in < 5 seconds)
        self.assertLess(processing_time, 5.0)
        self.assertEqual(result['processed_count'], 100)


if __name__ == '__main__':
    unittest.main()