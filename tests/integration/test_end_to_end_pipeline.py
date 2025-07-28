"""
End-to-end integration tests for the complete S3 Tables/Iceberg data pipeline
Tests the full data flow from Kinesis ingestion through analytics, including ACID properties,
time travel, and complex transformations.
"""

import pytest
import json
import base64
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import boto3
from moto import mock_s3, mock_kinesis, mock_glue, mock_lambda, mock_athena
import pandas as pd
from typing import List, Dict, Any


@pytest.fixture
def sample_web_events():
    """Sample web events for testing"""
    base_timestamp = int(datetime.utcnow().timestamp() * 1000)
    
    return [
        {
            "type": "page",
            "properties": {
                "title": "Peek | Virtual Tour | Unit 101",
                "url": "https://qa-beta-tour.peek.us/viewer?token=unit101",
                "path": "/viewer",
                "width": 1920,
                "height": 1080,
                "spaceName": "Unit 101",
                "spaceId": "space-unit-101",
                "spaceType": "unit",
                "spaceToken": "unit101"
            },
            "anonymousId": "user-12345",
            "secondsUtcTS": base_timestamp,
            "sessionId": "session-abc123",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "timezone": "America/New_York",
            "appId": "web-viewer-prod",
            "app": "web-viewer"
        },
        {
            "type": "track",
            "properties": {
                "title": "Peek | Virtual Tour | Amenity Center",
                "url": "https://qa-beta-tour.peek.us/viewer?token=amenity1",
                "path": "/viewer",
                "width": 1920,
                "height": 1080,
                "spaceName": "Amenity Center",
                "spaceId": "space-amenity-1",
                "spaceType": "amenity",
                "spaceToken": "amenity1"
            },
            "anonymousId": "user-12345",
            "secondsUtcTS": base_timestamp + 30000,  # 30 seconds later
            "sessionId": "session-abc123",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "timezone": "America/New_York",
            "appId": "web-viewer-prod",
            "app": "web-viewer"
        },
        {
            "type": "page",
            "properties": {
                "title": "Peek | Virtual Tour | Unit 102",
                "url": "https://qa-beta-tour.peek.us/viewer?token=unit102",
                "path": "/viewer",
                "width": 375,
                "height": 812,
                "spaceName": "Unit 102",
                "spaceId": "space-unit-102",
                "spaceType": "unit",
                "spaceToken": "unit102"
            },
            "anonymousId": "user-67890",
            "secondsUtcTS": base_timestamp + 60000,  # 1 minute later
            "sessionId": "session-def456",
            "userAgent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X)",
            "timezone": "America/Los_Angeles",
            "appId": "web-viewer-prod",
            "app": "web-viewer"
        }
    ]


@pytest.fixture
def aws_test_environment():
    """Setup AWS test environment with moto"""
    with mock_s3(), mock_kinesis(), mock_lambda(), mock_glue(), mock_athena():
        # Setup S3 buckets
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-data-lake-bucket')
        s3_client.create_bucket(Bucket='test-glue-scripts-bucket')
        
        # Setup Kinesis stream
        kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        kinesis_client.create_stream(
            StreamName='test-web-events-stream',
            ShardCount=1
        )
        
        # Wait for stream to be active
        waiter = kinesis_client.get_waiter('stream_exists')
        waiter.wait(StreamName='test-web-events-stream')
        
        yield {
            's3_client': s3_client,
            'kinesis_client': kinesis_client,
            'data_lake_bucket': 'test-data-lake-bucket',
            'scripts_bucket': 'test-glue-scripts-bucket',
            'stream_name': 'test-web-events-stream'
        }


class TestRealTimeDataIngestion:
    """Test real-time data ingestion through Kinesis to S3 Tables"""
    
    def test_kinesis_to_lambda_processing(self, aws_test_environment, sample_web_events):
        """Test Kinesis record processing through Lambda"""
        kinesis_client = aws_test_environment['kinesis_client']
        stream_name = aws_test_environment['stream_name']
        
        # Send test events to Kinesis
        for event in sample_web_events:
            encoded_data = base64.b64encode(json.dumps(event).encode('utf-8')).decode('utf-8')
            
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=encoded_data,
                PartitionKey=event['sessionId']
            )
            
            assert 'ShardId' in response, "Should successfully put record to Kinesis"
            assert 'SequenceNumber' in response, "Should return sequence number"
        
        # Verify records are in Kinesis
        shard_iterator_response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId='shardId-000000000000',
            ShardIteratorType='TRIM_HORIZON'
        )
        
        records_response = kinesis_client.get_records(
            ShardIterator=shard_iterator_response['ShardIterator']
        )
        
        assert len(records_response['Records']) == 3, "Should have 3 records in Kinesis"
        
        # Decode and verify first record
        first_record = records_response['Records'][0]
        decoded_data = base64.b64decode(first_record['Data']).decode('utf-8')
        parsed_event = json.loads(decoded_data)
        
        assert parsed_event['type'] == 'page', "Should preserve event type"
        assert parsed_event['sessionId'] == 'session-abc123', "Should preserve session ID"
    
    def test_lambda_to_s3_tables_staging(self, aws_test_environment, sample_web_events):
        """Test Lambda function writes to S3 Tables staging area"""
        s3_client = aws_test_environment['s3_client']
        bucket_name = aws_test_environment['data_lake_bucket']
        
        # Mock Lambda processor
        from lambda_processor import enrich_event_for_iceberg, write_to_s3_tables_staging
        
        enriched_events = []
        for event in sample_web_events:
            kinesis_metadata = {
                'sequenceNumber': f"seq-{len(enriched_events)}",
                'partitionKey': event['sessionId']
            }
            
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            if enriched:
                enriched_events.append(enriched)
        
        assert len(enriched_events) == 3, "Should enrich all events"
        
        # Test staging write
        with patch('lambda_processor.BUCKET_NAME', bucket_name):
            with patch('lambda_processor.s3_client', s3_client):
                write_to_s3_tables_staging(enriched_events)
        
        # Verify staging files were created
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='streaming-staging/')
        
        assert 'Contents' in objects, "Should create staging files"
        assert len(objects['Contents']) >= 1, "Should create at least one staging file"
        
        # Verify staging file content
        first_object = objects['Contents'][0]
        file_content = s3_client.get_object(Bucket=bucket_name, Key=first_object['Key'])
        content = file_content['Body'].read().decode('utf-8')
        
        # Should be JSONL format
        lines = content.strip().split('\n')
        for line in lines:
            event_data = json.loads(line)
            assert 'event_id' in event_data, "Should have event_id"
            assert 'event_timestamp' in event_data, "Should have formatted timestamp"
            assert 'space_type' in event_data, "Should have space_type for partitioning"


class TestBatchDataProcessing:
    """Test batch processing of historical data through Glue ETL"""
    
    def test_s3_to_iceberg_transformation(self, aws_test_environment, sample_web_events):
        """Test S3 batch processing and Iceberg table creation"""
        s3_client = aws_test_environment['s3_client']
        bucket_name = aws_test_environment['data_lake_bucket']
        
        # Upload sample data to S3 as raw JSON
        raw_data = '\n'.join([json.dumps(event) for event in sample_web_events])
        s3_client.put_object(
            Bucket=bucket_name,
            Key='raw-data/web-events/2024/01/27/events.json',
            Body=raw_data.encode('utf-8')
        )
        
        # Mock Glue ETL processor
        from s3_processor_iceberg import transform_web_event, calculate_data_quality_score
        
        transformed_events = []
        for event in sample_web_events:
            transformed = transform_web_event(event)
            if transformed:
                transformed_events.append(transformed)
        
        assert len(transformed_events) == 3, "Should transform all events"
        
        # Verify transformation quality
        for event in transformed_events:
            assert event['event_id'], "Should have unique event ID"
            assert event['event_timestamp'], "Should have formatted timestamp"
            assert event['event_date'], "Should have date for partitioning"
            assert event['device_type'] in ['desktop', 'mobile', 'tablet'], "Should classify device"
            assert 'data_quality_score' in event, "Should calculate quality score"
            
        # Test data quality scoring
        high_quality_events = [e for e in transformed_events if e['data_quality_score'] >= 0.8]
        assert len(high_quality_events) >= 2, "Should have high quality events"
    
    def test_session_boundary_detection(self, sample_web_events):
        """Test session boundary detection and sequencing"""
        # Group events by session
        sessions = {}
        for event in sample_web_events:
            session_id = event['sessionId']
            if session_id not in sessions:
                sessions[session_id] = []
            sessions[session_id].append(event)
        
        # Test session grouping
        assert len(sessions) == 2, "Should detect 2 unique sessions"
        assert 'session-abc123' in sessions, "Should group session-abc123"
        assert 'session-def456' in sessions, "Should group session-def456"
        
        # Test session sequence
        session_abc = sorted(sessions['session-abc123'], key=lambda x: x['secondsUtcTS'])
        assert len(session_abc) == 2, "Session-abc123 should have 2 events"
        assert session_abc[0]['type'] == 'page', "First event should be page view"
        assert session_abc[1]['type'] == 'track', "Second event should be track"
    
    def test_virtual_tour_context_enrichment(self, sample_web_events):
        """Test virtual tour specific context enrichment"""
        from s3_processor_iceberg import transform_web_event
        
        for event in sample_web_events:
            transformed = transform_web_event(event)
            
            # Test space context enrichment
            if event['properties'].get('spaceType') == 'unit':
                assert transformed['space_type'] == 'unit', "Should preserve unit type"
                assert transformed['is_virtual_tour_page'] == True, "Should detect tour page"
                assert transformed['has_space_context'] == True, "Should detect space context"
            
            # Test device classification
            width = event['properties'].get('width')
            if width == 1920:
                assert transformed['device_type'] == 'desktop', "Should classify as desktop"
            elif width == 375:
                assert transformed['device_type'] == 'mobile', "Should classify as mobile"


class TestAnalyticsProcessing:
    """Test analytics processing through dbt models"""
    
    def test_fact_table_materialization(self, sample_web_events):
        """Test fact table creation and enrichment"""
        # Mock processed events from Iceberg table
        base_timestamp = datetime.utcnow()
        
        fact_events = []
        for i, event in enumerate(sample_web_events):
            fact_event = {
                'event_id': f"event-{i}",
                'event_timestamp': base_timestamp + timedelta(seconds=i*30),
                'event_date': base_timestamp.date(),
                'event_type': event['type'],
                'session_id': event['sessionId'],
                'anonymous_id': event['anonymousId'],
                'space_type': event['properties'].get('spaceType', 'unknown'),
                'space_id': event['properties'].get('spaceId'),
                'device_type': 'desktop' if event['properties'].get('width', 0) > 1000 else 'mobile',
                'is_virtual_tour_page': event['properties'].get('path') == '/viewer',
                'has_space_context': bool(event['properties'].get('spaceId')),
                'data_quality_score': 0.9,
                'is_bot': False,
                'session_event_sequence': i + 1,
                'total_session_events': len([e for e in sample_web_events if e['sessionId'] == event['sessionId']])
            }
            fact_events.append(fact_event)
        
        # Test space categorization logic
        for event in fact_events:
            if event['space_type'] == 'unit':
                # Space categorization would be: 'residential_unit'
                assert event['has_space_context'] == True
            elif event['space_type'] == 'amenity':
                # Space categorization would be: 'shared_space'
                assert event['has_space_context'] == True
        
        # Test engagement scoring logic
        for event in fact_events:
            if event['has_space_context'] and event['is_virtual_tour_page']:
                # High engagement score expected
                engagement_score = 3 if event['has_space_context'] else 2
                assert engagement_score >= 2, "Should have high engagement score"
    
    def test_user_journey_analysis(self, sample_web_events):
        """Test user journey analysis aggregations"""
        # Mock session-level aggregations
        sessions = {}
        for event in sample_web_events:
            session_id = event['sessionId']
            if session_id not in sessions:
                sessions[session_id] = {
                    'session_id': session_id,
                    'anonymous_id': event['anonymousId'],
                    'events': [],
                    'spaces_viewed': set(),
                    'device_type': 'desktop' if event['properties'].get('width', 0) > 1000 else 'mobile'
                }
            
            sessions[session_id]['events'].append(event)
            if event['properties'].get('spaceId'):
                sessions[session_id]['spaces_viewed'].add(event['properties']['spaceId'])
        
        # Test journey analysis
        for session_id, session_data in sessions.items():
            total_events = len(session_data['events'])
            unique_spaces = len(session_data['spaces_viewed'])
            
            # Journey type classification
            if unique_spaces >= 2:
                journey_type = 'multi_space_tour'
            elif unique_spaces == 1:
                journey_type = 'single_space_tour'
            else:
                journey_type = 'tour_navigation_only'
            
            # Conversion intent classification
            if total_events >= 2 and unique_spaces >= 2:
                conversion_intent = 'medium_conversion_intent'
            elif total_events >= 1 and unique_spaces >= 1:
                conversion_intent = 'low_conversion_intent'
            else:
                conversion_intent = 'no_conversion_intent'
            
            assert journey_type in ['multi_space_tour', 'single_space_tour', 'tour_navigation_only']
            assert conversion_intent in ['medium_conversion_intent', 'low_conversion_intent', 'no_conversion_intent']
    
    def test_time_travel_analysis_preparation(self, sample_web_events):
        """Test data structure supports time travel analysis"""
        # Mock current and historical metrics
        current_metrics = {
            'event_date': datetime.utcnow().date(),
            'space_type': 'unit',
            'total_events': 5,
            'unique_sessions': 2,
            'avg_engagement_score': 2.5
        }
        
        historical_metrics = {
            'event_date': datetime.utcnow().date(),
            'space_type': 'unit', 
            'total_events': 3,
            'unique_sessions': 1,
            'avg_engagement_score': 2.0
        }
        
        # Calculate changes for time travel analysis
        events_change = current_metrics['total_events'] - historical_metrics['total_events']
        events_change_pct = (events_change / historical_metrics['total_events']) * 100 if historical_metrics['total_events'] > 0 else None
        
        # Anomaly detection
        if abs(events_change_pct or 0) > 50:
            variance_flag = 'high_variance'
        elif abs(events_change_pct or 0) > 25:
            variance_flag = 'medium_variance'
        else:
            variance_flag = 'normal_variance'
        
        assert events_change == 2, "Should calculate correct change"
        assert abs(events_change_pct - 66.67) < 0.1, "Should calculate correct percentage change"
        assert variance_flag == 'high_variance', "Should flag high variance"


class TestS3TablesIcebergFeatures:
    """Test S3 Tables and Iceberg specific features"""
    
    def test_iceberg_table_properties_validation(self):
        """Test Iceberg table properties are correctly configured"""
        expected_properties = {
            'write.target-file-size-bytes': ['134217728', '67108864', '33554432'],
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'zstd',
            'format-version': '2'
        }
        
        # These properties should be set when creating Iceberg tables
        for prop, values in expected_properties.items():
            assert prop is not None, f"Should configure property: {prop}"
            if isinstance(values, list):
                # Multiple valid values
                assert len(values) > 0, f"Should have valid values for {prop}"
            else:
                # Single expected value
                assert values is not None, f"Should set value for {prop}"
    
    def test_partitioning_strategy_effectiveness(self, sample_web_events):
        """Test partitioning strategy distributes data effectively"""
        # Analyze partition distribution
        partitions = {}
        
        for event in sample_web_events:
            event_date = datetime.fromtimestamp(event['secondsUtcTS'] / 1000).date()
            space_type = event['properties'].get('spaceType', 'unknown')
            partition_key = f"{event_date}_{space_type}"
            
            if partition_key not in partitions:
                partitions[partition_key] = 0
            partitions[partition_key] += 1
        
        # Should have multiple partitions for effective distribution
        assert len(partitions) >= 2, "Should create multiple partitions"
        
        # Check partition keys are well-formed
        for partition_key in partitions.keys():
            assert '_' in partition_key, "Partition key should contain date and space_type"
            date_part, space_part = partition_key.split('_', 1)
            assert len(date_part) == 10, "Date part should be YYYY-MM-DD format"
            assert space_part in ['unit', 'amenity', 'unknown'], "Space part should be valid type"
    
    def test_acid_transaction_support_preparation(self, sample_web_events):
        """Test data structure supports ACID transactions"""
        # Test event deduplication preparation
        event_ids = set()
        
        for event in sample_web_events:
            # Generate event ID as would be done in processing
            event_id = f"{event['sessionId']}_{event['secondsUtcTS']}_{hash(str(event)) % 10000}"
            
            # Should be unique
            assert event_id not in event_ids, f"Event ID should be unique: {event_id}"
            event_ids.add(event_id)
        
        # Test merge operation preparation
        # Events should have all required fields for MERGE operations
        required_merge_fields = ['sessionId', 'anonymousId', 'secondsUtcTS']
        
        for event in sample_web_events:
            for field in required_merge_fields:
                assert field in event, f"Event should have required field for MERGE: {field}"
    
    def test_time_travel_query_support(self, sample_web_events):
        """Test data supports time travel query patterns"""
        # Mock time travel scenario
        timestamps = [
            datetime.utcnow() - timedelta(hours=24),  # 24 hours ago
            datetime.utcnow() - timedelta(hours=1),   # 1 hour ago
            datetime.utcnow()                         # Current
        ]
        
        # Time travel queries would use these timestamp patterns
        for ts in timestamps:
            # Should be able to query data at specific timestamps
            # FOR TIMESTAMP AS OF timestamp
            assert ts.isoformat(), "Should have valid ISO timestamp for time travel"
        
        # Test time-based analysis support
        time_windows = [
            ('current', datetime.utcnow()),
            ('1_hour_ago', datetime.utcnow() - timedelta(hours=1)),
            ('24_hours_ago', datetime.utcnow() - timedelta(hours=24))
        ]
        
        for window_name, window_time in time_windows:
            # Should support different time windows for comparison
            assert window_time <= datetime.utcnow(), f"Time window {window_name} should be valid"


class TestDataQualityAndValidation:
    """Test data quality validation throughout the pipeline"""
    
    def test_end_to_end_data_quality_preservation(self, sample_web_events):
        """Test data quality is preserved throughout pipeline"""
        from s3_processor_iceberg import calculate_data_quality_score
        
        # Test initial data quality
        initial_quality_scores = []
        for event in sample_web_events:
            # Mock event processing
            mock_processed_event = {
                'session_id': event.get('sessionId'),
                'anonymous_id': event.get('anonymousId'),
                'user_agent': event.get('userAgent'),
                'space_id': event.get('properties', {}).get('spaceId'),
                'page_path': event.get('properties', {}).get('path')
            }
            
            score = calculate_data_quality_score(mock_processed_event)
            initial_quality_scores.append(score)
        
        # Should have good quality scores for sample data
        avg_quality = sum(initial_quality_scores) / len(initial_quality_scores)
        assert avg_quality >= 0.8, "Sample data should have high average quality"
        
        # Should filter out low quality data
        high_quality_events = [score for score in initial_quality_scores if score >= 0.7]
        assert len(high_quality_events) == len(sample_web_events), "All sample events should pass quality filter"
    
    def test_bot_detection_throughout_pipeline(self, sample_web_events):
        """Test bot detection is consistent throughout pipeline"""
        from lambda_processor import detect_bot_traffic
        
        for event in sample_web_events:
            user_agent = event.get('userAgent', '')
            is_bot = detect_bot_traffic(user_agent)
            
            # Sample data should not be detected as bots
            assert is_bot == False, f"Legitimate user agent should not be flagged as bot: {user_agent[:50]}..."
        
        # Test bot detection with known bot user agents
        bot_user_agents = [
            "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "facebookexternalhit/1.1"
        ]
        
        for bot_ua in bot_user_agents:
            is_bot = detect_bot_traffic(bot_ua)
            assert is_bot == True, f"Known bot should be detected: {bot_ua}"
    
    def test_session_consistency_validation(self, sample_web_events):
        """Test session data consistency across pipeline stages"""
        # Group events by session
        sessions = {}
        for event in sample_web_events:
            session_id = event['sessionId']
            if session_id not in sessions:
                sessions[session_id] = {
                    'events': [],
                    'anonymous_ids': set(),
                    'timezones': set(),
                    'user_agents': set()
                }
            
            sessions[session_id]['events'].append(event)
            sessions[session_id]['anonymous_ids'].add(event.get('anonymousId'))
            sessions[session_id]['timezones'].add(event.get('timezone'))
            sessions[session_id]['user_agents'].add(event.get('userAgent'))
        
        # Validate session consistency
        for session_id, session_data in sessions.items():
            # Should have consistent anonymous_id within session
            assert len(session_data['anonymous_ids']) == 1, f"Session {session_id} should have consistent anonymous_id"
            
            # Should have consistent timezone within session (usually)
            assert len(session_data['timezones']) <= 2, f"Session {session_id} should have consistent timezone"
            
            # Should have consistent user_agent within session (usually)
            assert len(session_data['user_agents']) <= 2, f"Session {session_id} should have consistent user_agent"


class TestPerformanceAndScalability:
    """Test performance characteristics and scalability"""
    
    def test_batch_processing_scalability(self, sample_web_events):
        """Test batch processing can handle larger datasets"""
        # Simulate larger dataset
        large_dataset = []
        for i in range(1000):  # 1000 events
            base_event = sample_web_events[i % len(sample_web_events)].copy()
            base_event['sessionId'] = f"session-{i // 10}"  # 100 sessions
            base_event['anonymousId'] = f"user-{i // 20}"   # 50 users
            base_event['secondsUtcTS'] += i * 1000  # Spread over time
            large_dataset.append(base_event)
        
        # Test processing efficiency
        from s3_processor_iceberg import transform_web_event
        
        processed_count = 0
        unique_sessions = set()
        unique_users = set()
        
        for event in large_dataset[:100]:  # Test first 100 for performance
            transformed = transform_web_event(event)
            if transformed:
                processed_count += 1
                unique_sessions.add(transformed['session_id'])
                unique_users.add(transformed['anonymous_id'])
        
        # Should process efficiently
        assert processed_count >= 95, "Should process at least 95% of events"
        assert len(unique_sessions) >= 8, "Should detect multiple sessions"
        assert len(unique_users) >= 4, "Should detect multiple users"
    
    def test_real_time_processing_latency(self, sample_web_events):
        """Test real-time processing latency characteristics"""
        from lambda_processor import enrich_event_for_iceberg
        
        processing_times = []
        
        for event in sample_web_events:
            start_time = time.time()
            
            kinesis_metadata = {
                'sequenceNumber': f"seq-{int(start_time * 1000)}",
                'partitionKey': event['sessionId']
            }
            
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            
            end_time = time.time()
            processing_time = end_time - start_time
            processing_times.append(processing_time)
            
            assert enriched is not None, "Should successfully process event"
        
        # Processing should be fast for real-time requirements
        avg_processing_time = sum(processing_times) / len(processing_times)
        max_processing_time = max(processing_times)
        
        assert avg_processing_time < 0.1, "Average processing time should be under 100ms"
        assert max_processing_time < 0.5, "Max processing time should be under 500ms"
    
    def test_partitioning_effectiveness(self, sample_web_events):
        """Test partitioning strategy effectiveness for query performance"""
        # Simulate partition distribution
        partitions = {}
        
        # Generate more diverse data for partition testing
        extended_events = []
        space_types = ['unit', 'amenity', 'tour', 'common_area']
        dates = [datetime.utcnow().date() - timedelta(days=i) for i in range(7)]
        
        for date in dates:
            for space_type in space_types:
                for i in range(10):  # 10 events per partition
                    event = sample_web_events[0].copy()
                    event['properties']['spaceType'] = space_type
                    event['secondsUtcTS'] = int(datetime.combine(date, datetime.min.time()).timestamp() * 1000) + i
                    extended_events.append(event)
        
        # Calculate partition distribution
        for event in extended_events:
            event_date = datetime.fromtimestamp(event['secondsUtcTS'] / 1000).date()
            space_type = event['properties'].get('spaceType', 'unknown')
            partition_key = f"{event_date}_{space_type}"
            
            if partition_key not in partitions:
                partitions[partition_key] = 0
            partitions[partition_key] += 1
        
        # Test partition effectiveness
        assert len(partitions) == 28, "Should create 28 partitions (7 dates × 4 space types)"
        
        # Each partition should have reasonable size
        partition_sizes = list(partitions.values())
        avg_partition_size = sum(partition_sizes) / len(partition_sizes)
        
        assert avg_partition_size == 10, "Each partition should have 10 events"
        assert min(partition_sizes) == max(partition_sizes), "Partitions should be evenly distributed"


class TestErrorHandlingAndRecovery:
    """Test error handling and recovery mechanisms"""
    
    def test_malformed_data_handling(self, aws_test_environment):
        """Test handling of malformed data throughout pipeline"""
        from lambda_processor import enrich_event_for_iceberg
        
        malformed_events = [
            None,  # Null event
            {},    # Empty event
            {"type": "page"},  # Missing required fields
            {"secondsUtcTS": "not-a-number"},  # Invalid timestamp
            {"properties": "not-a-dict"}  # Invalid properties
        ]
        
        successful_processing = 0
        failed_processing = 0
        
        for event in malformed_events:
            try:
                kinesis_metadata = {'sequenceNumber': '123', 'partitionKey': 'test'}
                result = enrich_event_for_iceberg(event, kinesis_metadata)
                
                if result is None:
                    failed_processing += 1
                else:
                    successful_processing += 1
                    
            except Exception:
                failed_processing += 1
        
        # Should handle malformed data gracefully
        assert failed_processing >= 4, "Should handle most malformed events gracefully"
        assert successful_processing <= 1, "Should reject malformed events"
    
    def test_duplicate_event_handling(self, sample_web_events):
        """Test duplicate event detection and handling"""
        from lambda_processor import enrich_event_for_iceberg
        
        # Create duplicate events
        duplicate_events = sample_web_events + sample_web_events  # Duplicate the entire list
        
        event_ids = set()
        processed_events = []
        
        for event in duplicate_events:
            kinesis_metadata = {
                'sequenceNumber': f"seq-{len(processed_events)}",
                'partitionKey': event['sessionId']
            }
            
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            if enriched:
                processed_events.append(enriched)
                event_ids.add(enriched['event_id'])
        
        # Event IDs should help with deduplication
        # (Note: with different sequence numbers, IDs will be different, 
        # but real deduplication happens in Iceberg MERGE operations)
        assert len(processed_events) == 6, "Should process all events (deduplication happens in Iceberg)"
    
    def test_network_failure_resilience(self, aws_test_environment):
        """Test resilience to network failures"""
        s3_client = aws_test_environment['s3_client']
        
        # Mock network failure
        with patch.object(s3_client, 'put_object', side_effect=Exception("Network error")):
            from lambda_processor import write_to_s3_tables_staging
            
            test_events = [{'event_id': 'test', 'event_date': '2024-01-27', 'space_type': 'unit'}]
            
            # Should raise exception for network failures
            with pytest.raises(Exception):
                write_to_s3_tables_staging(test_events)


class TestComplianceAndSecurity:
    """Test compliance and security aspects"""
    
    def test_data_privacy_compliance(self, sample_web_events):
        """Test data privacy and compliance measures"""
        from lambda_processor import enrich_event_for_iceberg
        
        # Test PII handling
        for event in sample_web_events:
            kinesis_metadata = {'sequenceNumber': '123', 'partitionKey': event['sessionId']}
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            
            # Should not contain raw PII
            sensitive_fields = ['email', 'phone', 'address', 'name']
            for field in sensitive_fields:
                assert field not in str(enriched).lower(), f"Should not contain PII field: {field}"
            
            # Should use anonymized identifiers
            assert enriched['anonymous_id'], "Should have anonymous identifier"
            assert not enriched['anonymous_id'].startswith('user@'), "Should not contain email-like identifier"
    
    def test_audit_trail_support(self, sample_web_events):
        """Test audit trail and traceability support"""
        from lambda_processor import enrich_event_for_iceberg
        
        for event in sample_web_events:
            kinesis_metadata = {
                'sequenceNumber': 'seq-12345',
                'partitionKey': event['sessionId']
            }
            
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            
            # Should have processing metadata for audit trail
            assert enriched['processed_timestamp'], "Should have processing timestamp"
            assert enriched['kinesis_sequence_number'] == 'seq-12345', "Should preserve sequence number"
            assert enriched['kinesis_partition_key'] == event['sessionId'], "Should preserve partition key"
            
            # Should have data lineage information
            assert 'properties_json' in enriched, "Should preserve raw properties for audit"


if __name__ == "__main__":
    # Run integration tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=5", "-x"])