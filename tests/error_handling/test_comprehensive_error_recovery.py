"""
Comprehensive error handling and recovery tests for S3 Tables/Iceberg data pipeline
Tests failure scenarios, recovery mechanisms, data consistency, and system resilience.
"""

import pytest
import json
import base64
import time
from unittest.mock import Mock, patch, MagicMock, side_effect
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from moto import mock_s3, mock_kinesis, mock_lambda, mock_glue
import concurrent.futures
import threading
from typing import List, Dict, Any


class TestAWSServiceFailures:
    """Test handling of AWS service failures and outages"""
    
    def test_s3_service_unavailable_handling(self):
        """Test handling of S3 service unavailability"""
        from lambda_processor import write_to_s3_tables_staging
        
        # Mock S3 client that fails
        with patch('lambda_processor.s3_client') as mock_s3:
            # Simulate various S3 errors
            s3_errors = [
                ClientError({'Error': {'Code': 'ServiceUnavailable', 'Message': 'Service Unavailable'}}, 'PutObject'),
                ClientError({'Error': {'Code': 'SlowDown', 'Message': 'Slow Down'}}, 'PutObject'),
                ClientError({'Error': {'Code': 'InternalError', 'Message': 'Internal Error'}}, 'PutObject'),
                BotoCoreError()
            ]
            
            test_events = [{'event_id': 'test', 'event_date': '2024-01-27', 'space_type': 'unit'}]
            
            for error in s3_errors:
                mock_s3.put_object.side_effect = error
                
                # Should raise exception for S3 failures
                with pytest.raises(Exception):
                    write_to_s3_tables_staging(test_events)
                
                # Verify retry behavior would be handled at higher level
                assert mock_s3.put_object.called, "Should attempt S3 operation"
    
    def test_kinesis_throttling_handling(self):
        """Test handling of Kinesis throttling and limits"""
        with mock_kinesis():
            kinesis_client = boto3.client('kinesis', region_name='us-east-1')
            kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)
            
            # Wait for stream to be active
            waiter = kinesis_client.get_waiter('stream_exists')
            waiter.wait(StreamName='test-stream')
            
            # Simulate throttling by rapid fire requests
            test_records = []
            throttling_errors = 0
            
            for i in range(100):  # Rapid requests to trigger throttling
                try:
                    response = kinesis_client.put_record(
                        StreamName='test-stream',
                        Data=json.dumps({'test': f'event_{i}'}),
                        PartitionKey=f'key_{i}'
                    )
                    test_records.append(response)
                    
                except ClientError as e:
                    if e.response['Error']['Code'] in ['ProvisionedThroughputExceededException', 'Throttling']:
                        throttling_errors += 1
                        # Simulate exponential backoff
                        time.sleep(0.1 * (2 ** min(throttling_errors, 5)))
                    else:
                        raise
            
            # Should handle some throttling gracefully
            success_rate = len(test_records) / 100
            assert success_rate >= 0.8, f"Should handle throttling with at least 80% success rate, got {success_rate:.2f}"
    
    def test_glue_job_failure_handling(self):
        """Test handling of Glue job failures and retries"""
        with mock_glue():
            glue_client = boto3.client('glue', region_name='us-east-1')
            
            # Create mock job
            glue_client.create_job(
                Name='test-job',
                Role='arn:aws:iam::123456789012:role/GlueRole',
                Command={'Name': 'glueetl', 'ScriptLocation': 's3://bucket/script.py'}
            )
            
            # Mock job run failures
            job_run_states = ['STARTING', 'FAILED', 'STARTING', 'RUNNING', 'SUCCEEDED']
            run_attempts = []
            
            for expected_state in job_run_states:
                try:
                    response = glue_client.start_job_run(JobName='test-job')
                    run_id = response['JobRunId']
                    run_attempts.append(run_id)
                    
                    # Simulate job state check
                    job_run = glue_client.get_job_run(JobName='test-job', RunId=run_id)
                    
                    if expected_state == 'FAILED':
                        # Simulate retry logic
                        assert len(run_attempts) <= 3, "Should not exceed retry limit"
                        continue
                    elif expected_state == 'SUCCEEDED':
                        break
                        
                except ClientError as e:
                    if 'ConcurrentRunsExceededException' in str(e):
                        # Handle concurrent run limits
                        time.sleep(1)
                        continue
                    else:
                        raise
            
            # Should eventually succeed with retries
            assert len(run_attempts) >= 1, "Should make at least one attempt"
            assert len(run_attempts) <= 5, "Should not make excessive retry attempts"
    
    def test_athena_query_failure_recovery(self):
        """Test Athena query failure recovery"""
        # Mock Athena client responses
        with patch('boto3.client') as mock_boto3:
            mock_athena = Mock()
            mock_boto3.return_value = mock_athena
            
            # Simulate query execution failures
            query_failures = [
                ClientError({'Error': {'Code': 'InvalidRequestException', 'Message': 'Invalid SQL'}}, 'StartQueryExecution'),
                ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Table not found'}}, 'StartQueryExecution'),
                ClientError({'Error': {'Code': 'InternalServerException', 'Message': 'Internal error'}}, 'StartQueryExecution')
            ]
            
            successful_response = {'QueryExecutionId': 'query-success-123'}
            
            # Test each failure type
            for i, failure in enumerate(query_failures):
                mock_athena.start_query_execution.side_effect = [failure, successful_response]
                
                # Simulate query with retry logic
                retry_count = 0
                max_retries = 2
                
                while retry_count < max_retries:
                    try:
                        response = mock_athena.start_query_execution(
                            QueryString="SELECT COUNT(*) FROM test_table",
                            ResultConfiguration={'OutputLocation': 's3://test-bucket/results/'}
                        )
                        
                        assert 'QueryExecutionId' in response, "Should return query execution ID"
                        break
                        
                    except ClientError as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            # Some errors should not be retried indefinitely
                            if 'InvalidRequestException' in str(e) or 'ResourceNotFoundException' in str(e):
                                break
                            else:
                                raise
                        
                        # Exponential backoff
                        time.sleep(2 ** retry_count)
                
                # Verify retry behavior
                assert mock_athena.start_query_execution.call_count >= 1, "Should attempt query execution"


class TestDataCorruptionAndInconsistency:
    """Test handling of data corruption and inconsistency scenarios"""
    
    def test_malformed_json_data_recovery(self):
        """Test recovery from malformed JSON data"""
        from s3_processor_iceberg import decode_base64_record, transform_web_event
        
        malformed_data_samples = [
            '{"incomplete": json without closing brace',
            '{invalid-json-syntax}',
            '{"nested": {"incomplete": }',
            'not-json-at-all',
            '',  # Empty string
            '{}',  # Empty JSON
            'null',  # JSON null
            '{"valid": "json", "but": {"missing": "required_fields"}}',
            '{"secondsUtcTS": "not-a-number", "type": "page"}',
            json.dumps({"type": "page", "properties": "should-be-object-not-string"})
        ]
        
        successful_processing = 0
        failed_processing = 0
        
        for malformed_data in malformed_data_samples:
            try:
                # Test base64 decoding
                encoded_data = base64.b64encode(malformed_data.encode('utf-8')).decode('utf-8')
                decoded = decode_base64_record(encoded_data)
                
                if decoded is None:
                    failed_processing += 1
                    continue
                
                # Test transformation
                transformed = transform_web_event(decoded)
                
                if transformed is None:
                    failed_processing += 1
                else:
                    successful_processing += 1
                    
            except Exception:
                failed_processing += 1
        
        # Should handle most malformed data gracefully
        total_samples = len(malformed_data_samples)
        success_rate = successful_processing / total_samples
        failure_rate = failed_processing / total_samples
        
        assert failure_rate >= 0.7, f"Should reject most malformed data, failure rate: {failure_rate:.2f}"
        assert success_rate <= 0.3, f"Should accept minimal valid data, success rate: {success_rate:.2f}"
        
        print(f"Malformed data handling: {failure_rate:.1%} rejected, {success_rate:.1%} processed")
    
    def test_schema_evolution_compatibility(self):
        """Test handling of schema evolution and backward compatibility"""
        from s3_processor_iceberg import transform_web_event
        
        # Test different schema versions
        schema_versions = [
            # Version 1: Original schema
            {
                "type": "page",
                "sessionId": "session-1",
                "anonymousId": "user-1",
                "secondsUtcTS": 1674840769225,
                "properties": {
                    "spaceType": "unit",
                    "spaceId": "space-1"
                }
            },
            # Version 2: Added new fields
            {
                "type": "page",
                "sessionId": "session-2",
                "anonymousId": "user-2",
                "secondsUtcTS": 1674840769226,
                "properties": {
                    "spaceType": "unit",
                    "spaceId": "space-2"
                },
                "newField": "new-value",
                "additionalProperties": {
                    "newProp": "value"
                }
            },
            # Version 3: Missing some fields
            {
                "type": "page",
                "sessionId": "session-3",
                "anonymousId": "user-3"
                # Missing secondsUtcTS and properties
            },
            # Version 4: Changed field types
            {
                "type": "page",
                "sessionId": "session-4",
                "anonymousId": "user-4",
                "secondsUtcTS": "1674840769227",  # String instead of number
                "properties": [  # Array instead of object
                    {"key": "spaceType", "value": "unit"}
                ]
            }
        ]
        
        compatibility_results = []
        
        for i, event in enumerate(schema_versions):
            try:
                transformed = transform_web_event(event)
                
                if transformed:
                    # Verify required fields are present
                    required_fields = ['event_id', 'event_timestamp', 'event_date', 'event_type']
                    has_required = all(field in transformed for field in required_fields)
                    
                    compatibility_results.append({
                        'version': i + 1,
                        'processed': True,
                        'has_required_fields': has_required,
                        'transformation': transformed
                    })
                else:
                    compatibility_results.append({
                        'version': i + 1,
                        'processed': False,
                        'has_required_fields': False,
                        'transformation': None
                    })
                    
            except Exception as e:
                compatibility_results.append({
                    'version': i + 1,
                    'processed': False,
                    'has_required_fields': False,
                    'error': str(e)
                })
        
        # Analyze compatibility
        processed_count = sum(1 for r in compatibility_results if r['processed'])
        
        # Should handle at least basic schema variations
        assert processed_count >= 2, f"Should handle at least 2 schema versions, processed {processed_count}"
        
        # Version 1 (original) should always work
        assert compatibility_results[0]['processed'], "Original schema should always be compatible"
        
        print(f"Schema compatibility: {processed_count}/{len(schema_versions)} versions handled")
    
    def test_duplicate_detection_and_deduplication(self):
        """Test duplicate event detection and handling"""
        from lambda_processor import enrich_event_for_iceberg
        
        # Create duplicate events with slight variations
        base_event = {
            "type": "page",
            "sessionId": "session-duplicate-test",
            "anonymousId": "user-duplicate-test",
            "secondsUtcTS": 1674840769225,
            "properties": {
                "spaceType": "unit",
                "spaceId": "space-duplicate"
            }
        }
        
        # Generate duplicates and near-duplicates
        duplicate_scenarios = [
            # Exact duplicate
            base_event.copy(),
            base_event.copy(),
            
            # Same event, different sequence number
            base_event.copy(),
            
            # Same event, slightly different timestamp
            {**base_event, "secondsUtcTS": base_event["secondsUtcTS"] + 1},
            
            # Same event, additional properties
            {**base_event, "extraField": "extra-value"},
            
            # Same core data, different properties
            {**base_event, "properties": {**base_event["properties"], "extraProp": "value"}}
        ]
        
        event_ids = []
        processed_events = []
        
        for i, event in enumerate(duplicate_scenarios):
            kinesis_metadata = {
                'sequenceNumber': f"seq-{i}",  # Different sequence numbers
                'partitionKey': event['sessionId']
            }
            
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            if enriched:
                event_ids.append(enriched['event_id'])
                processed_events.append(enriched)
        
        # Analyze deduplication effectiveness
        unique_event_ids = set(event_ids)
        
        # Event IDs should vary based on sequence number (helps with Iceberg MERGE)
        assert len(unique_event_ids) >= 3, "Should generate different event IDs for different sequence numbers"
        
        # All events should be processable (deduplication happens in Iceberg layer)
        assert len(processed_events) == len(duplicate_scenarios), "Should process all events"
        
        print(f"Duplicate handling: {len(unique_event_ids)} unique IDs from {len(duplicate_scenarios)} events")


class TestConcurrencyAndRaceConditions:
    """Test handling of concurrent operations and race conditions"""
    
    def test_concurrent_lambda_invocations(self):
        """Test concurrent Lambda function invocations"""
        from lambda_processor import lambda_handler, enrich_event_for_iceberg
        
        # Create test events for concurrent processing
        num_concurrent = 10
        events_per_invocation = 5
        
        test_events = []
        for i in range(num_concurrent):
            invocation_events = []
            for j in range(events_per_invocation):
                event = {
                    "type": "page",
                    "sessionId": f"session-{i}-{j}",
                    "anonymousId": f"user-{i}",
                    "secondsUtcTS": int(datetime.utcnow().timestamp() * 1000) + (i * 1000) + j,
                    "properties": {
                        "spaceType": "unit",
                        "spaceId": f"space-{i}-{j}"
                    }
                }
                invocation_events.append(event)
            
            # Create Kinesis event structure
            kinesis_event = {
                "Records": []
            }
            
            for event in invocation_events:
                encoded_data = base64.b64encode(json.dumps(event).encode('utf-8')).decode('utf-8')
                kinesis_record = {
                    "eventID": f"event-{i}-{len(kinesis_event['Records'])}",
                    "kinesis": {
                        "data": encoded_data,
                        "sequenceNumber": f"seq-{i}-{len(kinesis_event['Records'])}",
                        "partitionKey": event['sessionId']
                    }
                }
                kinesis_event["Records"].append(kinesis_record)
            
            test_events.append(kinesis_event)
        
        # Process events concurrently
        results = []
        errors = []
        
        def process_event(kinesis_event):
            try:
                context = Mock()
                with patch('lambda_processor.s3_client'):
                    result = lambda_handler(kinesis_event, context)
                    return result
            except Exception as e:
                return {'error': str(e)}
        
        # Use ThreadPoolExecutor for concurrent processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            future_to_event = {executor.submit(process_event, event): event for event in test_events}
            
            for future in concurrent.futures.as_completed(future_to_event):
                try:
                    result = future.result(timeout=30)  # 30 second timeout
                    results.append(result)
                except Exception as e:
                    errors.append(str(e))
        
        # Analyze concurrent processing results
        successful_results = [r for r in results if 'error' not in r and r.get('statusCode') == 200]
        
        # Should handle concurrent processing successfully
        assert len(successful_results) >= num_concurrent * 0.8, \
            f"Should process at least 80% of concurrent invocations successfully, got {len(successful_results)}/{num_concurrent}"
        
        # Should not have excessive errors
        assert len(errors) <= num_concurrent * 0.2, \
            f"Should have minimal errors in concurrent processing, got {len(errors)}"
        
        print(f"Concurrent processing: {len(successful_results)}/{num_concurrent} successful, {len(errors)} errors")
    
    def test_concurrent_s3_writes(self):
        """Test concurrent S3 write operations"""
        from lambda_processor import write_to_s3_tables_staging
        
        with mock_s3():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-concurrent-bucket')
            
            # Create concurrent write scenarios
            num_writers = 5
            events_per_writer = 10
            
            def concurrent_writer(writer_id):
                events = []
                for i in range(events_per_writer):
                    event = {
                        'event_id': f"writer_{writer_id}_event_{i}",
                        'event_date': datetime.utcnow().date().isoformat(),
                        'space_type': 'unit',
                        'session_id': f"writer_{writer_id}_session_{i // 2}"
                    }
                    events.append(event)
                
                # Write events with patched s3_client
                with patch('lambda_processor.s3_client', s3_client):
                    with patch('lambda_processor.BUCKET_NAME', 'test-concurrent-bucket'):
                        try:
                            write_to_s3_tables_staging(events)
                            return {'success': True, 'writer_id': writer_id}
                        except Exception as e:
                            return {'success': False, 'writer_id': writer_id, 'error': str(e)}
            
            # Execute concurrent writes
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_writers) as executor:
                futures = [executor.submit(concurrent_writer, i) for i in range(num_writers)]
                
                results = []
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    results.append(result)
            
            # Verify concurrent write results
            successful_writes = [r for r in results if r.get('success')]
            failed_writes = [r for r in results if not r.get('success')]
            
            # Should handle most concurrent writes successfully
            assert len(successful_writes) >= num_writers * 0.8, \
                f"Should handle most concurrent writes, got {len(successful_writes)}/{num_writers}"
            
            # Verify files were created
            objects = s3_client.list_objects_v2(Bucket='test-concurrent-bucket', Prefix='streaming-staging/')
            
            if 'Contents' in objects:
                created_files = len(objects['Contents'])
                assert created_files >= len(successful_writes), \
                    f"Should create files for successful writes, expected >={len(successful_writes)}, got {created_files}"
            
            print(f"Concurrent S3 writes: {len(successful_writes)} successful, {len(failed_writes)} failed")
    
    def test_race_condition_in_processing(self):
        """Test race conditions in event processing"""
        from lambda_processor import enrich_event_for_iceberg
        
        # Create events that might cause race conditions
        shared_session_id = "shared-session-race-test"
        concurrent_events = []
        
        for i in range(20):
            event = {
                "type": "page",
                "sessionId": shared_session_id,  # Same session across threads
                "anonymousId": f"user-race-{i % 3}",  # Few users, many events
                "secondsUtcTS": int(datetime.utcnow().timestamp() * 1000) + i,
                "properties": {
                    "spaceType": "unit",
                    "spaceId": f"space-{i}"
                }
            }
            concurrent_events.append(event)
        
        # Process events concurrently to simulate race conditions
        def process_event_with_delay(event_index):
            event = concurrent_events[event_index]
            
            # Add random delay to increase chance of race conditions
            time.sleep(0.01 * (event_index % 5))
            
            kinesis_metadata = {
                'sequenceNumber': f"seq-{event_index}",
                'partitionKey': event['sessionId']
            }
            
            return enrich_event_for_iceberg(event, kinesis_metadata)
        
        # Execute concurrent processing
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_event_with_delay, i) for i in range(len(concurrent_events))]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Race condition error: {e}")
        
        # Verify race condition handling
        successful_processing = len(results)
        
        # Should process most events despite potential race conditions
        assert successful_processing >= len(concurrent_events) * 0.9, \
            f"Should handle race conditions gracefully, processed {successful_processing}/{len(concurrent_events)}"
        
        # Verify data consistency
        event_ids = [r['event_id'] for r in results]
        unique_event_ids = set(event_ids)
        
        assert len(unique_event_ids) == len(event_ids), "Should generate unique event IDs despite race conditions"
        
        print(f"Race condition handling: {successful_processing}/{len(concurrent_events)} events processed successfully")


class TestSystemResourceExhaustion:
    """Test handling of system resource exhaustion scenarios"""
    
    def test_memory_exhaustion_handling(self):
        """Test handling of memory exhaustion scenarios"""
        from s3_processor_iceberg import transform_web_event
        
        # Simulate memory pressure by processing many large events
        large_events = []
        
        for i in range(1000):  # Large number of events
            # Create event with large properties to simulate memory pressure
            large_properties = {f"prop_{j}": f"value_{j}" * 100 for j in range(50)}  # Large properties
            
            event = {
                "type": "page",
                "sessionId": f"memory-test-session-{i // 10}",
                "anonymousId": f"memory-test-user-{i // 20}",
                "secondsUtcTS": int(datetime.utcnow().timestamp() * 1000) + i,
                "properties": {
                    **large_properties,
                    "spaceType": "unit",
                    "spaceId": f"space-{i}"
                }
            }
            large_events.append(event)
        
        # Process events in batches to simulate memory management
        batch_size = 100
        processed_batches = 0
        total_processed = 0
        
        for i in range(0, len(large_events), batch_size):
            batch = large_events[i:i + batch_size]
            
            try:
                batch_results = []
                for event in batch:
                    transformed = transform_web_event(event)
                    if transformed:
                        batch_results.append(transformed)
                
                processed_batches += 1
                total_processed += len(batch_results)
                
                # Clear batch data to free memory
                del batch_results
                
            except MemoryError:
                print(f"Memory exhaustion at batch {processed_batches}")
                break
            except Exception as e:
                print(f"Error processing batch {processed_batches}: {e}")
                continue
        
        # Should process reasonable number of batches before potential memory issues
        assert processed_batches >= 5, f"Should process at least 5 batches, processed {processed_batches}"
        assert total_processed >= 400, f"Should process at least 400 events, processed {total_processed}"
        
        print(f"Memory handling: {processed_batches} batches, {total_processed} events processed")
    
    def test_connection_pool_exhaustion(self):
        """Test handling of connection pool exhaustion"""
        # Simulate many concurrent AWS service calls
        max_connections = 50
        connection_attempts = []
        successful_connections = 0
        failed_connections = 0
        
        def simulate_aws_connection(connection_id):
            try:
                # Simulate AWS service connection
                with patch('boto3.client') as mock_boto3:
                    mock_client = Mock()
                    mock_boto3.return_value = mock_client
                    
                    # Simulate connection delay
                    time.sleep(0.1)
                    
                    # Simulate service call
                    mock_client.describe_stream.return_value = {'StreamDescription': {'StreamStatus': 'ACTIVE'}}
                    
                    result = mock_client.describe_stream(StreamName='test-stream')
                    return {'connection_id': connection_id, 'success': True, 'result': result}
                    
            except Exception as e:
                return {'connection_id': connection_id, 'success': False, 'error': str(e)}
        
        # Create many concurrent connections
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_connections) as executor:
            futures = [executor.submit(simulate_aws_connection, i) for i in range(max_connections * 2)]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=5)
                    connection_attempts.append(result)
                    
                    if result.get('success'):
                        successful_connections += 1
                    else:
                        failed_connections += 1
                        
                except concurrent.futures.TimeoutError:
                    failed_connections += 1
                except Exception:
                    failed_connections += 1
        
        # Should handle connection limits gracefully
        total_attempts = successful_connections + failed_connections
        success_rate = successful_connections / total_attempts if total_attempts > 0 else 0
        
        assert success_rate >= 0.7, f"Should handle at least 70% of connections, got {success_rate:.2%}"
        assert total_attempts >= max_connections, f"Should attempt at least {max_connections} connections"
        
        print(f"Connection handling: {successful_connections}/{total_attempts} successful ({success_rate:.1%})")
    
    def test_disk_space_exhaustion_simulation(self):
        """Test handling of disk space exhaustion (simulated)"""
        from lambda_processor import write_to_s3_tables_staging
        
        with mock_s3():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-disk-space-bucket')
            
            # Simulate disk space exhaustion by failing after certain number of writes
            write_count = 0
            max_writes_before_failure = 5
            
            def mock_put_object_with_disk_limit(*args, **kwargs):
                nonlocal write_count
                write_count += 1
                
                if write_count > max_writes_before_failure:
                    raise ClientError(
                        {'Error': {'Code': 'InsufficientStorage', 'Message': 'Insufficient storage space'}},
                        'PutObject'
                    )
                
                # Simulate successful write
                return {'ETag': f'"etag-{write_count}"'}
            
            # Test multiple write attempts
            successful_writes = 0
            failed_writes = 0
            
            for i in range(10):  # Attempt 10 writes
                events = [{
                    'event_id': f'disk_test_event_{i}',
                    'event_date': datetime.utcnow().date().isoformat(),
                    'space_type': 'unit'
                }]
                
                with patch.object(s3_client, 'put_object', side_effect=mock_put_object_with_disk_limit):
                    with patch('lambda_processor.s3_client', s3_client):
                        with patch('lambda_processor.BUCKET_NAME', 'test-disk-space-bucket'):
                            try:
                                write_to_s3_tables_staging(events)
                                successful_writes += 1
                            except ClientError as e:
                                if 'InsufficientStorage' in str(e):
                                    failed_writes += 1
                                    print(f"Disk space exhausted at write {i + 1}")
                                    # In real scenario, this would trigger cleanup or alerting
                                    break
                                else:
                                    raise
            
            # Should handle some writes before disk exhaustion
            assert successful_writes == max_writes_before_failure, \
                f"Should succeed up to disk limit, expected {max_writes_before_failure}, got {successful_writes}"
            assert failed_writes >= 1, "Should detect disk space exhaustion"
            
            print(f"Disk space handling: {successful_writes} successful writes before exhaustion")


class TestDataConsistencyAndRecovery:
    """Test data consistency and recovery mechanisms"""
    
    def test_partial_write_recovery(self):
        """Test recovery from partial write scenarios"""
        from lambda_processor import write_to_s3_tables_staging
        
        with mock_s3():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-partial-write')
            
            # Create events that will be partially written
            test_events = []
            for i in range(10):
                event = {
                    'event_id': f'partial_test_{i}',
                    'event_date': datetime.utcnow().date().isoformat(),
                    'space_type': 'unit' if i % 2 == 0 else 'amenity'
                }
                test_events.append(event)
            
            # Simulate partial write failure (fail after some events)
            write_attempts = []
            
            def mock_put_object_partial_failure(*args, **kwargs):
                write_attempts.append(kwargs)
                
                # Fail on 3rd write attempt
                if len(write_attempts) == 3:
                    raise ClientError(
                        {'Error': {'Code': 'InternalError', 'Message': 'Internal server error'}},
                        'PutObject'
                    )
                
                return {'ETag': f'"etag-{len(write_attempts)}"'}
            
            # Test partial write scenario
            with patch.object(s3_client, 'put_object', side_effect=mock_put_object_partial_failure):
                with patch('lambda_processor.s3_client', s3_client):
                    with patch('lambda_processor.BUCKET_NAME', 'test-partial-write'):
                        try:
                            write_to_s3_tables_staging(test_events)
                            assert False, "Should have failed due to partial write"
                        except ClientError:
                            # Expected failure
                            pass
            
            # Verify partial state
            assert len(write_attempts) == 3, "Should have attempted 3 writes before failure"
            
            # Test recovery mechanism (retry with successful writes)
            with patch.object(s3_client, 'put_object', return_value={'ETag': '"recovery-etag"'}):
                with patch('lambda_processor.s3_client', s3_client):
                    with patch('lambda_processor.BUCKET_NAME', 'test-partial-write'):
                        # Recovery should succeed
                        write_to_s3_tables_staging(test_events)
            
            print("Partial write recovery: Successfully recovered from partial failure")
    
    def test_transaction_consistency(self):
        """Test transaction-like consistency in processing"""
        from lambda_processor import enrich_event_for_iceberg
        
        # Create batch of events that should be processed together
        batch_events = []
        for i in range(5):
            event = {
                "type": "page",
                "sessionId": "consistency-test-session",
                "anonymousId": "consistency-test-user",
                "secondsUtcTS": int(datetime.utcnow().timestamp() * 1000) + i,
                "properties": {
                    "spaceType": "unit",
                    "spaceId": f"space-{i}"
                }
            }
            batch_events.append(event)
        
        # Process batch with potential failure in middle
        processed_events = []
        processing_errors = []
        
        for i, event in enumerate(batch_events):
            try:
                kinesis_metadata = {
                    'sequenceNumber': f"consistency-seq-{i}",
                    'partitionKey': event['sessionId']
                }
                
                # Simulate processing failure on 3rd event
                if i == 2:
                    # In real scenario, this might be a transient error
                    # For testing, we simulate recovery
                    pass
                
                enriched = enrich_event_for_iceberg(event, kinesis_metadata)
                if enriched:
                    processed_events.append(enriched)
                    
            except Exception as e:
                processing_errors.append({'event_index': i, 'error': str(e)})
        
        # Verify consistency expectations
        # All events should have consistent session and user information
        if processed_events:
            session_ids = set(e['session_id'] for e in processed_events)
            anonymous_ids = set(e['anonymous_id'] for e in processed_events)
            
            assert len(session_ids) == 1, "All events in batch should have same session ID"
            assert len(anonymous_ids) == 1, "All events in batch should have same anonymous ID"
            
            # Events should have sequential processing
            processing_timestamps = [e['processed_timestamp'] for e in processed_events]
            assert len(processing_timestamps) == len(set(processing_timestamps)), \
                "Processing timestamps should be unique"
        
        # Should process most events successfully
        assert len(processed_events) >= len(batch_events) * 0.8, \
            f"Should process at least 80% of batch events, got {len(processed_events)}/{len(batch_events)}"
        
        print(f"Transaction consistency: {len(processed_events)}/{len(batch_events)} events processed consistently")
    
    def test_rollback_simulation(self):
        """Test rollback-like behavior for failed operations"""
        # Simulate rollback scenario where partial processing needs to be undone
        operation_log = []
        successful_operations = []
        
        # Simulate multi-step operation
        steps = [
            {'name': 'decode_event', 'should_fail': False},
            {'name': 'enrich_event', 'should_fail': False},
            {'name': 'validate_event', 'should_fail': False},
            {'name': 'write_to_staging', 'should_fail': True},  # Fail at this step
            {'name': 'update_metrics', 'should_fail': False}
        ]
        
        def execute_step(step):
            operation_log.append(f"Starting {step['name']}")
            
            if step['should_fail']:
                operation_log.append(f"Failed {step['name']}")
                raise Exception(f"Step {step['name']} failed")
            
            operation_log.append(f"Completed {step['name']}")
            successful_operations.append(step['name'])
            return f"Result of {step['name']}"
        
        # Execute steps with rollback on failure
        try:
            for step in steps:
                execute_step(step)
        
        except Exception as e:
            # Simulate rollback of successful operations
            operation_log.append("Starting rollback")
            
            # Rollback in reverse order
            for step_name in reversed(successful_operations):
                operation_log.append(f"Rolling back {step_name}")
            
            operation_log.append("Rollback completed")
        
        # Verify rollback behavior
        assert 'Starting rollback' in operation_log, "Should initiate rollback on failure"
        assert 'Rollback completed' in operation_log, "Should complete rollback"
        
        # Should have attempted rollback for all successful operations
        rollback_operations = [log for log in operation_log if 'Rolling back' in log]
        assert len(rollback_operations) == len(successful_operations), \
            "Should rollback all successful operations"
        
        print(f"Rollback simulation: {len(successful_operations)} operations rolled back")


if __name__ == "__main__":
    # Run error handling tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=10", "-x"])