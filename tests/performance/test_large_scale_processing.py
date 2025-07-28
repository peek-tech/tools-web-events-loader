"""
Performance tests for large-scale dataset processing and Iceberg time travel queries
Tests 75GB+ dataset processing, concurrent operations, time travel performance, and optimization benefits.
"""

import pytest
import time
import json
import base64
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import concurrent.futures
import threading
import memory_profiler
from typing import List, Dict, Any
import pandas as pd
import numpy as np


class TestLargeDatasetProcessing:
    """Test processing of large datasets (75GB+ virtual tour data)"""
    
    @pytest.fixture
    def large_dataset_generator(self):
        """Generate large dataset for testing"""
        def generate_events(num_events: int = 100000):
            """Generate num_events web events"""
            base_timestamp = int(datetime.utcnow().timestamp() * 1000)
            space_types = ['unit', 'amenity', 'tour', 'common_area']
            device_types = [(1920, 1080, 'desktop'), (375, 812, 'mobile'), (768, 1024, 'tablet')]
            
            events = []
            for i in range(num_events):
                device_width, device_height, device_type = device_types[i % len(device_types)]
                space_type = space_types[i % len(space_types)]
                
                event = {
                    "type": "page" if i % 3 == 0 else "track",
                    "properties": {
                        "title": f"Peek | Virtual Tour | Space {i}",
                        "url": f"https://qa-beta-tour.peek.us/viewer?token=space{i}",
                        "path": "/viewer",
                        "width": device_width,
                        "height": device_height,
                        "spaceName": f"Space {i}",
                        "spaceId": f"space-{i}",
                        "spaceType": space_type,
                        "spaceToken": f"token{i}"
                    },
                    "anonymousId": f"user-{i // 20}",  # 20 events per user on average
                    "secondsUtcTS": base_timestamp + (i * 1000),  # 1 second apart
                    "sessionId": f"session-{i // 10}",  # 10 events per session on average
                    "userAgent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Session-{i}",
                    "timezone": "America/New_York",
                    "appId": "web-viewer-prod",
                    "app": "web-viewer"
                }
                events.append(event)
            
            return events
        
        return generate_events
    
    def test_batch_processing_performance_75gb_equivalent(self, large_dataset_generator):
        """Test batch processing performance with 75GB equivalent dataset"""
        # Generate dataset representing 75GB of data
        # Assuming average event size of ~1KB, 75GB ≈ 75M events
        # For testing, we'll use a smaller representative sample
        num_events = 50000  # Representative sample
        
        events = large_dataset_generator(num_events)
        
        # Test Glue ETL performance
        from s3_processor_iceberg import transform_web_event
        
        start_time = time.time()
        processed_events = []
        batch_size = 1000
        
        # Process in batches to simulate real Glue processing
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            batch_start = time.time()
            
            batch_processed = []
            for event in batch:
                transformed = transform_web_event(event)
                if transformed:
                    batch_processed.append(transformed)
            
            batch_end = time.time()
            batch_time = batch_end - batch_start
            
            processed_events.extend(batch_processed)
            
            # Performance assertions for each batch
            assert batch_time < 10.0, f"Batch processing should complete within 10 seconds, took {batch_time:.2f}s"
            assert len(batch_processed) >= len(batch) * 0.95, "Should process at least 95% of events in batch"
            
            # Memory efficiency check
            if i % (batch_size * 10) == 0:  # Every 10 batches
                print(f"Processed {i + len(batch)} events, batch time: {batch_time:.2f}s")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Overall performance assertions
        assert total_time < 300, f"Large dataset processing should complete within 5 minutes, took {total_time:.2f}s"
        assert len(processed_events) >= num_events * 0.95, "Should process at least 95% of all events"
        
        # Throughput calculation
        throughput = len(processed_events) / total_time
        assert throughput >= 100, f"Should process at least 100 events/second, achieved {throughput:.2f}"
        
        print(f"Performance Summary: {len(processed_events)} events processed in {total_time:.2f}s")
        print(f"Throughput: {throughput:.2f} events/second")
    
    def test_streaming_processing_performance(self, large_dataset_generator):
        """Test real-time streaming processing performance"""
        events = large_dataset_generator(10000)  # 10K events for streaming test
        
        from lambda_processor import enrich_event_for_iceberg
        
        processing_times = []
        memory_usage = []
        
        for i, event in enumerate(events):
            # Measure memory usage periodically
            if i % 1000 == 0:
                current_memory = memory_profiler.memory_usage()[0]
                memory_usage.append(current_memory)
            
            start_time = time.time()
            
            kinesis_metadata = {
                'sequenceNumber': f"seq-{i}",
                'partitionKey': event['sessionId']
            }
            
            enriched = enrich_event_for_iceberg(event, kinesis_metadata)
            
            end_time = time.time()
            processing_time = end_time - start_time
            processing_times.append(processing_time)
            
            assert enriched is not None, f"Should process event {i}"
            
            # Real-time performance requirements
            assert processing_time < 0.1, f"Event {i} processing took {processing_time:.4f}s, should be < 100ms"
        
        # Aggregate performance metrics
        avg_processing_time = sum(processing_times) / len(processing_times)
        p95_processing_time = np.percentile(processing_times, 95)
        p99_processing_time = np.percentile(processing_times, 99)
        
        # Performance assertions
        assert avg_processing_time < 0.05, f"Average processing time should be < 50ms, was {avg_processing_time * 1000:.2f}ms"
        assert p95_processing_time < 0.08, f"95th percentile should be < 80ms, was {p95_processing_time * 1000:.2f}ms"
        assert p99_processing_time < 0.15, f"99th percentile should be < 150ms, was {p99_processing_time * 1000:.2f}ms"
        
        # Memory efficiency
        if memory_usage:
            max_memory = max(memory_usage)
            min_memory = min(memory_usage)
            memory_growth = max_memory - min_memory
            
            assert memory_growth < 100, f"Memory growth should be < 100MB, was {memory_growth:.2f}MB"
        
        print(f"Streaming Performance Summary:")
        print(f"Average processing time: {avg_processing_time * 1000:.2f}ms")
        print(f"95th percentile: {p95_processing_time * 1000:.2f}ms")
        print(f"99th percentile: {p99_processing_time * 1000:.2f}ms")
    
    def test_concurrent_processing_performance(self, large_dataset_generator):
        """Test concurrent processing with multiple workers"""
        events = large_dataset_generator(20000)  # 20K events
        
        from s3_processor_iceberg import transform_web_event
        
        # Test different levels of concurrency
        concurrency_levels = [1, 2, 4, 8]
        results = {}
        
        for num_workers in concurrency_levels:
            start_time = time.time()
            
            # Split events among workers
            chunk_size = len(events) // num_workers
            event_chunks = [events[i:i + chunk_size] for i in range(0, len(events), chunk_size)]
            
            processed_events = []
            
            def process_chunk(chunk):
                chunk_results = []
                for event in chunk:
                    transformed = transform_web_event(event)
                    if transformed:
                        chunk_results.append(transformed)
                return chunk_results
            
            # Process chunks concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_chunk = {executor.submit(process_chunk, chunk): chunk for chunk in event_chunks}
                
                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_results = future.result()
                    processed_events.extend(chunk_results)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            results[num_workers] = {
                'time': total_time,
                'throughput': len(processed_events) / total_time,
                'processed_count': len(processed_events)
            }
            
            # Basic performance assertions
            assert len(processed_events) >= len(events) * 0.95, f"Should process 95% of events with {num_workers} workers"
            assert total_time < 120, f"Processing with {num_workers} workers should complete within 2 minutes"
        
        # Analyze concurrency benefits
        single_worker_time = results[1]['time']
        
        for workers in [2, 4, 8]:
            if workers in results:
                speedup = single_worker_time / results[workers]['time']
                efficiency = speedup / workers
                
                print(f"{workers} workers: {speedup:.2f}x speedup, {efficiency:.2f} efficiency")
                
                # Should see some speedup with concurrency
                assert speedup > 1.2, f"Should see at least 20% speedup with {workers} workers"


class TestIcebergTimeTravel:
    """Test Iceberg time travel query performance"""
    
    @pytest.fixture
    def historical_data_generator(self):
        """Generate historical data snapshots for time travel testing"""
        def generate_snapshots(num_snapshots: int = 24, events_per_snapshot: int = 1000):
            """Generate num_snapshots with events_per_snapshot each"""
            snapshots = []
            base_timestamp = datetime.utcnow() - timedelta(hours=num_snapshots)
            
            for snapshot_hour in range(num_snapshots):
                snapshot_time = base_timestamp + timedelta(hours=snapshot_hour)
                snapshot_events = []
                
                for i in range(events_per_snapshot):
                    event_timestamp = snapshot_time + timedelta(seconds=i)
                    
                    event = {
                        'event_id': f"event_{snapshot_hour}_{i}",
                        'event_timestamp': event_timestamp,
                        'event_date': event_timestamp.date(),
                        'event_type': 'page',
                        'session_id': f"session_{snapshot_hour}_{i // 10}",
                        'anonymous_id': f"user_{i // 20}",
                        'space_type': ['unit', 'amenity'][i % 2],
                        'data_quality_score': 0.9,
                        'engagement_score': 2.0 + (i % 3),
                        'is_core_virtual_tour_event': True
                    }
                    snapshot_events.append(event)
                
                snapshots.append({
                    'timestamp': snapshot_time,
                    'events': snapshot_events
                })
            
            return snapshots
        
        return generate_snapshots
    
    def test_time_travel_query_performance(self, historical_data_generator):
        """Test time travel query performance across multiple snapshots"""
        snapshots = historical_data_generator(24, 5000)  # 24 hours, 5K events each
        
        # Simulate time travel queries
        query_times = []
        
        # Test queries at different time points
        query_timestamps = [
            datetime.utcnow() - timedelta(hours=1),   # 1 hour ago
            datetime.utcnow() - timedelta(hours=6),   # 6 hours ago
            datetime.utcnow() - timedelta(hours=12),  # 12 hours ago
            datetime.utcnow() - timedelta(hours=24),  # 24 hours ago
        ]
        
        for query_timestamp in query_timestamps:
            start_time = time.time()
            
            # Simulate "SELECT COUNT(*) FROM table FOR TIMESTAMP AS OF query_timestamp"
            # Find the appropriate snapshot
            relevant_snapshots = [s for s in snapshots if s['timestamp'] <= query_timestamp]
            
            if relevant_snapshots:
                target_snapshot = max(relevant_snapshots, key=lambda x: x['timestamp'])
                
                # Simulate query processing
                result_count = len(target_snapshot['events'])
                
                # Simulate aggregation queries
                space_type_counts = {}
                engagement_sum = 0
                
                for event in target_snapshot['events']:
                    space_type = event['space_type']
                    space_type_counts[space_type] = space_type_counts.get(space_type, 0) + 1
                    engagement_sum += event['engagement_score']
                
                avg_engagement = engagement_sum / len(target_snapshot['events'])
                
                # Query should return valid results
                assert result_count > 0, "Time travel query should return results"
                assert len(space_type_counts) > 0, "Should have space type aggregations"
                assert avg_engagement > 0, "Should calculate average engagement"
            
            end_time = time.time()
            query_time = end_time - start_time
            query_times.append(query_time)
            
            # Time travel queries should be fast
            assert query_time < 1.0, f"Time travel query should complete within 1 second, took {query_time:.3f}s"
        
        # Overall time travel performance
        avg_query_time = sum(query_times) / len(query_times)
        max_query_time = max(query_times)
        
        assert avg_query_time < 0.5, f"Average time travel query time should be < 500ms, was {avg_query_time * 1000:.2f}ms"
        assert max_query_time < 1.0, f"Max time travel query time should be < 1s, was {max_query_time:.3f}s"
        
        print(f"Time Travel Performance:")
        print(f"Average query time: {avg_query_time * 1000:.2f}ms")
        print(f"Max query time: {max_query_time * 1000:.2f}ms")
    
    def test_time_travel_comparison_queries(self, historical_data_generator):
        """Test performance of time travel comparison queries"""
        snapshots = historical_data_generator(48, 2000)  # 48 hours, 2K events each
        
        # Test comparison queries between different time points
        comparison_pairs = [
            (datetime.utcnow(), datetime.utcnow() - timedelta(hours=1)),    # Current vs 1h ago
            (datetime.utcnow(), datetime.utcnow() - timedelta(hours=24)),   # Current vs 1d ago
            (datetime.utcnow() - timedelta(hours=12), datetime.utcnow() - timedelta(hours=36)),  # 12h vs 36h ago
        ]
        
        comparison_times = []
        
        for current_time, historical_time in comparison_pairs:
            start_time = time.time()
            
            # Get current snapshot
            current_snapshots = [s for s in snapshots if s['timestamp'] <= current_time]
            current_snapshot = max(current_snapshots, key=lambda x: x['timestamp']) if current_snapshots else None
            
            # Get historical snapshot
            historical_snapshots = [s for s in snapshots if s['timestamp'] <= historical_time]
            historical_snapshot = max(historical_snapshots, key=lambda x: x['timestamp']) if historical_snapshots else None
            
            if current_snapshot and historical_snapshot:
                # Calculate metrics for comparison
                current_metrics = {
                    'total_events': len(current_snapshot['events']),
                    'unique_sessions': len(set(e['session_id'] for e in current_snapshot['events'])),
                    'avg_engagement': sum(e['engagement_score'] for e in current_snapshot['events']) / len(current_snapshot['events'])
                }
                
                historical_metrics = {
                    'total_events': len(historical_snapshot['events']),
                    'unique_sessions': len(set(e['session_id'] for e in historical_snapshot['events'])),
                    'avg_engagement': sum(e['engagement_score'] for e in historical_snapshot['events']) / len(historical_snapshot['events'])
                }
                
                # Calculate changes
                events_change = current_metrics['total_events'] - historical_metrics['total_events']
                sessions_change = current_metrics['unique_sessions'] - historical_metrics['unique_sessions']
                engagement_change = current_metrics['avg_engagement'] - historical_metrics['avg_engagement']
                
                # Validate comparison results
                assert isinstance(events_change, int), "Events change should be integer"
                assert isinstance(sessions_change, int), "Sessions change should be integer"
                assert isinstance(engagement_change, float), "Engagement change should be float"
            
            end_time = time.time()
            comparison_time = end_time - start_time
            comparison_times.append(comparison_time)
            
            # Comparison queries should be efficient
            assert comparison_time < 2.0, f"Time travel comparison should complete within 2 seconds, took {comparison_time:.3f}s"
        
        # Overall comparison performance
        avg_comparison_time = sum(comparison_times) / len(comparison_times)
        
        assert avg_comparison_time < 1.0, f"Average comparison time should be < 1s, was {avg_comparison_time:.3f}s"
        
        print(f"Time Travel Comparison Performance: {avg_comparison_time * 1000:.2f}ms average")


class TestDataCompactionOptimization:
    """Test Iceberg data compaction and optimization performance"""
    
    def test_small_files_compaction_performance(self):
        """Test performance of small files compaction"""
        # Simulate many small files scenario
        num_small_files = 1000
        events_per_file = 100
        
        # Generate small file data
        small_files_data = []
        for file_idx in range(num_small_files):
            file_events = []
            for event_idx in range(events_per_file):
                event = {
                    'event_id': f"file_{file_idx}_event_{event_idx}",
                    'event_date': (datetime.utcnow() - timedelta(days=file_idx % 7)).date(),
                    'space_type': ['unit', 'amenity'][event_idx % 2],
                    'data_size_kb': 1  # Simulate 1KB per event
                }
                file_events.append(event)
            
            small_files_data.append({
                'file_id': f"file_{file_idx}",
                'size_kb': len(file_events),  # 100KB per file
                'events': file_events
            })
        
        # Simulate compaction process
        start_time = time.time()
        
        # Group files by partition for compaction
        partitions = {}
        for file_data in small_files_data:
            for event in file_data['events']:
                partition_key = f"{event['event_date']}_{event['space_type']}"
                if partition_key not in partitions:
                    partitions[partition_key] = []
                partitions[partition_key].append(event)
        
        # Simulate compaction into larger files (target 128MB)
        target_file_size_kb = 128 * 1024  # 128MB
        compacted_files = []
        
        for partition_key, partition_events in partitions.items():
            current_file_events = []
            current_file_size = 0
            
            for event in partition_events:
                current_file_events.append(event)
                current_file_size += event['data_size_kb']
                
                if current_file_size >= target_file_size_kb:
                    compacted_files.append({
                        'partition': partition_key,
                        'size_kb': current_file_size,
                        'events': current_file_events
                    })
                    current_file_events = []
                    current_file_size = 0
            
            # Add remaining events
            if current_file_events:
                compacted_files.append({
                    'partition': partition_key,
                    'size_kb': current_file_size,
                    'events': current_file_events
                })
        
        end_time = time.time()
        compaction_time = end_time - start_time
        
        # Validate compaction results
        original_file_count = num_small_files
        compacted_file_count = len(compacted_files)
        compression_ratio = original_file_count / compacted_file_count if compacted_file_count > 0 else 0
        
        # Performance assertions
        assert compaction_time < 30, f"Compaction should complete within 30 seconds, took {compaction_time:.2f}s"
        assert compression_ratio >= 2, f"Should reduce file count by at least 50%, ratio: {compression_ratio:.2f}"
        
        # Verify data integrity
        original_event_count = sum(len(f['events']) for f in small_files_data)
        compacted_event_count = sum(len(f['events']) for f in compacted_files)
        
        assert compacted_event_count == original_event_count, "Should preserve all events during compaction"
        
        print(f"Compaction Performance:")
        print(f"Original files: {original_file_count}, Compacted files: {compacted_file_count}")
        print(f"Compression ratio: {compression_ratio:.2f}x")
        print(f"Compaction time: {compaction_time:.2f}s")
    
    def test_partition_optimization_performance(self):
        """Test performance of partition-level optimizations"""
        # Generate data across multiple partitions
        num_partitions = 100
        events_per_partition = 10000
        
        start_time = time.time()
        
        partition_metrics = []
        
        for partition_idx in range(num_partitions):
            partition_start = time.time()
            
            # Simulate partition data
            partition_date = (datetime.utcnow() - timedelta(days=partition_idx % 30)).date()
            space_type = ['unit', 'amenity', 'tour'][partition_idx % 3]
            
            # Generate events for this partition
            events = []
            for event_idx in range(events_per_partition):
                event = {
                    'event_id': f"p{partition_idx}_e{event_idx}",
                    'event_date': partition_date,
                    'space_type': space_type,
                    'event_timestamp': datetime.combine(partition_date, datetime.min.time()) + timedelta(seconds=event_idx),
                    'engagement_score': 1 + (event_idx % 4)
                }
                events.append(event)
            
            # Simulate partition-level optimizations
            # 1. Sort by timestamp for better compression
            events.sort(key=lambda x: x['event_timestamp'])
            
            # 2. Calculate partition statistics
            min_timestamp = min(e['event_timestamp'] for e in events)
            max_timestamp = max(e['event_timestamp'] for e in events)
            avg_engagement = sum(e['engagement_score'] for e in events) / len(events)
            
            partition_end = time.time()
            partition_time = partition_end - partition_start
            
            partition_metrics.append({
                'partition_id': f"{partition_date}_{space_type}",
                'event_count': len(events),
                'processing_time': partition_time,
                'avg_engagement': avg_engagement
            })
            
            # Each partition should process efficiently
            assert partition_time < 1.0, f"Partition {partition_idx} should process within 1 second, took {partition_time:.3f}s"
        
        end_time = time.time()
        total_optimization_time = end_time - start_time
        
        # Overall optimization performance
        total_events = num_partitions * events_per_partition
        throughput = total_events / total_optimization_time
        
        assert total_optimization_time < 60, f"Partition optimization should complete within 1 minute, took {total_optimization_time:.2f}s"
        assert throughput >= 10000, f"Should process at least 10K events/second, achieved {throughput:.2f}"
        
        # Analyze partition efficiency
        avg_partition_time = sum(p['processing_time'] for p in partition_metrics) / len(partition_metrics)
        max_partition_time = max(p['processing_time'] for p in partition_metrics)
        
        assert avg_partition_time < 0.5, f"Average partition processing should be < 500ms, was {avg_partition_time * 1000:.2f}ms"
        assert max_partition_time < 1.0, f"Max partition processing should be < 1s, was {max_partition_time:.3f}s"
        
        print(f"Partition Optimization Performance:")
        print(f"Total events: {total_events:,}")
        print(f"Total time: {total_optimization_time:.2f}s")
        print(f"Throughput: {throughput:,.0f} events/second")
        print(f"Average partition time: {avg_partition_time * 1000:.2f}ms")


class TestQueryPerformanceOptimization:
    """Test query performance with Iceberg optimizations"""
    
    def test_predicate_pushdown_performance(self):
        """Test predicate pushdown optimization performance"""
        # Generate test dataset with known patterns
        num_events = 100000
        events = []
        
        for i in range(num_events):
            event_date = (datetime.utcnow() - timedelta(days=i % 30)).date()
            space_type = ['unit', 'amenity', 'tour'][i % 3]
            
            event = {
                'event_id': f"event_{i}",
                'event_date': event_date,
                'event_timestamp': datetime.combine(event_date, datetime.min.time()) + timedelta(seconds=i),
                'space_type': space_type,
                'anonymous_id': f"user_{i // 100}",  # 100 events per user
                'session_id': f"session_{i // 10}",   # 10 events per session
                'engagement_score': 1 + (i % 5),
                'data_quality_score': 0.8 + (i % 3) * 0.1
            }
            events.append(event)
        
        # Test different query patterns with predicate pushdown
        query_patterns = [
            {
                'name': 'date_filter',
                'predicate': lambda e: e['event_date'] >= (datetime.utcnow() - timedelta(days=7)).date(),
                'expected_reduction': 0.7  # Should filter out ~70% of data
            },
            {
                'name': 'space_type_filter',
                'predicate': lambda e: e['space_type'] == 'unit',
                'expected_reduction': 0.67  # Should filter out ~67% of data (2/3)
            },
            {
                'name': 'quality_filter',
                'predicate': lambda e: e['data_quality_score'] >= 0.9,
                'expected_reduction': 0.67  # Should filter out ~67% of data
            },
            {
                'name': 'combined_filter',
                'predicate': lambda e: (e['event_date'] >= (datetime.utcnow() - timedelta(days=7)).date() and 
                                      e['space_type'] == 'unit' and 
                                      e['data_quality_score'] >= 0.9),
                'expected_reduction': 0.9  # Should filter out ~90% of data
            }
        ]
        
        for pattern in query_patterns:
            start_time = time.time()
            
            # Simulate predicate pushdown (filter early)
            filtered_events = [e for e in events if pattern['predicate'](e)]
            
            # Simulate aggregation on filtered data
            if filtered_events:
                total_events = len(filtered_events)
                unique_users = len(set(e['anonymous_id'] for e in filtered_events))
                avg_engagement = sum(e['engagement_score'] for e in filtered_events) / len(filtered_events)
            else:
                total_events = unique_users = avg_engagement = 0
            
            end_time = time.time()
            query_time = end_time - start_time
            
            # Calculate actual reduction
            actual_reduction = 1 - (len(filtered_events) / len(events))
            
            # Performance assertions
            assert query_time < 1.0, f"Query '{pattern['name']}' should complete within 1 second, took {query_time:.3f}s"
            
            # Verify predicate effectiveness
            expected_reduction = pattern['expected_reduction']
            assert abs(actual_reduction - expected_reduction) < 0.1, \
                f"Query '{pattern['name']}' should reduce data by ~{expected_reduction:.0%}, actual: {actual_reduction:.0%}"
            
            print(f"Query '{pattern['name']}': {query_time * 1000:.2f}ms, reduced data by {actual_reduction:.0%}")
    
    def test_columnar_scan_performance(self):
        """Test columnar scan performance benefits"""
        # Generate wide table data
        num_events = 50000
        events = []
        
        for i in range(num_events):
            # Simulate wide table with many columns
            event = {
                'event_id': f"event_{i}",
                'event_timestamp': datetime.utcnow() + timedelta(seconds=i),
                'event_date': datetime.utcnow().date(),
                'event_type': 'page',
                'anonymous_id': f"user_{i // 100}",
                'session_id': f"session_{i // 10}",
                'space_type': ['unit', 'amenity'][i % 2],
                'engagement_score': 1 + (i % 5),
                # Many additional columns that might not be needed for specific queries
                'unused_col_1': f"data_{i}",
                'unused_col_2': f"more_data_{i}",
                'unused_col_3': f"extra_data_{i}",
                'unused_col_4': i * 1.5,
                'unused_col_5': i * 2.0,
                'page_url': f"https://example.com/page_{i}",
                'user_agent': f"Browser_{i}",
                'properties_json': json.dumps({'prop1': i, 'prop2': f"value_{i}"})
            }
            events.append(event)
        
        # Test column-specific queries (simulating columnar benefits)
        column_queries = [
            {
                'name': 'select_few_columns',
                'columns': ['event_id', 'event_timestamp', 'engagement_score'],
                'expected_speedup': 2.0  # Should be ~2x faster than full scan
            },
            {
                'name': 'aggregation_query',
                'columns': ['space_type', 'engagement_score'],
                'expected_speedup': 3.0  # Should be ~3x faster for aggregation
            },
            {
                'name': 'count_query',
                'columns': ['event_id'],
                'expected_speedup': 5.0  # Should be ~5x faster for count
            }
        ]
        
        # Baseline: full table scan
        start_time = time.time()
        full_scan_results = []
        for event in events:
            # Simulate reading all columns
            result = {k: v for k, v in event.items()}
            full_scan_results.append(result)
        end_time = time.time()
        full_scan_time = end_time - start_time
        
        # Test column-specific queries
        for query in column_queries:
            start_time = time.time()
            
            column_results = []
            for event in events:
                # Simulate reading only specific columns
                result = {col: event[col] for col in query['columns'] if col in event}
                column_results.append(result)
            
            # Simulate aggregation if applicable
            if query['name'] == 'aggregation_query':
                space_type_engagement = {}
                for result in column_results:
                    space_type = result['space_type']
                    if space_type not in space_type_engagement:
                        space_type_engagement[space_type] = []
                    space_type_engagement[space_type].append(result['engagement_score'])
                
                # Calculate averages
                for space_type in space_type_engagement:
                    avg_engagement = sum(space_type_engagement[space_type]) / len(space_type_engagement[space_type])
            
            elif query['name'] == 'count_query':
                count_result = len(column_results)
            
            end_time = time.time()
            column_query_time = end_time - start_time
            
            # Calculate actual speedup
            actual_speedup = full_scan_time / column_query_time if column_query_time > 0 else 0
            expected_speedup = query['expected_speedup']
            
            # Performance assertions
            assert column_query_time < full_scan_time, f"Column query '{query['name']}' should be faster than full scan"
            assert actual_speedup >= expected_speedup * 0.5, \
                f"Column query '{query['name']}' should achieve at least 50% of expected speedup"
            
            print(f"Column query '{query['name']}': {actual_speedup:.2f}x speedup (expected {expected_speedup:.1f}x)")


class TestMemoryAndResourceUsage:
    """Test memory usage and resource efficiency"""
    
    @pytest.mark.skipif(not hasattr(memory_profiler, 'memory_usage'), reason="memory_profiler required")
    def test_memory_efficiency_large_dataset(self):
        """Test memory efficiency with large datasets"""
        from s3_processor_iceberg import transform_web_event
        
        # Monitor memory usage during processing
        initial_memory = memory_profiler.memory_usage()[0]
        peak_memories = []
        
        # Process in chunks to test memory efficiency
        chunk_size = 5000
        total_events = 50000
        
        for chunk_start in range(0, total_events, chunk_size):
            chunk_events = []
            
            # Generate chunk of events
            for i in range(chunk_start, min(chunk_start + chunk_size, total_events)):
                event = {
                    "type": "page",
                    "properties": {
                        "spaceId": f"space-{i}",
                        "spaceType": "unit",
                        "width": 1920,
                        "height": 1080
                    },
                    "anonymousId": f"user-{i // 100}",
                    "sessionId": f"session-{i // 10}",
                    "secondsUtcTS": int(datetime.utcnow().timestamp() * 1000) + i,
                    "userAgent": "Mozilla/5.0 Test"
                }
                chunk_events.append(event)
            
            # Process chunk
            processed_chunk = []
            for event in chunk_events:
                transformed = transform_web_event(event)
                if transformed:
                    processed_chunk.append(transformed)
            
            # Monitor memory after processing chunk
            current_memory = memory_profiler.memory_usage()[0]
            peak_memories.append(current_memory)
            
            # Clean up chunk data
            del chunk_events
            del processed_chunk
        
        final_memory = memory_profiler.memory_usage()[0]
        max_memory = max(peak_memories)
        memory_growth = max_memory - initial_memory
        
        # Memory efficiency assertions
        assert memory_growth < 500, f"Memory growth should be < 500MB, was {memory_growth:.2f}MB"
        assert final_memory - initial_memory < 100, f"Final memory should be close to initial, difference: {final_memory - initial_memory:.2f}MB"
        
        print(f"Memory Usage:")
        print(f"Initial: {initial_memory:.2f}MB")
        print(f"Peak: {max_memory:.2f}MB")
        print(f"Final: {final_memory:.2f}MB")
        print(f"Max growth: {memory_growth:.2f}MB")
    
    def test_cpu_utilization_efficiency(self):
        """Test CPU utilization efficiency"""
        from s3_processor_iceberg import transform_web_event
        import psutil
        import threading
        
        # Monitor CPU usage during processing
        cpu_usage = []
        monitoring = True
        
        def monitor_cpu():
            while monitoring:
                cpu_percent = psutil.cpu_percent(interval=0.1)
                cpu_usage.append(cpu_percent)
                time.sleep(0.1)
        
        # Start CPU monitoring
        monitor_thread = threading.Thread(target=monitor_cpu)
        monitor_thread.start()
        
        # Generate and process events
        num_events = 20000
        start_time = time.time()
        
        processed_count = 0
        for i in range(num_events):
            event = {
                "type": "page",
                "properties": {
                    "spaceId": f"space-{i}",
                    "spaceType": "unit",
                    "width": 1920
                },
                "anonymousId": f"user-{i // 100}",
                "sessionId": f"session-{i // 10}",
                "secondsUtcTS": int(datetime.utcnow().timestamp() * 1000) + i
            }
            
            transformed = transform_web_event(event)
            if transformed:
                processed_count += 1
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Stop monitoring
        monitoring = False
        monitor_thread.join()
        
        # Analyze CPU usage
        if cpu_usage:
            avg_cpu = sum(cpu_usage) / len(cpu_usage)
            max_cpu = max(cpu_usage)
            
            # CPU efficiency assertions
            assert avg_cpu > 10, f"Should utilize CPU effectively, average: {avg_cpu:.1f}%"
            assert avg_cpu < 90, f"Should not max out CPU, average: {avg_cpu:.1f}%"
            assert max_cpu < 100, f"Should not completely max out CPU, max: {max_cpu:.1f}%"
            
            print(f"CPU Utilization:")
            print(f"Average: {avg_cpu:.1f}%")
            print(f"Maximum: {max_cpu:.1f}%")
            print(f"Processing rate: {processed_count / processing_time:.0f} events/second")


if __name__ == "__main__":
    # Run performance tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "-x", "--maxfail=3"])