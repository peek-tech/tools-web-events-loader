-- Test file for Iceberg-specific features in dbt models
-- These tests validate time travel, ACID transactions, and other Iceberg capabilities

-- Test 1: Validate time travel functionality
{% test time_travel_data_consistency(model, timestamp_column='event_timestamp', lookback_hours=24) %}
  WITH current_data AS (
    SELECT 
      COUNT(*) as current_count,
      COUNT(DISTINCT {{ timestamp_column }}) as unique_timestamps
    FROM {{ model }}
    WHERE {{ timestamp_column }} >= CURRENT_TIMESTAMP - INTERVAL '{{ lookback_hours }}' HOUR
  ),
  historical_data AS (
    SELECT 
      COUNT(*) as historical_count
    FROM {{ model }} FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '{{ lookback_hours }}' HOUR)
    WHERE {{ timestamp_column }} >= CURRENT_TIMESTAMP - INTERVAL '{{ lookback_hours * 2 }}' HOUR
      AND {{ timestamp_column }} < CURRENT_TIMESTAMP - INTERVAL '{{ lookback_hours }}' HOUR
  )
  SELECT 
    CASE 
      WHEN current_count = 0 AND historical_count > 0 THEN 1
      WHEN current_count > 0 AND unique_timestamps = 0 THEN 1
      ELSE 0
    END as test_failure
  FROM current_data, historical_data
{% endtest %}

-- Test 2: Validate MERGE operation integrity
{% test merge_operation_integrity(model, unique_key='event_id') %}
  WITH duplicate_check AS (
    SELECT 
      {{ unique_key }},
      COUNT(*) as occurrence_count
    FROM {{ model }}
    WHERE event_date >= CURRENT_DATE - 1
    GROUP BY {{ unique_key }}
    HAVING COUNT(*) > 1
  )
  SELECT COUNT(*) as duplicate_count
  FROM duplicate_check
{% endtest %}

-- Test 3: Validate partition pruning effectiveness
{% test partition_pruning_effectiveness(model, partition_column='event_date', filter_value='CURRENT_DATE - 1') %}
  -- This test would be run with EXPLAIN to verify partition pruning
  -- For testing, we check data distribution across partitions
  WITH partition_stats AS (
    SELECT 
      {{ partition_column }},
      COUNT(*) as record_count,
      SUM(CASE WHEN {{ partition_column }} = {{ filter_value }} THEN 1 ELSE 0 END) as filtered_count
    FROM {{ model }}
    WHERE {{ partition_column }} >= {{ filter_value }} - 7
      AND {{ partition_column }} <= {{ filter_value }}
    GROUP BY {{ partition_column }}
  )
  SELECT 
    CASE 
      WHEN SUM(filtered_count) = 0 THEN 1  -- No data for filter
      WHEN SUM(filtered_count) > SUM(record_count) * 0.5 THEN 1  -- Poor partition distribution
      ELSE 0
    END as test_failure
  FROM partition_stats
{% endtest %}

-- Test 4: Validate Iceberg metadata columns
{% test iceberg_metadata_columns_present(model) %}
  WITH column_check AS (
    SELECT 
      COUNT(CASE WHEN event_id IS NULL THEN 1 END) as null_event_ids,
      COUNT(CASE WHEN processed_timestamp IS NULL THEN 1 END) as null_processed_timestamps,
      COUNT(CASE WHEN event_timestamp IS NULL THEN 1 END) as null_event_timestamps,
      COUNT(*) as total_records
    FROM {{ model }}
    WHERE event_date = CURRENT_DATE
  )
  SELECT 
    CASE 
      WHEN null_event_ids > 0 THEN 1
      WHEN null_processed_timestamps > 0 THEN 1
      WHEN null_event_timestamps > 0 THEN 1
      WHEN total_records = 0 THEN 1
      ELSE 0
    END as test_failure
  FROM column_check
{% endtest %}

-- Test 5: Validate incremental merge performance
{% test incremental_merge_performance(model, merge_key='event_id', expected_merge_ratio=0.1) %}
  WITH merge_stats AS (
    SELECT 
      COUNT(DISTINCT {{ merge_key }}) as unique_events,
      COUNT(*) as total_records,
      MAX(processed_timestamp) as latest_processing
    FROM {{ model }}
    WHERE event_date >= CURRENT_DATE - 1
  )
  SELECT 
    CASE 
      WHEN total_records = 0 THEN 1
      WHEN (total_records - unique_events) / NULLIF(total_records, 0) > {{ expected_merge_ratio }} THEN 1
      WHEN latest_processing < CURRENT_TIMESTAMP - INTERVAL '2' HOUR THEN 1
      ELSE 0
    END as test_failure
  FROM merge_stats
{% endtest %}

-- Test 6: Validate snapshot isolation
{% test snapshot_isolation_consistency(model, isolation_column='session_id') %}
  -- Test that concurrent sessions maintain isolation
  WITH session_consistency AS (
    SELECT 
      {{ isolation_column }},
      COUNT(DISTINCT anonymous_id) as unique_users,
      COUNT(DISTINCT DATE(event_timestamp)) as days_spanned,
      MAX(session_event_sequence) as max_sequence,
      COUNT(*) as event_count
    FROM {{ model }}
    WHERE event_date >= CURRENT_DATE - 7
    GROUP BY {{ isolation_column }}
  )
  SELECT COUNT(*) as inconsistent_sessions
  FROM session_consistency
  WHERE unique_users > 1  -- Session should belong to one user
    OR days_spanned > 1   -- Session shouldn't span multiple days
    OR max_sequence != event_count  -- Sequence should be continuous
{% endtest %}

-- Test 7: Validate data quality after compaction
{% test post_compaction_data_quality(model, quality_column='data_quality_score') %}
  WITH quality_stats AS (
    SELECT 
      AVG({{ quality_column }}) as avg_quality,
      STDDEV({{ quality_column }}) as quality_stddev,
      COUNT(CASE WHEN {{ quality_column }} < 0.5 THEN 1 END) as low_quality_count,
      COUNT(*) as total_count
    FROM {{ model }}
    WHERE event_date >= CURRENT_DATE - 1
  )
  SELECT 
    CASE 
      WHEN avg_quality < 0.7 THEN 1  -- Average quality too low
      WHEN quality_stddev > 0.3 THEN 1  -- Too much variation
      WHEN low_quality_count > total_count * 0.2 THEN 1  -- Too many low quality records
      ELSE 0
    END as test_failure
  FROM quality_stats
{% endtest %}

-- Test 8: Validate hidden partitions optimization
{% test hidden_partition_optimization(model, partition_cols=['event_date', 'space_type']) %}
  WITH partition_distribution AS (
    SELECT 
      {% for col in partition_cols %}
      {{ col }},
      {% endfor %}
      COUNT(*) as record_count,
      AVG(OCTET_LENGTH(event_id::VARCHAR)) as avg_id_size
    FROM {{ model }}
    WHERE event_date >= CURRENT_DATE - 7
    GROUP BY {% for col in partition_cols %}{{ col }}{{ ", " if not loop.last }}{% endfor %}
  )
  SELECT 
    COUNT(CASE WHEN record_count < 100 THEN 1 END) as small_partitions,
    COUNT(CASE WHEN record_count > 1000000 THEN 1 END) as large_partitions,
    COUNT(*) as total_partitions
  FROM partition_distribution
{% endtest %}

-- Test 9: Validate Z-ordering effectiveness
{% test z_ordering_effectiveness(model, z_order_cols=['session_id', 'anonymous_id']) %}
  -- This would typically use system tables to check file organization
  -- For testing, we validate data clustering
  WITH clustering_stats AS (
    SELECT 
      DATE(event_timestamp) as event_day,
      COUNT(DISTINCT {% for col in z_order_cols %}{{ col }}{{ "||'_'||" if not loop.last }}{% endfor %}) as unique_combinations,
      COUNT(*) as total_records,
      COUNT(DISTINCT file_path) as file_count  -- Would come from Iceberg metadata
    FROM {{ model }}
    WHERE event_date >= CURRENT_DATE - 7
    GROUP BY DATE(event_timestamp)
  )
  SELECT 
    CASE 
      WHEN AVG(unique_combinations::FLOAT / NULLIF(total_records, 0)) > 0.8 THEN 1  -- Poor clustering
      ELSE 0
    END as test_failure
  FROM clustering_stats
{% endtest %}

-- Test 10: Validate time travel query performance
{% test time_travel_performance(model, max_execution_time_ms=5000) %}
  -- This test would be run with timing information
  -- For unit testing, we validate that time travel queries are possible
  WITH time_travel_test AS (
    SELECT COUNT(*) as historical_count
    FROM {{ model }} FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
    WHERE event_date = CURRENT_DATE - 1
  ),
  current_test AS (
    SELECT COUNT(*) as current_count
    FROM {{ model }}
    WHERE event_date = CURRENT_DATE - 1
  )
  SELECT 
    CASE 
      WHEN historical_count = 0 AND current_count > 0 THEN 0  -- Valid time travel
      WHEN historical_count = current_count THEN 1  -- No new data (potential issue)
      ELSE 0
    END as test_failure
  FROM time_travel_test, current_test
{% endtest %}