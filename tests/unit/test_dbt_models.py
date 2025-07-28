"""
Unit tests for dbt analytics models with S3 Tables/Iceberg integration
Tests SQL logic, transformations, materialization strategies, and Iceberg-specific features.
"""

import pytest
import re
from unittest.mock import Mock, patch
from pathlib import Path
import yaml


class TestDbtModelSQLLogic:
    """Test SQL logic in dbt models for correctness and Iceberg compatibility"""
    
    def setup_method(self):
        """Setup test environment"""
        # Read the actual SQL files
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
        
        # Load fact table model
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            self.fact_model_sql = f.read()
        
        # Load user journey model
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            self.journey_model_sql = f.read()
        
        # Load time travel model
        time_travel_path = self.models_path / "marts/analytics/time_travel_analysis.sql"
        with open(time_travel_path, 'r') as f:
            self.time_travel_sql = f.read()
    
    def test_fact_model_iceberg_configuration(self):
        """Test that fact model has correct Iceberg configuration"""
        # Check for Iceberg-specific config
        assert "table_type='iceberg'" in self.fact_model_sql, "Should specify Iceberg table type"
        assert "partitioned_by=['event_date', 'space_type']" in self.fact_model_sql, "Should partition correctly"
        
        # Check for Iceberg table properties
        iceberg_properties = [
            'write.target-file-size-bytes',
            'write.format.default',
            'write.parquet.compression-codec'
        ]
        
        for prop in iceberg_properties:
            assert prop in self.fact_model_sql, f"Should include Iceberg property: {prop}"
        
        # Check for merge mode configuration
        assert 'merge-on-read' in self.fact_model_sql, "Should use merge-on-read for streaming"
    
    def test_fact_model_unique_key_specification(self):
        """Test that fact model specifies unique key for deduplication"""
        assert "unique_key='event_id'" in self.fact_model_sql, "Should specify event_id as unique key"
    
    def test_fact_model_space_categorization_logic(self):
        """Test space categorization CASE statement logic"""
        # Check for comprehensive space type mapping
        space_mappings = [
            ("space_type = 'unit'", 'residential_unit'),
            ("space_type IN ('common_area', 'amenity')", 'shared_space'),
            ("space_type = 'tour'", 'guided_experience'),
            ("space_type IS NULL AND is_virtual_tour_page", 'tour_navigation'),
            ("space_type = 'unknown' AND page_path LIKE '%viewer%'", 'unclassified_space')
        ]
        
        for condition, expected_category in space_mappings:
            # Check that the condition exists in the CASE statement
            assert condition in self.fact_model_sql or condition.replace("'", '"') in self.fact_model_sql
            assert expected_category in self.fact_model_sql
    
    def test_fact_model_engagement_scoring(self):
        """Test engagement scoring logic"""
        # Check for engagement level scoring
        engagement_conditions = [
            "engagement_level = 'engaged' AND has_space_context THEN 3",
            "engagement_level = 'engaged' THEN 2",
            "engagement_level = 'moderate' THEN 1"
        ]
        
        for condition in engagement_conditions:
            assert condition in self.fact_model_sql, f"Should include engagement condition: {condition}"
    
    def test_fact_model_journey_milestone_detection(self):
        """Test journey milestone detection logic"""
        milestone_patterns = [
            'external_referral_entry',
            'internal_referral_entry', 
            'direct_entry',
            'early_space_exploration',
            'deep_space_exploration',
            'engaged_exit',
            'quick_exit'
        ]
        
        for pattern in milestone_patterns:
            assert pattern in self.fact_model_sql, f"Should detect milestone: {pattern}"
    
    def test_fact_model_time_period_classification(self):
        """Test time-based classification logic"""
        time_conditions = [
            "event_hour BETWEEN 9 AND 17 THEN 'business_hours'",
            "event_hour BETWEEN 18 AND 22 THEN 'evening'",
            "event_hour BETWEEN 6 AND 8 THEN 'morning'"
        ]
        
        for condition in time_conditions:
            assert condition in self.fact_model_sql, f"Should include time condition: {condition}"
    
    def test_fact_model_data_quality_filtering(self):
        """Test data quality filtering criteria"""
        quality_filters = [
            "data_quality_score >= 0.7",
            "NOT is_bot",
            "session_id IS NOT NULL",
            "anonymous_id IS NOT NULL"
        ]
        
        for filter_condition in quality_filters:
            assert filter_condition in self.fact_model_sql, f"Should filter by: {filter_condition}"
    
    def test_journey_model_iceberg_configuration(self):
        """Test user journey model Iceberg configuration"""
        assert "table_type='iceberg'" in self.journey_model_sql, "Should specify Iceberg table type"
        assert "partitioned_by=['analysis_date']" in self.journey_model_sql, "Should partition by analysis_date"
        
        # Check for Iceberg table properties
        assert 'write.format.default' in self.journey_model_sql
        assert 'write.parquet.compression-codec' in self.journey_model_sql
    
    def test_journey_model_session_aggregation(self):
        """Test session-level aggregation logic"""
        aggregation_functions = [
            "MIN(event_timestamp) AS session_start",
            "MAX(event_timestamp) AS session_end", 
            "COUNT(*) AS total_events",
            "COUNT(DISTINCT space_id) AS unique_spaces_viewed",
            "SUM(time_on_page_seconds) AS total_time_on_site_seconds"
        ]
        
        for agg_func in aggregation_functions:
            assert agg_func in self.journey_model_sql, f"Should include aggregation: {agg_func}"
    
    def test_journey_model_journey_sequence_construction(self):
        """Test journey sequence array construction"""
        # Should use ARRAY_AGG with STRUCT for journey sequence
        assert "ARRAY_AGG(" in self.journey_model_sql, "Should aggregate journey sequence"
        assert "STRUCT(" in self.journey_model_sql, "Should use STRUCT for complex journey data"
        assert "ORDER BY session_event_sequence" in self.journey_model_sql, "Should order journey sequence"
    
    def test_journey_model_window_functions(self):
        """Test window function usage for session analysis"""
        window_functions = [
            "FIRST_VALUE(",
            "LAST_VALUE(",
            "OVER (",
            "PARTITION BY session_id",
            "ORDER BY session_event_sequence"
        ]
        
        for window_func in window_functions:
            assert window_func in self.journey_model_sql, f"Should use window function: {window_func}"
    
    def test_journey_model_conversion_intent_logic(self):
        """Test conversion intent level classification"""
        conversion_levels = [
            'high_conversion_intent',
            'medium_conversion_intent',
            'low_conversion_intent',
            'no_conversion_intent'
        ]
        
        for level in conversion_levels:
            assert level in self.journey_model_sql, f"Should classify conversion intent: {level}"
        
        # Check for specific conditions
        high_intent_conditions = [
            "core_tour_events >= 5",
            "unique_spaces_viewed >= 3",
            "session_duration_minutes >= 3",
            "total_engagement_score >= 10"
        ]
        
        for condition in high_intent_conditions:
            assert condition in self.journey_model_sql, f"Should check high intent condition: {condition}"
    
    def test_journey_model_space_preference_analysis(self):
        """Test space preference pattern detection"""
        preference_patterns = [
            'comprehensive_explorer',
            'unit_focused',
            'amenity_focused', 
            'tour_focused',
            'unfocused_browsing'
        ]
        
        for pattern in preference_patterns:
            assert pattern in self.journey_model_sql, f"Should detect preference pattern: {pattern}"
        
        # Check for ARRAY_CONTAINS usage
        assert "ARRAY_CONTAINS(" in self.journey_model_sql, "Should use ARRAY_CONTAINS for space type analysis"
    
    def test_journey_model_calculated_metrics(self):
        """Test calculated metrics and ratios"""
        calculated_metrics = [
            "users_per_session_ratio",
            "total_bounce_rate",
            "engagement_rate", 
            "conversion_intent_rate",
            "referral_rate",
            "high_quality_rate"
        ]
        
        for metric in calculated_metrics:
            assert metric in self.journey_model_sql, f"Should calculate metric: {metric}"
    
    def test_time_travel_model_iceberg_features(self):
        """Test time travel model uses Iceberg time travel features"""
        # Check for FOR TIMESTAMP AS OF clause
        assert "FOR TIMESTAMP AS OF" in self.time_travel_sql, "Should use Iceberg time travel"
        assert "CURRENT_TIMESTAMP - INTERVAL" in self.time_travel_sql, "Should query historical snapshots"
        
        # Check for time travel comparison logic
        assert "current_events" in self.time_travel_sql, "Should compare current state"
        assert "historical_events" in self.time_travel_sql, "Should compare historical state"
    
    def test_time_travel_model_anomaly_detection(self):
        """Test anomaly detection logic in time travel analysis"""
        anomaly_flags = [
            'high_variance',
            'medium_variance',
            'normal_variance',
            'quality_degradation',
            'quality_improvement',
            'quality_stable'
        ]
        
        for flag in anomaly_flags:
            assert flag in self.time_travel_sql, f"Should detect anomaly type: {flag}"
        
        # Check for variance thresholds
        variance_thresholds = ["50", "25", "30", "15"]  # Percentage thresholds
        
        for threshold in variance_thresholds:
            assert threshold in self.time_travel_sql, f"Should use variance threshold: {threshold}%"
    
    def test_time_travel_model_data_freshness_indicators(self):
        """Test data freshness status indicators"""
        freshness_statuses = [
            'data_growing',
            'data_stale',
            'data_decreasing',
            'data_stable'
        ]
        
        for status in freshness_statuses:
            assert status in self.time_travel_sql, f"Should detect freshness status: {status}"


class TestDbtModelConfiguration:
    """Test dbt model configurations and materialization strategies"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_fact_model_materialization_config(self):
        """Test fact model materialization configuration"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            content = f.read()
        
        # Extract config block
        config_match = re.search(r'config\((.*?)\)', content, re.DOTALL)
        assert config_match, "Should have config block"
        
        config_content = config_match.group(1)
        
        # Test materialization strategy
        assert "materialized='table'" in config_content, "Should materialize as table"
        
        # Test Iceberg-specific configs
        iceberg_configs = [
            "table_type='iceberg'",
            "unique_key='event_id'",
            "write.target-file-size-bytes",
            "write.format.default",
            "write.parquet.compression-codec"
        ]
        
        for config in iceberg_configs:
            assert config in config_content, f"Should include config: {config}"
        
        # Test partitioning
        assert "partitioned_by=['event_date', 'space_type']" in config_content
    
    def test_journey_model_materialization_config(self):
        """Test user journey model materialization configuration"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        config_match = re.search(r'config\((.*?)\)', content, re.DOTALL)
        assert config_match, "Should have config block"
        
        config_content = config_match.group(1)
        
        # Test basic configs
        assert "materialized='table'" in config_content
        assert "table_type='iceberg'" in config_content
        assert "partitioned_by=['analysis_date']" in config_content
    
    def test_time_travel_model_materialization_config(self):
        """Test time travel model materialization configuration"""
        time_travel_path = self.models_path / "marts/analytics/time_travel_analysis.sql"
        with open(time_travel_path, 'r') as f:
            content = f.read()
        
        config_match = re.search(r'config\((.*?)\)', content, re.DOTALL)
        assert config_match, "Should have config block"
        
        config_content = config_match.group(1)
        
        # Should be Iceberg table
        assert "table_type='iceberg'" in config_content
    
    def test_model_dependencies(self):
        """Test model dependencies through ref() functions"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            fact_content = f.read()
        
        # Should reference intermediate models
        assert "ref('int_session_boundaries')" in fact_content, "Should reference session boundaries model"
        
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            journey_content = f.read()
        
        # Should reference fact model
        assert "ref('fct_web_events_iceberg')" in journey_content, "Should reference fact model"
        
        time_travel_path = self.models_path / "marts/analytics/time_travel_analysis.sql"
        with open(time_travel_path, 'r') as f:
            time_travel_content = f.read()
        
        # Should reference fact model
        assert "ref('fct_web_events_iceberg')" in time_travel_content, "Should reference fact model"


class TestDbtModelDataTypes:
    """Test data type specifications and casting in models"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_fact_model_data_type_casting(self):
        """Test proper data type casting in fact model"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            content = f.read()
        
        # Test timestamp conversions
        timestamp_casts = [
            "F.to_timestamp(",
            "F.to_date("
        ]
        
        for cast_func in timestamp_casts:
            assert cast_func in content, f"Should use timestamp cast: {cast_func}"
        
        # Test integer casts
        integer_casts = [
            ".cast('int')",
            ".cast('bigint')"
        ]
        
        for cast_func in integer_casts:
            assert cast_func in content, f"Should cast integers: {cast_func}"
    
    def test_journey_model_aggregation_types(self):
        """Test aggregation function return types"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Test date/time functions
        date_functions = [
            "DATE_DIFF(",
            "PERCENTILE_CONT("
        ]
        
        for func in date_functions:
            assert func in content, f"Should use date function: {func}"
        
        # Test array functions
        array_functions = [
            "ARRAY_AGG(",
            "ARRAY_CONTAINS("
        ]
        
        for func in array_functions:
            assert func in content, f"Should use array function: {func}"


class TestDbtModelFiltering:
    """Test filtering logic and data quality controls"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_fact_model_quality_filters(self):
        """Test data quality filtering in fact model"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            content = f.read()
        
        # Test quality score thresholds
        assert "data_quality_score >= 0.7" in content, "Should filter by quality score"
        
        # Test bot filtering
        assert "NOT is_bot" in content, "Should filter out bots"
        
        # Test null filtering
        assert "session_id IS NOT NULL" in content, "Should filter null sessions"
        assert "anonymous_id IS NOT NULL" in content, "Should filter null anonymous IDs"
    
    def test_journey_model_date_filtering(self):
        """Test date-based filtering in journey model"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Should filter recent data
        assert "CURRENT_DATE - INTERVAL" in content, "Should filter by recent dates"
        assert "30" in content and "DAY" in content, "Should look at 30-day window"
        
        # Should maintain quality filtering
        assert "data_quality_score >= 0.7" in content, "Should maintain quality threshold"
    
    def test_journey_model_session_filtering(self):
        """Test session-level filtering criteria"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Should filter low quality sessions
        assert "min_data_quality_score >= 0.5" in content, "Should filter low quality sessions"
        
        # Should filter small segments
        assert "session_count >= 10" in content, "Should filter out very small segments"


class TestDbtModelComments:
    """Test model documentation and comments"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_fact_model_documentation(self):
        """Test fact model has proper documentation"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            content = f.read()
        
        # Should have post-hook comment
        assert "post_hook" in content, "Should have post-hook for table comment"
        assert "COMMENT ON TABLE" in content, "Should add table comment"
        assert "Iceberg format" in content, "Should mention Iceberg in comment"
    
    def test_model_inline_comments(self):
        """Test models have helpful inline comments"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            fact_content = f.read()
        
        # Should have section comments
        section_comments = [
            "-- Space categorization",
            "-- User engagement",
            "-- Journey analysis",
            "-- Data lineage"
        ]
        
        for comment in section_comments:
            assert comment in fact_content, f"Should have section comment: {comment}"


class TestDbtModelPerformance:
    """Test performance considerations in dbt models"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_iceberg_optimization_properties(self):
        """Test Iceberg optimization properties are set"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            content = f.read()
        
        # Test file size targeting
        assert "write.target-file-size-bytes" in content, "Should set target file size"
        
        # Test compression
        assert "zstd" in content, "Should use zstd compression"
        
        # Test format
        assert "parquet" in content, "Should use Parquet format"
        
        # Test merge strategy
        assert "merge-on-read" in content, "Should use merge-on-read for streaming"
    
    def test_partitioning_strategy(self):
        """Test partitioning strategies are appropriate"""
        fact_model_path = self.models_path / "marts/core/fct_web_events_iceberg.sql"
        with open(fact_model_path, 'r') as f:
            fact_content = f.read()
        
        # Fact table should partition by date and space_type
        assert "partitioned_by=['event_date', 'space_type']" in fact_content
        
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            journey_content = f.read()
        
        # Analytics table should partition by analysis_date
        assert "partitioned_by=['analysis_date']" in journey_content
    
    def test_aggregation_efficiency(self):
        """Test that aggregations are efficiently structured"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Should use appropriate GROUP BY clauses
        assert "GROUP BY" in content, "Should group data appropriately"
        
        # Should use window functions efficiently
        window_patterns = [
            "PARTITION BY session_id",
            "ORDER BY session_event_sequence"
        ]
        
        for pattern in window_patterns:
            assert pattern in content, f"Should use efficient window function: {pattern}"


class TestDbtModelIntegration:
    """Test integration between models and external systems"""
    
    def test_s3_tables_catalog_references(self):
        """Test that models reference S3 Tables catalog correctly"""
        time_travel_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models/marts/analytics/time_travel_analysis.sql")
        with open(time_travel_path, 'r') as f:
            content = f.read()
        
        # Should reference s3_tables catalog
        # This would be in the actual queries generated by dbt
        # The models themselves use ref() functions
        assert "ref(" in content, "Should use dbt ref() functions for dependencies"
    
    def test_iceberg_specific_functions(self):
        """Test usage of Iceberg-specific functions"""
        time_travel_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models/marts/analytics/time_travel_analysis.sql")
        with open(time_travel_path, 'r') as f:
            content = f.read()
        
        # Should use Iceberg time travel
        assert "FOR TIMESTAMP AS OF" in content, "Should use Iceberg time travel syntax"
        
        # Should use current timestamp functions
        assert "CURRENT_TIMESTAMP" in content, "Should use current timestamp"


class TestDbtModelEdgeCases:
    """Test edge cases and error handling in dbt models"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_null_handling_in_aggregations(self):
        """Test null value handling in aggregations"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Should handle nulls in calculations
        null_handling_patterns = [
            "COALESCE(",
            "IS NOT NULL",
            "IS NULL"
        ]
        
        for pattern in null_handling_patterns:
            assert pattern in content, f"Should handle nulls: {pattern}"
    
    def test_division_by_zero_protection(self):
        """Test protection against division by zero"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Should check denominators before division
        division_patterns = [
            "* 100.0 /",  # Percentage calculations
            "WHEN .* > 0"  # Conditional checks
        ]
        
        for pattern in division_patterns:
            if re.search(pattern, content):
                # If division exists, should have protection
                assert re.search(r"WHEN.*>.*0.*THEN", content), "Should protect against division by zero"
    
    def test_array_function_safety(self):
        """Test safe usage of array functions"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Should filter nulls in array aggregations
        if "ARRAY_AGG(" in content:
            # Should have FILTER or WHERE clause
            assert "FILTER" in content or "WHERE" in content, "Should filter nulls in array aggregation"


class TestDbtModelBusinessLogic:
    """Test business logic correctness in models"""
    
    def setup_method(self):
        """Setup test environment"""
        self.models_path = Path("/Users/james/workspace-peek/load-webevents/dbt/models")
    
    def test_engagement_scoring_thresholds(self):
        """Test engagement scoring thresholds are reasonable"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Test engagement thresholds
        engagement_thresholds = [
            ("25", "extremely_engaged"),
            ("15", "highly_engaged"),
            ("8", "moderately_engaged"),
            ("3", "lightly_engaged")
        ]
        
        for threshold, level in engagement_thresholds:
            assert threshold in content, f"Should have threshold {threshold}"
            assert level in content, f"Should have engagement level {level}"
    
    def test_conversion_intent_criteria(self):
        """Test conversion intent criteria are business-appropriate"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # High conversion intent criteria
        high_intent_criteria = [
            "core_tour_events >= 5",
            "unique_spaces_viewed >= 3", 
            "session_duration_minutes >= 3",
            "total_engagement_score >= 10"
        ]
        
        for criteria in high_intent_criteria:
            assert criteria in content, f"Should include high intent criteria: {criteria}"
    
    def test_bounce_rate_definitions(self):
        """Test bounce rate definitions are consistent"""
        journey_model_path = self.models_path / "marts/analytics/user_journey_analysis_iceberg.sql"
        with open(journey_model_path, 'r') as f:
            content = f.read()
        
        # Bounce classifications
        bounce_types = [
            'immediate_bounce',
            'quick_bounce',
            'engaged_session'
        ]
        
        for bounce_type in bounce_types:
            assert bounce_type in content, f"Should classify bounce type: {bounce_type}"
        
        # Should have reasonable time thresholds
        assert "total_events = 1" in content, "Should detect immediate bounces"
        assert "session_duration_minutes < 1" in content, "Should detect quick bounces"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=10"])