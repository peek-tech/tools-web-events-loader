import json
import base64
import boto3  
import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re

def decode_base64_record(encoded_data):
    """Decode base64 encoded data from S3 objects"""
    try:
        decoded_bytes = base64.b64decode(encoded_data)
        decoded_string = decoded_bytes.decode('utf-8')
        return json.loads(decoded_string)
    except Exception as e:
        print(f"Error decoding record: {e}")
        return None

def transform_web_event_mongodb(event_data):
    """Transform MongoDB web event document to standardized format for Iceberg"""
    if not event_data:
        return None
    
    try:
        # Handle MongoDB document structure
        if 'eventData' in event_data:
            # MongoDB structure
            mongodb_doc = event_data
            event_data = mongodb_doc['eventData']
            metadata = mongodb_doc.get('metadata', {})
            
            # MongoDB metadata
            mongodb_id = str(mongodb_doc.get('_id', {}).get('$oid', ''))
            created_at = mongodb_doc.get('createdAt', {}).get('$date')
            updated_at = mongodb_doc.get('updatedAt', {}).get('$date')
        else:
            # Direct web event structure (fallback)
            metadata = {}
            mongodb_id = None
            created_at = None
            updated_at = None
        
        # Extract timestamp and convert to standard format
        timestamp = event_data.get('secondsUtcTS', 0)
        if timestamp == 0:
            timestamp = int(datetime.utcnow().timestamp() * 1000)
            
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # Parse properties and page context
        properties = event_data.get('properties', {})
        page_context = event_data.get('page', properties)
        
        # Extract coordinates structure
        coordinates = properties.get('coordinates')
        coordinates_struct = None
        if coordinates:
            coordinates_struct = {
                'pitch': coordinates.get('pitch'),
                'yaw': coordinates.get('yaw'),
                'hfov': coordinates.get('hfov')
            }
        
        transformed = {
            'event_id': f"{event_data.get('sessionId', 'unknown')}_{timestamp}_{hash(str(event_data)) % 10000}",
            'event_timestamp': dt.isoformat(),
            'event_date': dt.date().isoformat(),
            'event_hour': dt.hour,
            'event_type': event_data.get('type', 'unknown'),
            'event_name': event_data.get('event'),  # NEW: Specific event name
            'anonymous_id': event_data.get('anonymousId') or metadata.get('anonymousId'),
            'user_id': event_data.get('userId'),  # NEW: Authenticated user
            'session_id': event_data.get('sessionId'),
            'app_id': event_data.get('appId') or metadata.get('appId'),
            'app_name': event_data.get('app'),
            
            # Page context (prefer page object, fallback to properties)
            'page_url': page_context.get('url'),
            'page_path': page_context.get('path'),
            'page_title': page_context.get('title'),
            'url_search': page_context.get('search'),
            'url_hash': page_context.get('hash'),
            
            # Space context (check both properties and page)
            'space_id': properties.get('spaceId') or page_context.get('spaceId'),
            'space_name': properties.get('spaceName') or page_context.get('spaceName'),
            'space_type': properties.get('spaceType') or page_context.get('spaceType') or 'unknown',
            'space_token': properties.get('spaceToken') or page_context.get('spaceToken'),
            
            # NEW: Room-level granularity
            'room_id': properties.get('roomId'),
            'room_name': properties.get('roomName'),
            
            # NEW: Location hierarchy
            'community': properties.get('community'),
            'building': properties.get('building'),
            'floorplan': properties.get('floorplan'),
            
            # NEW: 3D navigation coordinates
            'coordinates': coordinates_struct,
            
            # User context
            'user_agent': event_data.get('userAgent'),
            'os_name': event_data.get('os', {}).get('name'),
            'locale': event_data.get('locale'),
            'timezone': event_data.get('timezone'),
            'referrer': event_data.get('referrer', ''),
            
            # Screen dimensions (prefer page context)
            'screen_width': page_context.get('width') or properties.get('width'),
            'screen_height': page_context.get('height') or properties.get('height'),
            
            # Quality and processing metadata
            'processed_timestamp': datetime.utcnow().isoformat(),
            'seconds_utc_ts': timestamp,
            
            # NEW: MongoDB metadata for data lineage
            'mongodb_id': mongodb_id,
            'created_at': created_at,
            'updated_at': updated_at,
            
            # Raw properties for future extensibility
            'properties_json': json.dumps(properties)
        }
        
        return transformed
    except Exception as e:
        print(f"Error transforming MongoDB event: {e}")
        return None

def transform_web_event(event_data):
    """Transform web event to standardized format for Iceberg (legacy compatibility)"""
    return transform_web_event_mongodb(event_data)

def calculate_data_quality_score(row):
    """Enhanced data quality score calculation for MongoDB structure"""
    score = 1.0
    
    # Core identifiers
    if not row.get('session_id'):
        score -= 0.3
    if not (row.get('anonymous_id') or row.get('user_id')):
        score -= 0.3
    if not row.get('user_agent'):
        score -= 0.2
    
    # Space context quality
    if not row.get('space_id') and '/viewer' in str(row.get('page_path', '')):
        score -= 0.1
    
    # Bonus for enhanced MongoDB fields
    if row.get('room_id') and row.get('room_name'):
        score += 0.1
    if row.get('coordinates'):
        score += 0.05
    if row.get('event_name'):
        score += 0.05
    
    return max(0.0, min(1.0, score))

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'SOURCE_BUCKET', 'OUTPUT_BUCKET', 
        'TABLE_BUCKET_ARN', 'WEB_EVENTS_TABLE', 'SESSION_METRICS_TABLE'
    ])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Enable Iceberg optimizations
    spark.conf.set('spark.sql.adaptive.enabled', 'true')
    spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    spark.conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    spark.conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    spark.conf.set('spark.sql.catalog.s3_tables', 'org.apache.iceberg.spark.SparkCatalog')
    spark.conf.set('spark.sql.catalog.s3_tables.catalog-impl', 'org.apache.iceberg.aws.s3.S3Tables')
    
    # Read from source S3 bucket
    input_path = f"s3://{args['SOURCE_BUCKET']}/"
    
    print(f"Reading from: {input_path}")
    print(f"Writing to S3 Tables: {args['WEB_EVENTS_TABLE']}")
    
    # Read and parse historical data
    try:
        # Read text files from S3
        raw_df = spark.read.text(input_path)
        print(f"Read {raw_df.count()} text lines from S3")
        
        # Parse JSON with UDF
        def safe_json_parse(line):
            try:
                if line and line.strip():
                    # Try base64 decoding first
                    try:
                        decoded = decode_base64_record(line.strip())
                        if decoded:
                            return transform_web_event(decoded)
                    except:
                        pass
                    
                    # Try direct JSON parse
                    event_data = json.loads(line.strip())
                    return transform_web_event(event_data)
                return None
            except Exception as e:
                return None
        
        # Register UDF
        from pyspark.sql.functions import udf
        from pyspark.sql.types import MapType, StringType
        parse_json_udf = udf(safe_json_parse, MapType(StringType(), StringType()))
        
        # Parse and clean data
        parsed_df = raw_df.select(parse_json_udf(F.col('value')).alias('parsed_event')) \\\n                          .filter(F.col('parsed_event').isNotNull())\n        \n        # Flatten parsed events into proper schema\n        events_df = parsed_df.select(\n            F.col('parsed_event.event_id').alias('event_id'),\n            F.to_timestamp(F.col('parsed_event.event_timestamp')).alias('event_timestamp'),\n            F.to_date(F.col('parsed_event.event_date')).alias('event_date'),\n            F.col('parsed_event.event_hour').cast('int').alias('event_hour'),\n            F.col('parsed_event.event_type').alias('event_type'),\n            F.col('parsed_event.anonymous_id').alias('anonymous_id'),\n            F.col('parsed_event.session_id').alias('session_id'),\n            F.col('parsed_event.app_id').alias('app_id'),\n            F.col('parsed_event.app_name').alias('app_name'),\n            F.col('parsed_event.page_url').alias('page_url'),\n            F.col('parsed_event.page_path').alias('page_path'),\n            F.col('parsed_event.page_title').alias('page_title'),\n            F.col('parsed_event.url_search').alias('url_search'),\n            F.col('parsed_event.url_hash').alias('url_hash'),\n            F.col('parsed_event.space_id').alias('space_id'),\n            F.col('parsed_event.space_name').alias('space_name'),\n            F.col('parsed_event.space_type').alias('space_type'),\n            F.col('parsed_event.space_token').alias('space_token'),\n            F.col('parsed_event.user_agent').alias('user_agent'),\n            F.col('parsed_event.os_name').alias('os_name'),\n            F.col('parsed_event.locale').alias('locale'),\n            F.col('parsed_event.timezone').alias('timezone'),\n            F.col('parsed_event.referrer').alias('referrer'),\n            F.col('parsed_event.screen_width').cast('int').alias('screen_width'),\n            F.col('parsed_event.screen_height').cast('int').alias('screen_height'),\n            F.to_timestamp(F.col('parsed_event.processed_timestamp')).alias('processed_timestamp'),\n            F.col('parsed_event.seconds_utc_ts').cast('bigint').alias('seconds_utc_ts'),\n            F.col('parsed_event.properties_json').alias('properties_json')\n        )\n        \n        # Filter and enrich data\n        clean_df = events_df.filter(\n            F.col('session_id').isNotNull() & \n            F.col('anonymous_id').isNotNull() &\n            F.col('event_timestamp').isNotNull()\n        )\n        \n        print(f\"Clean records count: {clean_df.count()}\")\n        \n        # Add enrichment columns\n        enriched_df = clean_df.withColumn(\n            'device_type',\n            F.when(F.col('screen_width') < 768, 'mobile')\n            .when(F.col('screen_width') < 1024, 'tablet')\n            .otherwise('desktop')\n        ).withColumn(\n            'is_virtual_tour_page',\n            F.col('page_path').contains('/viewer')\n        ).withColumn(\n            'has_space_context',\n            F.col('space_id').isNotNull()\n        ).withColumn(\n            'region',\n            F.when(F.col('timezone').contains('America'), 'Americas')\n            .when(F.col('timezone').contains('Europe'), 'Europe')\n            .when(F.col('timezone').contains('Asia') | F.col('timezone').contains('Pacific'), 'Asia-Pacific')\n            .otherwise('Other')\n        )\n        \n        # Add session sequencing using window functions\n        window_spec = Window.partitionBy('session_id').orderBy('event_timestamp')\n        \n        session_df = enriched_df.withColumn(\n            'session_event_sequence',\n            F.row_number().over(window_spec)\n        ).withColumn(\n            'is_first_event_in_session',\n            F.col('session_event_sequence') == 1\n        ).withColumn(\n            'next_event_timestamp',\n            F.lead('event_timestamp').over(window_spec)\n        ).withColumn(\n            'time_on_page_seconds',\n            F.when(\n                F.col('next_event_timestamp').isNotNull(),\n                F.unix_timestamp('next_event_timestamp') - F.unix_timestamp('event_timestamp')\n            ).otherwise(30)  # Default 30 seconds for last event\n        ).withColumn(\n            'data_quality_score',\n            F.when(\n                F.col('session_id').isNull() | F.col('anonymous_id').isNull(), 0.0\n            ).when(\n                F.col('user_agent').isNull() | (F.col('user_agent') == ''), 0.6\n            ).when(\n                F.col('space_id').isNull() & F.col('is_virtual_tour_page'), 0.7\n            ).otherwise(1.0)\n        ).withColumn(\n            'is_bot',\n            F.when(\n                F.lower(F.col('user_agent')).rlike('bot|crawler|spider|scraper'), True\n            ).when(\n                F.col('time_on_page_seconds') < 1, True\n            ).otherwise(False)\n        )\n        \n        # Filter out bots and low quality data\n        final_df = session_df.filter(\n            (F.col('is_bot') == False) & \n            (F.col('data_quality_score') >= 0.5)\n        )\n        \n        print(f\"Final clean events: {final_df.count()}\")\n        \n        # Write to S3 Tables using Iceberg format\n        # First create the table if it doesn't exist\n        spark.sql(f\"\"\"\n        CREATE TABLE IF NOT EXISTS s3_tables.analytics.web_events (\n            event_id string,\n            event_timestamp timestamp,\n            event_date date,\n            event_hour int,\n            event_type string,\n            anonymous_id string,\n            session_id string,\n            session_event_sequence int,\n            is_first_event_in_session boolean,\n            time_on_page_seconds bigint,\n            app_id string,\n            app_name string,\n            page_url string,\n            page_path string,\n            page_title string,\n            url_search string,\n            url_hash string,\n            space_id string,\n            space_name string,\n            space_type string,\n            space_token string,\n            user_agent string,\n            os_name string,\n            locale string,\n            timezone string,\n            region string,\n            referrer string,\n            screen_width int,\n            screen_height int,\n            device_type string,\n            is_virtual_tour_page boolean,\n            has_space_context boolean,\n            data_quality_score double,\n            is_bot boolean,\n            processed_timestamp timestamp,\n            seconds_utc_ts bigint,\n            properties_json string\n        ) USING iceberg\n        TBLPROPERTIES (\n            'write.target-file-size-bytes'='134217728',\n            'write.format.default'='parquet',\n            'write.parquet.compression-codec'='zstd'\n        )\n        PARTITIONED BY (event_date, space_type)\n        \"\"\")\n        \n        # Write data to Iceberg table\n        final_df.write \\\n            .format('iceberg') \\\n            .mode('append') \\\n            .option('write.format.default', 'parquet') \\\n            .option('write.target-file-size-bytes', '134217728') \\\n            .saveAsTable('s3_tables.analytics.web_events')\n        \n        print(\"Successfully wrote data to S3 Tables web_events table\")\n        \n        # Create session-level aggregations\n        session_metrics = final_df.groupBy('session_id', 'anonymous_id', 'event_date') \\\n            .agg(\n                F.min('event_timestamp').alias('session_start'),\n                F.max('event_timestamp').alias('session_end'),\n                F.count('*').alias('total_events'),\n                F.countDistinct('space_id').alias('unique_spaces_viewed'),\n                F.sum('time_on_page_seconds').alias('total_time_on_site_seconds'),\n                F.collect_list('space_type').alias('space_types_viewed'),\n                F.first('referrer').alias('referrer'),\n                F.first('user_agent').alias('user_agent'),\n                F.first('timezone').alias('timezone'),\n                F.first('device_type').alias('device_type'),\n                F.first('region').alias('region')\n            ).withColumn(\n                'session_duration_seconds',\n                F.unix_timestamp('session_end') - F.unix_timestamp('session_start')\n            ).withColumn(\n                'bounce_session',\n                F.col('total_events') == 1\n            ).withColumn(\n                'processed_timestamp',\n                F.current_timestamp()\n            )\n        \n        # Create session metrics table\n        spark.sql(f\"\"\"\n        CREATE TABLE IF NOT EXISTS s3_tables.analytics.session_metrics (\n            session_id string,\n            anonymous_id string,\n            session_start timestamp,\n            session_end timestamp,\n            session_duration_seconds bigint,\n            total_events bigint,\n            unique_spaces_viewed bigint,\n            total_time_on_site_seconds bigint,\n            bounce_session boolean,\n            space_types_viewed array<string>,\n            referrer string,\n            user_agent string,\n            timezone string,\n            device_type string,\n            region string,\n            processed_timestamp timestamp,\n            event_date date\n        ) USING iceberg\n        PARTITIONED BY (event_date)\n        \"\"\")\n        \n        # Write session metrics\n        session_metrics.write \\\n            .format('iceberg') \\\n            .mode('append') \\\n            .saveAsTable('s3_tables.analytics.session_metrics')\n        \n        print(f\"Successfully wrote session metrics: {session_metrics.count()} sessions\")\n        \n    except Exception as e:\n        print(f\"Failed to process historical data: {e}")
        raise

    print("Historical data processing with S3 Tables completed successfully")
    job.commit()

if __name__ == '__main__':
    main()