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

def parse_kinesis_records_file(line):
    """Parse a line that contains Kinesis Records wrapper format"""
    events = []
    
    try:
        if not line or not line.strip():
            return events
            
        line_data = json.loads(line.strip())
        
        # Check if this is a Kinesis Records wrapper format
        if 'Records' in line_data and isinstance(line_data['Records'], list):
            # Process Kinesis Records wrapper - multiple events per file
            for record in line_data['Records']:
                if 'kinesis' in record and 'data' in record['kinesis']:
                    try:
                        # Decode base64 data from Kinesis record
                        encoded_data = record['kinesis']['data']
                        decoded_bytes = base64.b64decode(encoded_data)
                        decoded_string = decoded_bytes.decode('utf-8')
                        event_data = json.loads(decoded_string)
                        
                        transformed = transform_web_event_mongodb(event_data)
                        if transformed:
                            events.append(transformed)
                            
                    except Exception as e:
                        print(f"Error processing Kinesis record: {e}")
                        continue
        
        else:
            # Handle direct event format (legacy compatibility)
            try:
                if isinstance(line_data, str):
                    # Try base64 decoding first
                    decoded_bytes = base64.b64decode(line_data)
                    decoded_string = decoded_bytes.decode('utf-8')
                    event_data = json.loads(decoded_string)
                    transformed = transform_web_event_mongodb(event_data)
                    if transformed:
                        events.append(transformed)
                else:
                    # Direct JSON event parse
                    transformed = transform_web_event_mongodb(line_data)
                    if transformed:
                        events.append(transformed)
            except Exception as e:
                print(f"Error processing direct event: {e}")
        
    except Exception as e:
        print(f"Error parsing line: {e}")
        
    return events

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
            
            # NEW: 3D navigation coordinates (as JSON string for Spark compatibility)
            'coordinates_json': json.dumps(coordinates_struct) if coordinates_struct else None,
            
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
    
    print(f"Reading Kinesis Records files from: {input_path}")
    print(f"Writing to S3 Tables: {args['WEB_EVENTS_TABLE']}")
    
    # Read and parse Kinesis Records files
    try:
        # Read text files from S3
        raw_df = spark.read.text(input_path)
        raw_count = raw_df.count()
        print(f"Read {raw_count} Kinesis Records files from S3")
        
        # Process files and extract all events
        all_events = []
        raw_lines = raw_df.collect()
        
        print(f"Processing {len(raw_lines)} files for Kinesis Records...")
        
        for row in raw_lines:
            line = row['value']  
            events = parse_kinesis_records_file(line)
            all_events.extend(events)
        
        print(f"Extracted {len(all_events)} events from Kinesis Records files")
        
        if not all_events:
            print("No valid events found to process")
            job.commit()
            return
        
        # Convert to Spark DataFrame
        events_rdd = spark.sparkContext.parallelize(all_events)
        
        # Define schema for events
        event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("event_date", StringType(), True),
            StructField("event_hour", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_name", StringType(), True),
            StructField("anonymous_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("app_id", StringType(), True),
            StructField("app_name", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("page_path", StringType(), True),
            StructField("page_title", StringType(), True),
            StructField("url_search", StringType(), True),
            StructField("url_hash", StringType(), True),
            StructField("space_id", StringType(), True),
            StructField("space_name", StringType(), True),
            StructField("space_type", StringType(), True),
            StructField("space_token", StringType(), True),
            StructField("room_id", StringType(), True),
            StructField("room_name", StringType(), True),
            StructField("community", StringType(), True),
            StructField("building", StringType(), True),
            StructField("floorplan", StringType(), True),
            StructField("coordinates_json", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("os_name", StringType(), True),
            StructField("locale", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("screen_width", StringType(), True),
            StructField("screen_height", StringType(), True),
            StructField("processed_timestamp", StringType(), True),
            StructField("seconds_utc_ts", StringType(), True),
            StructField("mongodb_id", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("properties_json", StringType(), True)
        ])
        
        # Create DataFrame from events and convert types properly
        events_df = spark.createDataFrame(events_rdd, schema=event_schema) \
            .select(
                F.col('event_id'),
                F.to_timestamp(F.col('event_timestamp')).alias('event_timestamp'),
                F.to_date(F.col('event_date')).alias('event_date'),
                F.col('event_hour').cast('int').alias('event_hour'),
                F.col('event_type'),
                F.col('event_name'),
                F.col('anonymous_id'),
                F.col('user_id'),
                F.col('session_id'),
                F.col('app_id'),
                F.col('app_name'),
                F.col('page_url'),
                F.col('page_path'),
                F.col('page_title'),
                F.col('url_search'),
                F.col('url_hash'),
                F.col('space_id'),
                F.col('space_name'), 
                F.col('space_type'),
                F.col('space_token'),
                F.col('room_id'),
                F.col('room_name'),
                F.col('community'),
                F.col('building'),
                F.col('floorplan'),
                F.col('coordinates_json'),
                F.col('user_agent'),
                F.col('os_name'),
                F.col('locale'),
                F.col('timezone'),
                F.col('referrer'),
                F.col('screen_width').cast('int').alias('screen_width'),
                F.col('screen_height').cast('int').alias('screen_height'),
                F.to_timestamp(F.col('processed_timestamp')).alias('processed_timestamp'),
                F.col('seconds_utc_ts').cast('bigint').alias('seconds_utc_ts'),
                F.col('mongodb_id'),
                F.col('created_at'),
                F.col('updated_at'),
                F.col('properties_json')
            )
        
        # Filter and enrich data
        clean_df = events_df.filter(
            F.col('session_id').isNotNull() & 
            (F.col('anonymous_id').isNotNull() | F.col('user_id').isNotNull()) &
            F.col('event_timestamp').isNotNull()
        )
        
        print(f"Clean records count: {clean_df.count()}")
        
        # Add enrichment columns
        enriched_df = clean_df.withColumn(
            'device_type',
            F.when(F.col('screen_width') < 768, 'mobile')
            .when(F.col('screen_width') < 1024, 'tablet')
            .otherwise('desktop')
        ).withColumn(
            'is_virtual_tour_page',
            F.col('page_path').contains('/viewer')
        ).withColumn(
            'has_space_context',
            F.col('space_id').isNotNull()
        ).withColumn(
            'has_room_context',
            F.col('room_id').isNotNull()
        ).withColumn(
            'has_coordinates',
            F.col('coordinates_json').isNotNull()
        ).withColumn(
            'region',
            F.when(F.col('timezone').contains('America'), 'Americas')
            .when(F.col('timezone').contains('Europe'), 'Europe')
            .when(F.col('timezone').contains('Asia') | F.col('timezone').contains('Pacific'), 'Asia-Pacific')
            .otherwise('Other')
        )
        
        # Add session sequencing using window functions
        window_spec = Window.partitionBy('session_id').orderBy('event_timestamp')
        
        session_df = enriched_df.withColumn(
            'session_event_sequence',
            F.row_number().over(window_spec)
        ).withColumn(
            'is_first_event_in_session',
            F.col('session_event_sequence') == 1
        ).withColumn(
            'next_event_timestamp',
            F.lead('event_timestamp').over(window_spec)
        ).withColumn(
            'time_on_page_seconds',
            F.when(
                F.col('next_event_timestamp').isNotNull(),
                F.unix_timestamp('next_event_timestamp') - F.unix_timestamp('event_timestamp')
            ).otherwise(30)  # Default 30 seconds for last event
        ).withColumn(
            'data_quality_score',
            F.when(
                F.col('session_id').isNull() | 
                (F.col('anonymous_id').isNull() & F.col('user_id').isNull()), 0.0
            ).when(
                F.col('user_agent').isNull() | (F.col('user_agent') == ''), 0.6
            ).when(
                F.col('space_id').isNull() & F.col('is_virtual_tour_page'), 0.7
            ).when(
                F.col('has_room_context') & F.col('has_coordinates'), 1.0
            ).otherwise(0.8)
        ).withColumn(
            'is_bot',
            F.when(
                F.lower(F.col('user_agent')).rlike('bot|crawler|spider|scraper'), True
            ).when(
                F.col('time_on_page_seconds') < 1, True
            ).otherwise(False)
        )
        
        # Filter out bots and low quality data
        final_df = session_df.filter(
            (F.col('is_bot') == False) & 
            (F.col('data_quality_score') >= 0.5)
        )
        
        print(f"Final clean events: {final_df.count()}")
        
        # Create the enhanced web_events table for Kinesis Records data
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3_tables.analytics.web_events_kinesis (
            event_id string,
            event_timestamp timestamp,
            event_date date,
            event_hour int,
            event_type string,
            event_name string,
            anonymous_id string,
            user_id string,
            session_id string,
            session_event_sequence int,
            is_first_event_in_session boolean,
            time_on_page_seconds bigint,
            app_id string,
            app_name string,
            page_url string,
            page_path string,
            page_title string,
            url_search string,
            url_hash string,
            space_id string,
            space_name string,
            space_type string,
            space_token string,
            room_id string,
            room_name string,
            community string,
            building string,
            floorplan string,
            coordinates_json string,
            user_agent string,
            os_name string,
            locale string,
            timezone string,
            region string,
            referrer string,
            screen_width int,
            screen_height int,
            device_type string,
            is_virtual_tour_page boolean,
            has_space_context boolean,
            has_room_context boolean,
            has_coordinates boolean,
            data_quality_score double,
            is_bot boolean,
            processed_timestamp timestamp,
            seconds_utc_ts bigint,
            mongodb_id string,
            created_at string,
            updated_at string,
            properties_json string
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes'='134217728',
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd'
        )
        PARTITIONED BY (event_date, space_type)
        """)
        
        # Write data to Iceberg table
        final_df.write \
            .format('iceberg') \
            .mode('append') \
            .option('write.format.default', 'parquet') \
            .option('write.target-file-size-bytes', '134217728') \
            .saveAsTable('s3_tables.analytics.web_events_kinesis')
        
        print("Successfully wrote Kinesis Records data to S3 Tables web_events_kinesis table")
        
        # Create session-level aggregations
        session_metrics = final_df.groupBy('session_id', 'anonymous_id', 'user_id', 'event_date') \
            .agg(
                F.min('event_timestamp').alias('session_start'),
                F.max('event_timestamp').alias('session_end'),
                F.count('*').alias('total_events'),
                F.countDistinct('space_id').alias('unique_spaces_viewed'),
                F.countDistinct('room_id').alias('unique_rooms_viewed'),
                F.sum('time_on_page_seconds').alias('total_time_on_site_seconds'),
                F.sum(F.when(F.col('has_coordinates'), 1).otherwise(0)).alias('events_with_3d_navigation'),
                F.collect_set('space_type').alias('space_types_viewed'),
                F.collect_set('room_name').alias('rooms_visited'),
                F.first('referrer').alias('referrer'),
                F.first('user_agent').alias('user_agent'),
                F.first('timezone').alias('timezone'),
                F.first('device_type').alias('device_type'),
                F.first('region').alias('region'),
                F.first('community').alias('community'),
                F.avg('data_quality_score').alias('avg_data_quality_score')
            ).withColumn(
                'session_duration_seconds',
                F.unix_timestamp('session_end') - F.unix_timestamp('session_start')
            ).withColumn(
                'bounce_session',
                F.col('total_events') == 1
            ).withColumn(
                'has_3d_engagement',
                F.col('events_with_3d_navigation') > 0
            ).withColumn(
                'processed_timestamp',
                F.current_timestamp()
            )
        
        # Create enhanced session metrics table for Kinesis Records
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3_tables.analytics.session_metrics_kinesis (
            session_id string,
            anonymous_id string,
            user_id string,
            session_start timestamp,
            session_end timestamp,
            session_duration_seconds bigint,
            total_events bigint,
            unique_spaces_viewed bigint,
            unique_rooms_viewed bigint,
            events_with_3d_navigation bigint,
            total_time_on_site_seconds bigint,
            bounce_session boolean,
            has_3d_engagement boolean,
            space_types_viewed array<string>,
            rooms_visited array<string>,
            referrer string,
            user_agent string,
            timezone string,
            device_type string,
            region string,
            community string,
            avg_data_quality_score double,
            processed_timestamp timestamp,
            event_date date
        ) USING iceberg
        PARTITIONED BY (event_date)
        """)
        
        # Write session metrics
        session_metrics.write \
            .format('iceberg') \
            .mode('append') \
            .saveAsTable('s3_tables.analytics.session_metrics_kinesis')
        
        print(f"Successfully wrote session metrics: {session_metrics.count()} sessions")
        
    except Exception as e:
        print(f"Failed to process Kinesis Records data: {e}")
        raise

    print("Kinesis Records processing with S3 Tables completed successfully")
    job.commit()

if __name__ == '__main__':
    main()