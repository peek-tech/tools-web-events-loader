import json
import base64
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

def decode_kinesis_record(encoded_data):
    """Decode base64 encoded Kinesis record"""
    try:
        decoded_bytes = base64.b64decode(encoded_data)
        decoded_string = decoded_bytes.decode('utf-8')
        return json.loads(decoded_string)
    except Exception as e:
        print(f"Error decoding record: {e}")
        return None

def transform_web_event_streaming(event_data, kinesis_metadata):
    """Transform web event for real-time Iceberg ingestion"""
    if not event_data:
        return None
    
    try:
        timestamp = event_data.get('secondsUtcTS', int(datetime.utcnow().timestamp() * 1000))
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        transformed = {
            'event_id': f"{event_data.get('sessionId', 'unknown')}_{timestamp}_{kinesis_metadata.get('sequenceNumber', '')}",
            'event_timestamp': dt.isoformat(),
            'event_date': dt.date().isoformat(),
            'event_hour': dt.hour,
            'event_type': event_data.get('type', 'unknown'),
            'anonymous_id': event_data.get('anonymousId'),
            'session_id': event_data.get('sessionId'),
            'app_id': event_data.get('appId'),
            'app_name': event_data.get('app'),
            
            # Page context
            'page_url': event_data.get('properties', {}).get('url'),
            'page_path': event_data.get('properties', {}).get('path'),
            'page_title': event_data.get('properties', {}).get('title'),
            'url_search': event_data.get('properties', {}).get('search'),
            'url_hash': event_data.get('properties', {}).get('hash'),
            
            # Space context
            'space_id': event_data.get('properties', {}).get('spaceId'),
            'space_name': event_data.get('properties', {}).get('spaceName'),
            'space_type': event_data.get('properties', {}).get('spaceType', 'unknown'),
            'space_token': event_data.get('properties', {}).get('spaceToken'),
            
            # User context
            'user_agent': event_data.get('userAgent'),
            'os_name': event_data.get('os', {}).get('name'),
            'locale': event_data.get('locale'),
            'timezone': event_data.get('timezone'),
            'referrer': event_data.get('referrer', ''),
            
            # Device info
            'screen_width': event_data.get('properties', {}).get('width'),
            'screen_height': event_data.get('properties', {}).get('height'),
            
            # Processing metadata
            'processed_timestamp': datetime.utcnow().isoformat(),
            'seconds_utc_ts': timestamp,
            'kinesis_sequence_number': kinesis_metadata.get('sequenceNumber'),
            'kinesis_partition_key': kinesis_metadata.get('partitionKey'),
            'properties_json': json.dumps(event_data.get('properties', {}))
        }
        
        return transformed
    except Exception as e:
        print(f"Error transforming streaming event: {e}")
        return None

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'KINESIS_STREAM_ARN', 'OUTPUT_BUCKET',
        'TABLE_BUCKET_ARN', 'WEB_EVENTS_TABLE', 'SESSION_METRICS_TABLE'
    ])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Configure Spark for Iceberg and streaming
    spark.conf.set('spark.sql.adaptive.enabled', 'true')
    spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    spark.conf.set('spark.sql.streaming.stateStore.maintenanceInterval', '300s')
    spark.conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    spark.conf.set('spark.sql.catalog.s3_tables', 'org.apache.iceberg.spark.SparkCatalog')
    spark.conf.set('spark.sql.catalog.s3_tables.catalog-impl', 'org.apache.iceberg.aws.s3.S3Tables')
    
    print(f"Processing Kinesis stream: {args['KINESIS_STREAM_ARN']}")
    print(f"Writing to S3 Tables: {args['WEB_EVENTS_TABLE']}")
    
    # Create Iceberg tables if they don't exist
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS s3_tables.analytics.web_events (
        event_id string,
        event_timestamp timestamp,
        event_date date,
        event_hour int,
        event_type string,
        anonymous_id string,
        session_id string,
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
        user_agent string,
        os_name string,
        locale string,
        timezone string,
        referrer string,
        screen_width int,
        screen_height int,
        processed_timestamp timestamp,
        seconds_utc_ts bigint,
        kinesis_sequence_number string,
        kinesis_partition_key string,
        properties_json string
    ) USING iceberg
    TBLPROPERTIES (
        'write.target-file-size-bytes'='67108864',
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    PARTITIONED BY (event_date, space_type)
    """)
    
    # Read from Kinesis stream
    kinesis_df = spark \\\n        .readStream \\\n        .format("kinesis") \\\n        .option("streamName", args['KINESIS_STREAM_ARN'].split('/')[-1]) \\\n        .option("region", "us-east-1") \\\n        .option("initialPosition", "TRIM_HORIZON") \\\n        .load()\n    \n    # Process streaming events\n    def process_kinesis_batch(batch_df, batch_id):\n        if batch_df.count() > 0:\n            print(f"Processing batch {batch_id} with {batch_df.count()} records")\n            \n            # Decode and transform Kinesis records\n            def decode_and_transform(row):\n                try:\n                    # Decode base64 data\n                    encoded_data = row.data\n                    kinesis_metadata = {\n                        'sequenceNumber': row.sequenceNumber,\n                        'partitionKey': row.partitionKey\n                    }\n                    \n                    # Decode and transform event\n                    event_data = decode_kinesis_record(encoded_data)\n                    if event_data:\n                        return transform_web_event_streaming(event_data, kinesis_metadata)\n                    return None\n                except Exception as e:\n                    print(f"Error processing Kinesis record: {e}")\n                    return None\n            \n            # Apply transformation\n            from pyspark.sql.functions import udf\n            from pyspark.sql.types import MapType, StringType\n            \n            transform_udf = udf(decode_and_transform, MapType(StringType(), StringType()))\n            \n            # Transform and clean data\n            transformed_df = batch_df.select(\n                transform_udf(F.struct(\n                    F.col('data').alias('data'),\n                    F.col('sequenceNumber').alias('sequenceNumber'),\n                    F.col('partitionKey').alias('partitionKey')\n                )).alias('transformed')\n            ).filter(F.col('transformed').isNotNull())\n            \n            if transformed_df.count() > 0:\n                # Flatten transformed data\n                events_df = transformed_df.select(\n                    F.col('transformed.event_id').alias('event_id'),\n                    F.to_timestamp(F.col('transformed.event_timestamp')).alias('event_timestamp'),\n                    F.to_date(F.col('transformed.event_date')).alias('event_date'),\n                    F.col('transformed.event_hour').cast('int').alias('event_hour'),\n                    F.col('transformed.event_type').alias('event_type'),\n                    F.col('transformed.anonymous_id').alias('anonymous_id'),\n                    F.col('transformed.session_id').alias('session_id'),\n                    F.col('transformed.app_id').alias('app_id'),\n                    F.col('transformed.app_name').alias('app_name'),\n                    F.col('transformed.page_url').alias('page_url'),\n                    F.col('transformed.page_path').alias('page_path'),\n                    F.col('transformed.page_title').alias('page_title'),\n                    F.col('transformed.url_search').alias('url_search'),\n                    F.col('transformed.url_hash').alias('url_hash'),\n                    F.col('transformed.space_id').alias('space_id'),\n                    F.col('transformed.space_name').alias('space_name'),\n                    F.col('transformed.space_type').alias('space_type'),\n                    F.col('transformed.space_token').alias('space_token'),\n                    F.col('transformed.user_agent').alias('user_agent'),\n                    F.col('transformed.os_name').alias('os_name'),\n                    F.col('transformed.locale').alias('locale'),\n                    F.col('transformed.timezone').alias('timezone'),\n                    F.col('transformed.referrer').alias('referrer'),\n                    F.col('transformed.screen_width').cast('int').alias('screen_width'),\n                    F.col('transformed.screen_height').cast('int').alias('screen_height'),\n                    F.to_timestamp(F.col('transformed.processed_timestamp')).alias('processed_timestamp'),\n                    F.col('transformed.seconds_utc_ts').cast('bigint').alias('seconds_utc_ts'),\n                    F.col('transformed.kinesis_sequence_number').alias('kinesis_sequence_number'),\n                    F.col('transformed.kinesis_partition_key').alias('kinesis_partition_key'),\n                    F.col('transformed.properties_json').alias('properties_json')\n                )\n                \n                # Filter out invalid records\n                clean_df = events_df.filter(\n                    F.col('session_id').isNotNull() & \n                    F.col('anonymous_id').isNotNull() &\n                    F.col('event_timestamp').isNotNull()\n                )\n                \n                # Write to Iceberg table using MERGE for deduplication\n                try:\n                    # Create temporary view for merge operation\n                    clean_df.createOrReplaceTempView(f"batch_{batch_id}")\n                    \n                    # Use MERGE to handle potential duplicates\n                    spark.sql(f\"\"\"\n                    MERGE INTO s3_tables.analytics.web_events AS target\n                    USING batch_{batch_id} AS source\n                    ON target.event_id = source.event_id\n                    WHEN NOT MATCHED THEN INSERT *\n                    \"\"\")\n                    \n                    print(f"Successfully merged {clean_df.count()} events to Iceberg table")\n                    \n                except Exception as e:\n                    print(f"MERGE operation failed, falling back to append: {e}")\n                    # Fallback to append if MERGE fails\n                    clean_df.write \\\n                        .format('iceberg') \\\n                        .mode('append') \\\n                        .saveAsTable('s3_tables.analytics.web_events')\n                    \n                    print(f"Successfully appended {clean_df.count()} events to Iceberg table")\n    \n    # Start streaming query with foreachBatch\n    query = kinesis_df.writeStream \\\n        .foreachBatch(process_kinesis_batch) \\\n        .outputMode("append") \\\n        .option("checkpointLocation", f"s3://{args['OUTPUT_BUCKET']}/streaming-checkpoints/web-events/") \\\n        .trigger(processingTime='30 seconds') \\\n        .start()\n    \n    print("Started Kinesis streaming job for S3 Tables")\n    \n    # Wait for termination\n    query.awaitTermination()\n    \n    job.commit()

if __name__ == "__main__":\n    main()