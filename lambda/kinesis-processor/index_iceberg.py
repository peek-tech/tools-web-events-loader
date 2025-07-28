import json
import base64
import boto3
import os
from datetime import datetime
from typing import Dict, List, Any
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']
GLUE_DATABASE = os.environ['GLUE_DATABASE']
TABLE_BUCKET_ARN = os.environ.get('TABLE_BUCKET_ARN', '')
WEB_EVENTS_TABLE = os.environ.get('WEB_EVENTS_TABLE', '')
SESSION_METRICS_TABLE = os.environ.get('SESSION_METRICS_TABLE', '')

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Process Kinesis records containing base64-encoded web events for S3 Tables/Iceberg
    """
    try:
        processed_records = []
        failed_records = []
        
        for record in event.get('Records', []):
            try:
                # Decode Kinesis record
                kinesis_data = record['kinesis']
                encoded_data = kinesis_data['data']
                
                # Decode base64 data
                decoded_bytes = base64.b64decode(encoded_data)
                decoded_string = decoded_bytes.decode('utf-8')
                event_data = json.loads(decoded_string)
                
                # Enrich with processing metadata
                enriched_event = enrich_event_for_iceberg(event_data, kinesis_data)
                if enriched_event:
                    processed_records.append(enriched_event)
                
            except Exception as e:
                logger.error(f"Failed to process record: {str(e)}")
                failed_records.append({
                    'record_id': record.get('eventID', 'unknown'),
                    'error': str(e)
                })
        
        # Batch write to S3 Tables staging
        if processed_records:
            write_to_s3_tables_staging(processed_records)
        
        # Log results
        logger.info(f"Processed {len(processed_records)} records successfully")
        if failed_records:
            logger.warning(f"Failed to process {len(failed_records)} records")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed': len(processed_records),
                'failed': len(failed_records)
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def enrich_event_for_iceberg(event_data: Dict, kinesis_data: Dict) -> Dict:
    """
    Enrich web event with processing metadata for Iceberg format
    Handles both legacy event structure and nested eventData structure
    """
    try:
        current_time = datetime.utcnow()
        
        # Handle nested document structure if present
        if 'eventData' in event_data:
            # Nested structure: extract eventData payload
            nested_document = event_data  
            event_data = nested_document['eventData']
            metadata = nested_document.get('metadata', {})
            
            # Document metadata
            document_id = str(nested_document.get('_id', {}).get('$oid', ''))
            created_at = nested_document.get('createdAt', {}).get('$date')
            updated_at = nested_document.get('updatedAt', {}).get('$date')
        else:
            # Direct web event structure
            metadata = {}
            document_id = None
            created_at = None
            updated_at = None
        
        # Extract timestamp from event or use current time
        event_timestamp_ms = event_data.get('secondsUtcTS', int(current_time.timestamp() * 1000))
        event_datetime = datetime.fromtimestamp(event_timestamp_ms / 1000)
        
        # Parse properties (can be nested in eventData.properties or event.properties)
        properties = event_data.get('properties', {})
        
        # Extract page context (MongoDB has separate page object)
        page_context = event_data.get('page', properties)
        
        # Create event ID for deduplication
        session_id = event_data.get('sessionId') or metadata.get('anonymousId', 'unknown')
        event_id = f"{session_id}_{kinesis_data.get('sequenceNumber', '')}_{event_timestamp_ms}"
        
        enriched = {
            # Unique identifier for Iceberg
            'event_id': event_id,
            
            # Timestamps in ISO format for Iceberg
            'event_timestamp': event_datetime.isoformat(),
            'event_date': event_datetime.date().isoformat(),
            'event_hour': event_datetime.hour,
            
            # Core event attributes
            'event_type': event_data.get('type', 'unknown'),
            'event_name': event_data.get('event'),  # NEW: Specific event name from MongoDB
            'anonymous_id': event_data.get('anonymousId') or metadata.get('anonymousId'),
            'user_id': event_data.get('userId'),  # NEW: Authenticated user ID
            'session_id': event_data.get('sessionId'),
            'app_id': event_data.get('appId') or metadata.get('appId'),
            'app_name': event_data.get('app'),
            
            # Page context (using page object if available, otherwise properties)
            'page_url': page_context.get('url'),
            'page_path': page_context.get('path'),
            'page_title': page_context.get('title'),
            'url_search': page_context.get('search'),
            'url_hash': page_context.get('hash'),
            
            # Space context (virtual tour specific)
            'space_id': properties.get('spaceId') or page_context.get('spaceId'),
            'space_name': properties.get('spaceName') or page_context.get('spaceName'),
            'space_type': (properties.get('spaceType') or page_context.get('spaceType') or 'unknown'),
            'space_token': properties.get('spaceToken') or page_context.get('spaceToken'),
            
            # NEW: Room-level granularity from MongoDB
            'room_id': properties.get('roomId'),
            'room_name': properties.get('roomName'),
            
            # NEW: Location hierarchy from MongoDB
            'community': properties.get('community'),
            'building': properties.get('building'),
            'floorplan': properties.get('floorplan'),
            
            # NEW: 3D Navigation coordinates from MongoDB
            'coordinates': extract_coordinates(properties.get('coordinates')),
            
            # User context
            'user_agent': event_data.get('userAgent'),
            'os_name': event_data.get('os', {}).get('name'),
            'locale': event_data.get('locale'),
            'timezone': event_data.get('timezone'),
            'referrer': event_data.get('referrer', ''),
            
            # Device info (use page context for dimensions)
            'screen_width': page_context.get('width') or properties.get('width'),
            'screen_height': page_context.get('height') or properties.get('height'),
            
            # Device type classification
            'device_type': classify_device_type(page_context.get('width') or properties.get('width')),
            
            # Processing metadata
            'processed_timestamp': current_time.isoformat(),
            'seconds_utc_ts': event_timestamp_ms,
            'kinesis_sequence_number': kinesis_data.get('sequenceNumber'),
            'kinesis_partition_key': kinesis_data.get('partitionKey'),
            
            # Raw properties for extensibility
            'properties_json': json.dumps(properties),
            
            # Document metadata for data lineage
            'document_id': document_id,
            'created_at': parse_document_timestamp(created_at),
            'updated_at': parse_document_timestamp(updated_at),
            
            # Data quality indicators (enhanced for nested structure)
            'has_space_context': bool(properties.get('spaceId') or page_context.get('spaceId')),
            'is_virtual_tour_page': (page_context.get('path', '') or properties.get('path', '')).startswith('/viewer'),
            'data_quality_score': calculate_data_quality_score_enhanced(event_data, properties, page_context),
            'is_bot': detect_bot_traffic(event_data.get('userAgent', '')),
        }
        
        return enriched
        
    except Exception as e:
        logger.error(f"Error enriching event: {str(e)}")
        return None

def classify_device_type(screen_width) -> str:
    """Classify device type based on screen width"""
    if not screen_width:
        return 'unknown'
    
    try:
        width = int(screen_width)
        if width < 768:
            return 'mobile'
        elif width < 1024:
            return 'tablet'
        else:
            return 'desktop'
    except (ValueError, TypeError):
        return 'unknown'

def extract_coordinates(coordinates_data) -> Dict:
    """Extract 3D navigation coordinates from MongoDB structure"""
    if not coordinates_data:
        return None
    
    try:
        return {
            'pitch': coordinates_data.get('pitch'),
            'yaw': coordinates_data.get('yaw'),
            'hfov': coordinates_data.get('hfov')
        }
    except (AttributeError, TypeError):
        return None

def parse_document_timestamp(timestamp_data) -> str:
    """Parse document timestamp to ISO string"""
    if not timestamp_data:
        return None
    
    try:
        if isinstance(timestamp_data, str):
            return timestamp_data
        elif isinstance(timestamp_data, dict) and '$date' in timestamp_data:
            return timestamp_data['$date']
        else:
            return str(timestamp_data)
    except Exception:
        return None

def calculate_data_quality_score_enhanced(event_data: Dict, properties: Dict, page_context: Dict) -> float:
    """Enhanced data quality score calculation for nested event structure"""
    score = 1.0
    
    # Core identifiers
    if not event_data.get('sessionId'):
        score -= 0.3
    if not (event_data.get('anonymousId') or event_data.get('userId')):
        score -= 0.3
    if not event_data.get('userAgent'):
        score -= 0.2
    
    # Space context quality (check both properties and page context)
    space_id = properties.get('spaceId') or page_context.get('spaceId')
    path = properties.get('path') or page_context.get('path', '')
    
    if not space_id and path.startswith('/viewer'):
        score -= 0.1
    
    # Room-level context (bonus for detailed spatial data)
    if properties.get('roomId') and properties.get('roomName'):
        score += 0.1
    
    # 3D coordinates (bonus for navigation data)
    if properties.get('coordinates'):
        score += 0.05
    
    return max(0.0, min(1.0, score))

def calculate_data_quality_score(event_data: Dict, properties: Dict) -> float:
    """Calculate data quality score for the event (legacy compatibility)"""
    return calculate_data_quality_score_enhanced(event_data, properties, properties)

def detect_bot_traffic(user_agent: str) -> bool:
    """Simple bot detection logic"""
    if not user_agent:
        return True
        
    bot_indicators = ['bot', 'crawler', 'spider', 'scraper', 'automated']
    user_agent_lower = user_agent.lower()
    
    return any(indicator in user_agent_lower for indicator in bot_indicators)

def write_to_s3_tables_staging(events: List[Dict]) -> None:
    """
    Write events to S3 staging area for Iceberg ingestion via Glue streaming job
    """
    try:
        # Group events by partition (event_date, space_type)
        partitioned_events = {}
        
        for event in events:
            event_date = event['event_date']
            space_type = event.get('space_type', 'unknown')
            partition_key = f"{event_date}_{space_type}"
            
            if partition_key not in partitioned_events:
                partitioned_events[partition_key] = []
            
            partitioned_events[partition_key].append(event)
        
        # Write to S3 as staging files for Glue streaming job to pick up
        for partition_key, partition_events in partitioned_events.items():
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
            s3_key = f"streaming-staging/web-events/{partition_key}/lambda_batch_{timestamp}.jsonl"
            
            # Prepare data as JSON Lines format
            jsonl_data = '\\n'.join([json.dumps(event) for event in partition_events])
            
            # Upload to staging location with metadata
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=jsonl_data.encode('utf-8'),
                ContentType='application/jsonl',
                ServerSideEncryption='aws:kms',
                Metadata={
                    'table-target': 'web_events',
                    'partition-date': partition_key.split('_')[0],
                    'partition-space-type': '_'.join(partition_key.split('_')[1:]),
                    'record-count': str(len(partition_events)),
                    'source': 'lambda-kinesis-processor'
                }
            )
            
            logger.info(f"Wrote {len(partition_events)} events to S3 Tables staging: s3://{BUCKET_NAME}/{s3_key}")
        
        # Trigger Glue streaming job if not already running (optional)
        try:
            # Check if streaming job is running
            response = glue_client.get_job_runs(
                JobName='peek-web-events-kinesis-processor',
                MaxResults=1
            )
            
            running_jobs = [run for run in response.get('JobRuns', []) 
                          if run['JobRunState'] in ['RUNNING', 'STARTING']]
            
            if not running_jobs:
                logger.info("No streaming job running, could start one here if needed")
                # Optionally start the streaming job
                # glue_client.start_job_run(JobName='peek-web-events-kinesis-processor')
                
        except Exception as e:
            logger.warning(f"Could not check/start Glue streaming job: {str(e)}")
            
    except Exception as e:
        logger.error(f"Failed to write to S3 Tables staging: {str(e)}")
        raise