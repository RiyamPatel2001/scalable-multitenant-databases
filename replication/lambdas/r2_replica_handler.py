import json
import boto3
import tempfile
import os

# Initialize AWS clients
s3_primary = boto3.client('s3', region_name='us-east-1')
s3_standby = boto3.client('s3', region_name='us-east-2')


def lambda_handler(event, context):
    """
    Lambda handler for replicating database snapshots to the standby bucket (us-east-2).
    Triggered by SQS messages from SNS.
    """
    
    print(f'Received event with {len(event.get("Records", []))} record(s)')
    
    # Process each SQS message
    for record in event['Records']:
        try:
            # Parse the SQS message body (which contains the SNS message)
            message_body = json.loads(record['body'])
            
            # Extract the actual SNS message
            if 'Message' in message_body:
                # Message came through SNS -> SQS
                sns_message = json.loads(message_body['Message'])
            else:
                # Direct SQS message (fallback)
                sns_message = message_body
            
            # Step 1: Print message details
            tenant_name = sns_message.get('tenant_name')
            tenant_id = sns_message.get('tenant_id')
            snapshot_filename = sns_message.get('snapshot_filename')
            snapshot_bucket = sns_message.get('snapshot_bucket')
            snapshot_s3_key = sns_message.get('snapshot_s3_key')
            db_path = sns_message.get('db_path')
            standby_bucket = sns_message.get('standby_bucket')
            rows_affected = sns_message.get('rows_affected')
            timestamp = sns_message.get('timestamp')
            
            print('R2 STANDBY REPLICATION REQUEST RECEIVED')
            print(f'Tenant Name: {tenant_name}')
            print(f'Tenant ID: {tenant_id}')
            print(f'Snapshot Filename: {snapshot_filename}')
            print(f'Database Path: {db_path}')
            print(f'Rows Affected: {rows_affected}')
            print(f'Timestamp: {timestamp}')
            print(f'Source: {snapshot_bucket}/{snapshot_s3_key}')
            print(f'Destination: {standby_bucket}/{db_path}')
            
            # Validate required fields
            if not all([snapshot_bucket, snapshot_s3_key, standby_bucket, db_path]):
                print('ERROR: Missing required fields in message')
                raise ValueError('Missing required fields: snapshot_bucket, snapshot_s3_key, standby_bucket, or db_path')
            
            # Download snapshot file from primary bucket (us-east-1)
            tmp_snapshot_path = None
            try:
                print(f'Step 2: Downloading snapshot from S3...')
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp_file:
                    tmp_snapshot_path = tmp_file.name
                
                s3_primary.download_file(snapshot_bucket, snapshot_s3_key, tmp_snapshot_path)
                
            except Exception as e:
                print(f'ERROR: Failed to download snapshot from S3: {str(e)}')
                raise
            
            # Upload snapshot to standby bucket (us-east-2)
            try:
                print(f'Step 3: Uploading to standby bucket...')
                
                s3_standby.upload_file(tmp_snapshot_path, standby_bucket, db_path)
                
                print(f'Snapshot uploaded successfully to standby bucket')
                
            except Exception as e:
                print(f'ERROR: Failed to upload snapshot to standby bucket: {str(e)}')
                raise
            
            finally:
                # Clean up temporary file
                if tmp_snapshot_path and os.path.exists(tmp_snapshot_path):
                    try:
                        os.unlink(tmp_snapshot_path)
                        print('Temporary snapshot file cleaned up')
                    except Exception as e:
                        print(f'WARNING: Failed to delete temporary file {tmp_snapshot_path}: {str(e)}')
            
            # Replication completed
            print(f'R2 STANDBY REPLICATION COMPLETED SUCCESSFULLY')
            print(f'  Tenant: {tenant_name} ({tenant_id})')
            print(f'  File: {snapshot_filename}')
            print(f'  Replicated to: {standby_bucket}/{db_path}')
            
        except json.JSONDecodeError as e:
            print(f'ERROR: Failed to parse message JSON: {str(e)}')
            print(f'Raw message: {record.get("body", "N/A")}')
            
        except Exception as e:
            print(f'ERROR: Replication failed: {str(e)}')
            print(f'Message details: {record.get("body", "N/A")}')
            # Re-raise to trigger SQS retry mechanism
            raise
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'R2 standby replication processing completed',
            'records_processed': len(event.get('Records', []))
        })
    }
