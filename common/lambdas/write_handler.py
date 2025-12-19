import os
import json
import boto3
import sqlite3
import tempfile
from datetime import datetime
from boto3.dynamodb.conditions import Key

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
lambda_client = boto3.client('lambda')

# Environment variables
TENANT_METADATA_TABLE = os.environ.get('TENANT_METADATA_TABLE', 'octodb-tenants')
REPLICA_METADATA_TABLE = os.environ.get('REPLICA_METADATA_TABLE', 'tenant-metadata')
TENANT_NAME_INDEX = os.environ.get('TENANT_NAME_INDEX', 'Tenant_Name_Index')
SNS_TOPIC_ARN = os.environ.get(
    'SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:666802050343:ReplicationStack-WriteTopic-wyk88ACy3a2i'
)

# env vars for AccessTenant / hot cold logic
EFS_MOUNT_DIR = os.environ.get('EFS_MOUNT_DIR', '/mnt/efs')
REHYDRATION_FUNCTION_NAME = os.environ.get('REHYDRATION_FUNCTION_NAME')

# ------------------------
# Redis (ElastiCache) invalidation
# ------------------------
try:
    import redis  # provided via Lambda Layer
except Exception:
    redis = None

REDIS_ENABLED = os.environ.get("REDIS_ENABLED", "false").lower() == "true"
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_TLS = os.environ.get("REDIS_TLS", "false").lower() == "true"
REDIS_AUTH_TOKEN = os.environ.get("REDIS_AUTH_TOKEN")
REDIS_CONNECT_TIMEOUT_MS = int(os.environ.get("REDIS_CONNECT_TIMEOUT_MS", "50"))
REDIS_SOCKET_TIMEOUT_MS = int(os.environ.get("REDIS_SOCKET_TIMEOUT_MS", "50"))

_redis_client = None


def _get_redis_client():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if not (REDIS_ENABLED and redis and REDIS_HOST):
        _redis_client = None
        return None
    try:
        kwargs = dict(
            host=REDIS_HOST,
            port=REDIS_PORT,
            socket_connect_timeout=REDIS_CONNECT_TIMEOUT_MS / 1000.0,
            socket_timeout=REDIS_SOCKET_TIMEOUT_MS / 1000.0,
            decode_responses=False,
        )
        if REDIS_TLS:
            kwargs["ssl"] = True
        if REDIS_AUTH_TOKEN:
            kwargs["password"] = REDIS_AUTH_TOKEN
        client = redis.Redis(**kwargs)
        client.ping()
        _redis_client = client
        return client
    except Exception as e:
        print(f"WARNING: Redis invalidation disabled/unreachable: {e}")
        _redis_client = None
        return None


def _tenant_ver_key(tenant_id: str) -> str:
    return f"octodb:tenant:{tenant_id}:ver"


def bump_cache_version(tenant_id: str):
    client = _get_redis_client()
    if not client or not tenant_id:
        return
    try:
        client.incr(_tenant_ver_key(tenant_id))
        print(f"Redis cache version bumped for tenant_id={tenant_id}")
    except Exception as e:
        print(f"WARNING: Redis cache version bump failed: {e}")


def lambda_handler(event, context):
    try:
        if 'body' not in event:
            print('ERROR: Request body is missing')
            return create_response(400, {'error': 'Request body is missing'})

        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']

        tenant_name = body.get('tenant_name')
        api_key = body.get('api_key')
        sql_query = body.get('sql_query')

        if not tenant_name or not api_key or not sql_query:
            print('ERROR: Missing required fields in request')
            return create_response(400, {
                'error': 'Missing required fields. Please provide tenant_name, api_key, and sql_query'
            })

        print(f'Processing write request for tenant: {tenant_name}')

        # Step 1: tenant lookup and API key validation
        tenant_table = dynamodb.Table(TENANT_METADATA_TABLE)

        try:
            response = tenant_table.query(
                IndexName=TENANT_NAME_INDEX,
                KeyConditionExpression=Key('tenant_name').eq(tenant_name)
            )
        except Exception as e:
            print(f'ERROR: Failed to query tenant metadata: {str(e)}')
            return create_response(500, {
                'error': f'Failed to query tenant metadata: {str(e)}'
            })

        if not response.get('Items'):
            print(f'WARNING: Tenant not found: {tenant_name}')
            return create_response(404, {
                'error': f'Tenant "{tenant_name}" not found'
            })

        tenant_item = response['Items'][0]

        if tenant_item.get('api_key') != api_key:
            print(f'WARNING: Invalid API key for tenant: {tenant_name}')
            return create_response(401, {
                'error': 'Invalid API key'
            })

        tenant_id = tenant_item.get('tenant_id')
        if not tenant_id:
            print('ERROR: Tenant ID not found in metadata')
            return create_response(500, {
                'error': 'Tenant ID not found in metadata'
            })

        print(f'Tenant ID retrieved: {tenant_id}')

        # Step 2: replica metadata
        replica_table = dynamodb.Table(REPLICA_METADATA_TABLE)

        try:
            replica_response = replica_table.get_item(
                Key={'tenantId': tenant_id}
            )
        except Exception as e:
            print(f'ERROR: Failed to query replica metadata: {str(e)}')
            return create_response(500, {
                'error': f'Failed to query replica metadata: {str(e)}'
            })

        if 'Item' not in replica_response:
            print(f'ERROR: Replica metadata not found for tenant_id: {tenant_id}')
            return create_response(404, {
                'error': f'Replica metadata not found for tenant_id "{tenant_id}"'
            })

        replica_item = replica_response['Item']
        primary_bucket = replica_item.get('primary_bucket')
        read_only_bucket = replica_item.get('read_only_bucket')
        standby_bucket = replica_item.get('standby_bucket')
        db_path = replica_item.get('db_path')

        if not primary_bucket or not db_path:
            print('ERROR: Primary bucket or database path not found in replica metadata')
            return create_response(500, {
                'error': 'Primary bucket or database path not found in replica metadata'
            })

        print(f'Retrieved metadata - Primary bucket: {primary_bucket}, DB path: {db_path}')

        # AccessTenant: determine storage tier and update last_accessed_at
        storage_tier = (tenant_item.get('storage_tier') or 'COLD').upper()
        db_key = tenant_item.get('current_db_path') or db_path

        update_last_accessed(tenant_table, tenant_id)

        use_efs = bool(
            storage_tier == 'HOT' and
            EFS_MOUNT_DIR and
            db_key
        )

        efs_db_path = None
        if use_efs:
            efs_db_path = os.path.join(EFS_MOUNT_DIR, db_key)
            os.makedirs(os.path.dirname(efs_db_path), exist_ok=True)

            if not os.path.exists(efs_db_path):
                if REHYDRATION_FUNCTION_NAME:
                    try:
                        print(f'Rehydration needed for tenant {tenant_id}, calling {REHYDRATION_FUNCTION_NAME}')
                        invoke_rehydration(
                            tenant_id=tenant_id,
                            tenant_name=tenant_name,
                            source_bucket=primary_bucket,
                            db_key=db_key,
                            target_path=efs_db_path,
                            source_type='primary'
                        )
                        print(f'Rehydration completed for tenant {tenant_id}')
                    except Exception as e:
                        print(f'WARNING: Rehydration invocation failed for tenant {tenant_id}: {e}')
                        use_efs = False
                else:
                    print('REHYDRATION_FUNCTION_NAME not set, falling back to S3')
                    use_efs = False

        tmp_db_path = None
        snapshot_path = None

        try:
            # If not using EFS, create temp file and download DB from S3
            if not use_efs:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp_file:
                    tmp_db_path = tmp_file.name

                print(f'Downloading database from S3: {primary_bucket}/{db_path}')
                try:
                    s3.download_file(primary_bucket, db_path, tmp_db_path)
                except Exception as e:
                    print(f'ERROR: Failed to download database from S3: {str(e)}')
                    return create_response(500, {
                        'error': f'Failed to download database from S3: {str(e)}'
                    })

            db_file_path = efs_db_path if use_efs and efs_db_path else tmp_db_path

            # Step 3: execute write query
            try:
                conn = sqlite3.connect(db_file_path)
                cursor = conn.cursor()

                try:
                    print(f'Executing SQL query: {sql_query[:100]}...')
                    cursor.execute(sql_query)
                    conn.commit()
                    rows_affected = cursor.rowcount
                    print(f'SQL query executed successfully. Rows affected: {rows_affected}')

                    # Create snapshot using VACUUM INTO
                    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                    snapshot_filename = f'{tenant_id}_snapshot_{timestamp}.db'
                    snapshot_path = f'/tmp/{snapshot_filename}'

                    print(f'Creating database snapshot: {snapshot_path}')
                    cursor.execute(f"VACUUM INTO '{snapshot_path}'")

                except sqlite3.Error as e:
                    print(f'ERROR: SQL query execution failed: {str(e)}')
                    return create_response(400, {
                        'error': f'SQL query execution failed: {str(e)}'
                    })
                finally:
                    cursor.close()
                    conn.close()

            except Exception as e:
                print(f'ERROR: Database connection error: {str(e)}')
                return create_response(500, {
                    'error': f'Database connection error: {str(e)}'
                })

            # Step 4: upload modified database to primary bucket
            try:
                source_path = efs_db_path if use_efs and efs_db_path else tmp_db_path
                print(f'Uploading modified database to S3: {primary_bucket}/{db_path}')
                s3.upload_file(source_path, primary_bucket, db_path)
            except Exception as e:
                print(f'ERROR: Failed to upload modified database to S3: {str(e)}')
                return create_response(500, {
                    'error': f'Failed to upload modified database to S3: {str(e)}'
                })

            # Upload snapshot to S3 in replication_snapshots folder
            snapshot_s3_key = f'replication_snapshots/{snapshot_filename}'
            try:
                print(f'Uploading snapshot to S3: {primary_bucket}/{snapshot_s3_key}')
                s3.upload_file(snapshot_path, primary_bucket, snapshot_s3_key)
                print('Snapshot uploaded to S3 successfully')
            except Exception as e:
                print(f'ERROR: Failed to upload snapshot to S3: {str(e)}')
                return create_response(500, {
                    'error': f'Failed to upload snapshot to S3: {str(e)}'
                })

            # Step 5: SNS notification
            if SNS_TOPIC_ARN:
                try:
                    sns_message = {
                        'tenant_name': tenant_name,
                        'tenant_id': tenant_id,
                        'snapshot_bucket': primary_bucket,
                        'snapshot_s3_key': snapshot_s3_key,
                        'snapshot_filename': snapshot_filename,
                        'primary_bucket': primary_bucket,
                        'db_path': db_path,
                        'read_only_bucket': read_only_bucket,
                        'standby_bucket': standby_bucket,
                        'timestamp': datetime.utcnow().isoformat(),
                        'rows_affected': rows_affected,
                        'storage_tier': storage_tier,
                        'db_source': 'EFS' if use_efs else 'S3_PRIMARY'
                    }

                    print(f'Publishing message to SNS topic: {SNS_TOPIC_ARN}')
                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Message=json.dumps(sns_message),
                        Subject=f'Database Write Notification - {tenant_name}'
                    )
                    print('SNS message published successfully')

                except Exception as e:
                    print(f'ERROR: Failed to publish to SNS: {str(e)}')

            # Step 6: update replica metadata last_updated_at
            try:
                current_timestamp = datetime.utcnow().isoformat()
                print(f'Updating replica metadata with last_updated_at: {current_timestamp}')

                replica_table.update_item(
                    Key={'tenantId': tenant_id},
                    UpdateExpression='SET last_updated_at = :timestamp',
                    ExpressionAttributeValues={
                        ':timestamp': current_timestamp
                    }
                )
                print('Replica metadata updated successfully')

            except Exception as e:
                print(f'ERROR: Failed to update replica metadata: {str(e)}')
                return create_response(500, {
                    'error': f'Failed to update replica metadata: {str(e)}'
                })

            bump_cache_version(tenant_id)

            print(f'Write operation completed successfully for tenant: {tenant_name}')
            return create_response(200, {
                'success': True,
                'message': 'Write operation completed successfully',
                'rows_affected': rows_affected,
                'snapshot_created': snapshot_filename,
                'snapshot_s3_key': snapshot_s3_key,
                'last_updated_at': current_timestamp,
                'storage_tier': storage_tier,
                'db_source': 'EFS' if use_efs else 'S3_PRIMARY',
                'region': 'us-east-1'
            })

        finally:
            if tmp_db_path and os.path.exists(tmp_db_path):
                try:
                    os.unlink(tmp_db_path)
                    print('Temporary database file cleaned up')
                except Exception as e:
                    print(f'WARNING: Failed to delete temporary file {tmp_db_path}: {str(e)}')

            if snapshot_path and os.path.exists(snapshot_path):
                try:
                    os.unlink(snapshot_path)
                    print('Temporary snapshot file cleaned up')
                except Exception as e:
                    print(f'WARNING: Failed to delete temporary snapshot file {snapshot_path}: {str(e)}')

    except json.JSONDecodeError as e:
        print(f'ERROR: Invalid JSON in request body: {str(e)}')
        return create_response(400, {
            'error': 'Invalid JSON in request body'
        })
    except Exception as e:
        print(f'ERROR: Unexpected error: {str(e)}')
        return create_response(500, {
            'error': f'Unexpected error: {str(e)}'
        })


def update_last_accessed(tenant_table, tenant_id):
    try:
        now = datetime.utcnow().isoformat()
        tenant_table.update_item(
            Key={'tenant_id': tenant_id},
            UpdateExpression='SET last_accessed_at = :ts',
            ExpressionAttributeValues={':ts': now}
        )
    except Exception as e:
        print(f'WARNING: Failed to update last_accessed_at for tenant {tenant_id}: {e}')


def invoke_rehydration(tenant_id, tenant_name, source_bucket, db_key, target_path, source_type):
    if not REHYDRATION_FUNCTION_NAME:
        raise RuntimeError('REHYDRATION_FUNCTION_NAME is not configured')

    payload = {
        'tenant_id': tenant_id,
        'tenant_name': tenant_name,
        'source_bucket': source_bucket,
        'db_key': db_key,
        'target_path': target_path,
        'source_type': source_type
    }

    resp = lambda_client.invoke(
        FunctionName=REHYDRATION_FUNCTION_NAME,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )

    if 'FunctionError' in resp:
        raise RuntimeError(f'Rehydration Lambda returned error: {resp["FunctionError"]}')


def create_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'POST,OPTIONS'
        },
        'body': json.dumps(body)
    }
