import os
import json
import boto3
import sqlite3
import tempfile
from datetime import datetime
from boto3.dynamodb.conditions import Key

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')

TENANT_METADATA_TABLE = os.environ.get('TENANT_METADATA_TABLE', 'octodb-tenants')
REPLICA_METADATA_TABLE = os.environ.get('REPLICA_METADATA_TABLE', 'tenant-metadata')
TENANT_NAME_INDEX = os.environ.get('TENANT_NAME_INDEX', 'Tenant_Name_Index')

# Optional env vars for AccessTenant / hot cold logic
EFS_MOUNT_DIR = os.environ.get('EFS_MOUNT_DIR', '/mnt/efs')
REHYDRATION_FUNCTION_NAME = os.environ.get('REHYDRATION_FUNCTION_NAME')


def lambda_handler(event, context):
    """
    Sample Request:
    {
        "tenant_name": "Tandon",
        "api_key": "xcv",
        "sql_query": "SELECT * FROM Users;"
    }
    """
    try:
        if 'body' not in event:
            return create_response(400, {'error': 'Request body is missing'})

        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']

        tenant_name = body.get('tenant_name')
        api_key = body.get('api_key')
        sql_query = body.get('sql_query')

        if not tenant_name or not api_key or not sql_query:
            return create_response(400, {
                'error': 'Missing required fields. Please provide tenant_name, api_key, and sql_query'
            })

        # Step 1: tenant lookup and API key validation
        tenant_table = dynamodb.Table(TENANT_METADATA_TABLE)

        try:
            response = tenant_table.query(
                IndexName=TENANT_NAME_INDEX,
                KeyConditionExpression=Key('tenant_name').eq(tenant_name)
            )
        except Exception as e:
            return create_response(500, {
                'error': f'Failed to query tenant metadata: {str(e)}'
            })

        if not response.get('Items'):
            return create_response(404, {
                'error': f'Tenant "{tenant_name}" not found'
            })

        tenant_item = response['Items'][0]

        if tenant_item.get('api_key') != api_key:
            return create_response(401, {
                'error': 'Invalid API key'
            })

        tenant_id = tenant_item.get('tenant_id')
        if not tenant_id:
            return create_response(500, {
                'error': 'Tenant ID not found in metadata'
            })

        # Step 2: replica metadata (read only replica)
        replica_table = dynamodb.Table(REPLICA_METADATA_TABLE)

        try:
            replica_response = replica_table.get_item(
                Key={'tenantId': tenant_id}
            )
        except Exception as e:
            return create_response(500, {
                'error': f'Failed to query replica metadata: {str(e)}'
            })

        if 'Item' not in replica_response:
            return create_response(404, {
                'error': f'Replica metadata not found for tenant_id "{tenant_id}"'
            })

        replica_item = replica_response['Item']
        read_only_bucket = replica_item.get('read_only_bucket')
        db_path = replica_item.get('db_path')

        if not read_only_bucket or not db_path:
            return create_response(500, {
                'error': 'Read-only bucket or database path not found in replica metadata'
            })

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
            # Hot copy lives under /mnt/efs/{db_key}
            efs_db_path = os.path.join(EFS_MOUNT_DIR, db_key)
            os.makedirs(os.path.dirname(efs_db_path), exist_ok=True)

            if not os.path.exists(efs_db_path):
                # Ask rehydration handler to populate EFS from the read replica
                if REHYDRATION_FUNCTION_NAME:
                    try:
                        invoke_rehydration(
                            tenant_id=tenant_id,
                            tenant_name=tenant_name,
                            source_bucket=read_only_bucket,
                            db_key=db_key,
                            target_path=efs_db_path,
                            source_type='read_replica'
                        )
                    except Exception as e:
                        print(f'WARNING: Rehydration invocation failed for tenant {tenant_id}: {e}')
                        use_efs = False
                else:
                    use_efs = False

        # Step 3: execute query, prefer EFS when available
        if use_efs and efs_db_path and os.path.exists(efs_db_path):
            # Hot path: use EFS directly
            try:
                conn = sqlite3.connect(efs_db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                try:
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()
                    result = [dict(row) for row in rows]

                    return create_response(200, {
                        'success': True,
                        'data': result,
                        'row_count': len(result),
                        'storage_tier': storage_tier,
                        'db_source': 'EFS'
                    })
                except sqlite3.Error as e:
                    return create_response(400, {
                        'error': f'SQL query execution failed: {str(e)}'
                    })
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                return create_response(500, {
                    'error': f'Database connection error (EFS): {str(e)}'
                })
        else:
            # Cold path: existing behavior using S3 read replica
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_db_path = tmp_file.name

            try:
                try:
                    s3.download_file(read_only_bucket, db_path, tmp_db_path)
                except Exception as e:
                    return create_response(500, {
                        'error': f'Failed to download database from S3: {str(e)}'
                    })

                try:
                    conn = sqlite3.connect(tmp_db_path)
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    try:
                        cursor.execute(sql_query)
                        rows = cursor.fetchall()
                        result = [dict(row) for row in rows]

                        return create_response(200, {
                            'success': True,
                            'data': result,
                            'row_count': len(result),
                            'storage_tier': storage_tier,
                            'db_source': 'S3_READ_REPLICA'
                        })
                    except sqlite3.Error as e:
                        return create_response(400, {
                            'error': f'SQL query execution failed: {str(e)}'
                        })
                    finally:
                        cursor.close()
                        conn.close()
                except Exception as e:
                    return create_response(500, {
                        'error': f'Database connection error: {str(e)}'
                    })
                finally:
                    try:
                        os.unlink(tmp_db_path)
                    except OSError:
                        pass
            except Exception as e:
                return create_response(500, {
                    'error': f'Unexpected error during DB processing: {str(e)}'
                })

    except json.JSONDecodeError:
        return create_response(400, {
            'error': 'Invalid JSON in request body'
        })
    except Exception as e:
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
    """
    Helper that calls the rehydration Lambda synchronously.
    The rehydration-handler will be implemented separately.
    """
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
