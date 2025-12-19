import os
import json
import boto3
import sqlite3
import tempfile
from boto3.dynamodb.conditions import Key

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

TENANT_METADATA_TABLE = os.environ.get('TENANT_METADATA_TABLE', 'octodb-tenants')
REPLICA_METADATA_TABLE = os.environ.get('REPLICA_METADATA_TABLE', 'tenant-metadata')
TENANT_NAME_INDEX = os.environ.get('TENANT_NAME_INDEX', 'Tenant_Name_Index')


def lambda_handler(event, context):
    try:
        # Parse the POST request body
        if 'body' not in event:
            return create_response(400, {'error': 'Request body is missing'})
        
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        # Validate required fields
        tenant_name = body.get('tenant_name')
        api_key = body.get('api_key')
        sql_query = body.get('sql_query')
        
        if not tenant_name or not api_key or not sql_query:
            return create_response(400, {
                'error': 'Missing required fields. Please provide tenant_name, api_key, and sql_query'
            })
        
        # Step 1: Query tenant metadata table using tenant_name index
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
        
        # Check if tenant exists
        if not response.get('Items'):
            return create_response(404, {
                'error': f'Tenant "{tenant_name}" not found'
            })
        
        tenant_item = response['Items'][0]
        
        # Validate API key
        if tenant_item.get('api_key') != api_key:
            return create_response(401, {
                'error': 'Invalid API key'
            })
        
        # Step 2: Get tenant_id from tenant metadata
        tenant_id = tenant_item.get('tenant_id')
        if not tenant_id:
            return create_response(500, {
                'error': 'Tenant ID not found in metadata'
            })
        
        # Step 3: Get replica metadata using tenant_id
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
        
        # Step 4: Download DB file from S3 and execute query
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_db_path = tmp_file.name
            
            try:
                # Download the database file from S3
                s3.download_file(read_only_bucket, db_path, tmp_db_path)
            except Exception as e:
                return create_response(500, {
                    'error': f'Failed to download database from S3: {str(e)}'
                })
            
            try:
                # Connect to SQLite database and execute query
                conn = sqlite3.connect(tmp_db_path)
                conn.row_factory = sqlite3.Row  # Enable column name access
                cursor = conn.cursor()
                
                try:
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()
                    
                    # Convert rows to list of dictionaries
                    result = [dict(row) for row in rows]
                    
                    return create_response(200, {
                        'success': True,
                        'data': result,
                        'row_count': len(result),
                        'source': {
                            'region': 'us-east-1',
                        }
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
                # Clean up temporary file
                try:
                    os.unlink(tmp_db_path)
                except:
                    pass
    
    except json.JSONDecodeError:
        return create_response(400, {
            'error': 'Invalid JSON in request body'
        })
    except Exception as e:
        return create_response(500, {
            'error': f'Unexpected error: {str(e)}'
        })


def create_response(status_code, body):
    """Helper function to create standardized API responses"""
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
    
    
    '''{
        "tenant_name": "Tandon",
        "api_key": "sk_live_PAC_x0LUbWVUZLzUeBitoTsDbt83bQSNmqf3uaTx-Yo",
        "sql_query": "SELECT * FROM Users;"
        } 
    '''
