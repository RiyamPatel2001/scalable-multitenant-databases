import os
import json
import boto3
import sqlite3
import tempfile
from boto3.dynamodb.conditions import Key

# S3 client for us-east-2 
s3 = boto3.client('s3', region_name='us-east-2')
# DynamoDB resource 
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

TENANT_METADATA_TABLE = os.environ.get('TENANT_METADATA_TABLE', 'octodb-tenants')
REPLICA_METADATA_TABLE = os.environ.get('REPLICA_METADATA_TABLE', 'tenant-metadata')
TENANT_NAME_INDEX = os.environ.get('TENANT_NAME_INDEX', 'Tenant_Name_Index')


def lambda_handler(event, context):
    '''
    Standby Read Handler - Reads from standby bucket in us-east-2
    Used when primary region (us-east-1) is unavailable
    
    Sample Request:
    {
        "tenant_name": "Tandon",
        "api_key": "xcv",
        "sql_query": "SELECT * FROM Users;"
    } 
    '''
    try:
        print('STANDBY READ HANDLER - Processing request')
        
        # Parse the POST request body
        if 'body' not in event:
            print('ERROR: Request body is missing')
            return create_response(400, {'error': 'Request body is missing'})
        
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        # Validate required fields
        tenant_name = body.get('tenant_name')
        api_key = body.get('api_key')
        sql_query = body.get('sql_query')
        
        if not tenant_name or not api_key or not sql_query:
            print('ERROR: Missing required fields in request')
            return create_response(400, {
                'error': 'Missing required fields. Please provide tenant_name, api_key, and sql_query'
            })
        
        print(f'Processing standby read request for tenant: {tenant_name}')
        
        # Step 1: Query tenant metadata table using tenant_name index
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
        
        # Check if tenant exists
        if not response.get('Items'):
            print(f'WARNING: Tenant not found: {tenant_name}')
            return create_response(404, {
                'error': f'Tenant "{tenant_name}" not found'
            })
        
        tenant_item = response['Items'][0]
        
        # Validate API key
        if tenant_item.get('api_key') != api_key:
            print(f'WARNING: Invalid API key for tenant: {tenant_name}')
            return create_response(401, {
                'error': 'Invalid API key'
            })
        
        # Step 2: Get tenant_id from tenant metadata
        tenant_id = tenant_item.get('tenant_id')
        if not tenant_id:
            print('ERROR: Tenant ID not found in metadata')
            return create_response(500, {
                'error': 'Tenant ID not found in metadata'
            })
        
        print(f'Tenant ID retrieved: {tenant_id}')
        
        # Step 3: Get replica metadata using tenant_id
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
        # Use standby_bucket instead of read_only_bucket
        standby_bucket = replica_item.get('standby_bucket')
        db_path = replica_item.get('db_path')
        
        if not standby_bucket or not db_path:
            print('ERROR: Standby bucket or database path not found in replica metadata')
            return create_response(500, {
                'error': 'Standby bucket or database path not found in replica metadata'
            })
        
        print(f'Reading from standby bucket: {standby_bucket}/{db_path}')
        
        # Step 4: Download DB file from standby S3 bucket and execute query
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_db_path = tmp_file.name
            
            try:
                # Download the database file from standby S3 bucket
                s3.download_file(standby_bucket, db_path, tmp_db_path)
                print('Database downloaded successfully from standby bucket')
            except Exception as e:
                print(f'ERROR: Failed to download database from standby S3 bucket: {str(e)}')
                return create_response(500, {
                    'error': f'Failed to download database from standby S3 bucket: {str(e)}'
                })
            
            try:
                # Connect to SQLite database and execute query
                conn = sqlite3.connect(tmp_db_path)
                conn.row_factory = sqlite3.Row  # Enable column name access
                cursor = conn.cursor()
                
                try:
                    print(f'Executing SQL query: {sql_query[:100]}...')
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()
                    
                    # Convert rows to list of dictionaries
                    result = [dict(row) for row in rows]
                    
                    print(f'Query executed successfully. Rows returned: {len(result)}')
                    
                    return create_response(200, {
                        'success': True,
                        'data': result,
                        'row_count': len(result),
                    })
                    
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
            finally:
                # Clean up temporary file
                try:
                    os.unlink(tmp_db_path)
                    print('Temporary database file cleaned up')
                except Exception as e:
                    print(f'WARNING: Failed to delete temporary file: {str(e)}')
    
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

