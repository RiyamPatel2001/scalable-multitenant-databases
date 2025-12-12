import json
import boto3
import uuid
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
standby_s3 = boto3.client('s3', region_name='us-east-2')
dynamodb = boto3.resource('dynamodb')

# Configuration
S3_BUCKET = 'octodb-tenants-bucket'
TENANTS_TABLE = dynamodb.Table('octodb-tenants')
READ_ONLY_S3_BUCKET = 'scalable-db-read-only-replica-bucket'
STANDBY_S3_BUCKET = 'octodb-tenants-standby-bucket'
REPLICA_METADATA_TABLE = dynamodb.Table('tenant-metadata')

def lambda_handler(event, context):
    """
    Lambda function for Tenant CRUD operations.
    
    Routes:
    - GET    /tenants                 → List all tenants
    - GET    /tenants/{tenant_id}     → Get tenant details
    - PUT    /tenants/{tenant_id}     → Update tenant
    - DELETE /tenants/{tenant_id}     → Delete tenant
    """
    
    try:
        http_method = event.get('httpMethod')
        path = event.get('path', '')
        path_params = event.get('pathParameters') or {}
        query_params = event.get('queryStringParameters') or {}
        tenant_id = path_params.get('tenant_id')
        
        print(f"Method: {http_method}, Path: {path}, Tenant: {tenant_id}, Query: {query_params}")
        
        # Route to appropriate handler
        if http_method == 'GET' and not tenant_id:
            # GET /tenants - List all (with optional filters)
            return list_tenants(query_params)
        elif http_method == 'GET' and tenant_id:
            # GET /tenants/{tenant_id} - Get one
            return get_tenant(tenant_id)
        elif http_method == 'PUT' and tenant_id:
            # PUT /tenants/{tenant_id} - Update
            body = json.loads(event.get('body', '{}'))
            return update_tenant(tenant_id, body)
        elif http_method == 'DELETE' and tenant_id:
            # DELETE /tenants/{tenant_id} - Delete
            return delete_tenant(tenant_id)
        else:
            return error_response(405, f'Method {http_method} not allowed')
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return error_response(500, f'Internal server error: {str(e)}')


def list_tenants(query_params=None):
    """List all tenants with optional filtering by name or email"""
    try:
        query_params = query_params or {}
        filter_name = query_params.get('name')
        filter_email = query_params.get('email')
        
        # Scan all tenants (DynamoDB doesn't index by name/email, so we scan)
        response = TENANTS_TABLE.scan()
        tenants = response.get('Items', [])
        
        # Apply filters if provided
        if filter_name:
            filter_name_lower = filter_name.lower()
            tenants = [
                t for t in tenants 
                if filter_name_lower in t.get('tenant_name', '').lower()
            ]
            print(f"Filtered by name '{filter_name}': {len(tenants)} results")
        
        if filter_email:
            filter_email_lower = filter_email.lower()
            tenants = [
                t for t in tenants 
                if filter_email_lower in t.get('admin_email', '').lower()
            ]
            print(f"Filtered by email '{filter_email}': {len(tenants)} results")
        
        # Sort by created_at (most recent first)
        tenants.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Tenants retrieved successfully',
                'count': len(tenants),
                'filters': {
                    'name': filter_name,
                    'email': filter_email
                },
                'tenants': tenants
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to list tenants: {str(e)}')


def get_tenant(tenant_id):
    """Get specific tenant details"""
    try:
        response = TENANTS_TABLE.get_item(Key={'tenant_id': tenant_id})
        
        if 'Item' not in response:
            return error_response(404, f'Tenant {tenant_id} not found')
        
        tenant = response['Item']
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Tenant retrieved successfully',
                'tenant': tenant
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to get tenant: {str(e)}')


def update_tenant(tenant_id, body):
    """Update tenant information"""
    try:
        # Check if tenant exists
        response = TENANTS_TABLE.get_item(Key={'tenant_id': tenant_id})
        if 'Item' not in response:
            return error_response(404, f'Tenant {tenant_id} not found')
        
        # Build update expression
        update_expr = "SET "
        expr_attr_values = {}
        expr_attr_names = {}
        
        if 'tenant_name' in body:
            update_expr += "#tn = :tn, "
            expr_attr_names['#tn'] = 'tenant_name'
            expr_attr_values[':tn'] = body['tenant_name']
        
        if 'admin_email' in body:
            update_expr += "admin_email = :ae, "
            expr_attr_values[':ae'] = body['admin_email']
        
        if not expr_attr_values:
            return error_response(400, 'No fields to update')
        
        # Add updated_at timestamp
        update_expr += "updated_at = :ua"
        expr_attr_values[':ua'] = datetime.utcnow().isoformat() + 'Z'
        
        # Perform update
        update_kwargs = {
            'Key': {'tenant_id': tenant_id},
            'UpdateExpression': update_expr,
            'ExpressionAttributeValues': expr_attr_values,
            'ReturnValues': 'ALL_NEW'
        }
        
        if expr_attr_names:
            update_kwargs['ExpressionAttributeNames'] = expr_attr_names
        
        result = TENANTS_TABLE.update_item(**update_kwargs)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Tenant updated successfully',
                'tenant': result['Attributes']
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to update tenant: {str(e)}')


def delete_tenant(tenant_id):
    """Delete tenant and their database"""
    try:
        # Get tenant info first
        response = TENANTS_TABLE.get_item(Key={'tenant_id': tenant_id})
        if 'Item' not in response:
            return error_response(404, f'Tenant {tenant_id} not found')
        
        tenant = response['Item']
        db_path = tenant.get('current_db_path', '')
        
        # Delete database from S3
        if db_path:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=db_path)
                print(f"Deleted database: s3://{S3_BUCKET}/{db_path}")
            except Exception as s3_error:
                print(f"Warning: Failed to delete S3 object: {s3_error}")
                # Continue with DynamoDB deletion even if S3 fails
            
            try:
                standby_s3.delete_object(Bucket=STANDBY_S3_BUCKET, Key=db_path)
                print(f"Deleted database: s3://{STANDBY_S3_BUCKET}/{db_path}")
            except Exception as s3_error:
                print(f"Warning: Failed to delete S3 object from Standby bucket: {s3_error}")
            
            try:
                s3.delete_object(Bucket=READ_ONLY_S3_BUCKET, Key=db_path)
                print(f"Deleted database: s3://{READ_ONLY_S3_BUCKET}/{db_path}")
            except Exception as s3_error:
                print(f"Warning: Failed to delete S3 object from Read-Only bucket: {s3_error}")
            
        
        # Delete tenant from DynamoDB
        TENANTS_TABLE.delete_item(Key={'tenant_id': tenant_id})
        print(f"Deleted tenant from DynamoDB: {tenant_id}")
        
        # Delete replica metadata from DynamoDB
        REPLICA_METADATA_TABLE.delete_item(Key={'tenantId': tenant_id})
        print(f"Deleted replica metadata from DynamoDB: {tenant_id}")
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Tenant deleted successfully',
                'tenant_id': tenant_id,
                'deleted_database': db_path
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to delete tenant: {str(e)}')


def error_response(status_code, message):
    """Helper function for error responses"""
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'error': message})
    }
