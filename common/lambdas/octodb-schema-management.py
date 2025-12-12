import json
import boto3
import uuid
from datetime import datetime

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Configuration
SCHEMAS_TABLE = dynamodb.Table('octodb-schemas')

def lambda_handler(event, context):
    """
    Lambda function for Schema operations.
    
    Routes:
    - GET  /schemas                           → List all schemas
    - GET  /schemas/{schema_id}               → Get specific schema
    - POST /schemas                           → Create custom schema
    - GET  /tenants/{tenant_id}/schema        → Get tenant's current schema
    - POST /tenants/{tenant_id}/schema        → Create custom schema for tenant
    """
    
    try:
        http_method = event.get('httpMethod')
        path = event.get('path', '')
        path_params = event.get('pathParameters') or {}
        schema_id = path_params.get('schema_id')
        tenant_id = path_params.get('tenant_id') or path_params.get('id')
        query_params = event.get('queryStringParameters') or {}
        
        print(f"Method: {http_method}, Path: {path}, Schema ID: {schema_id}, Tenant: {tenant_id}")
        
        # Check if this is a tenant-specific schema route
        if '/tenants/' in path and '/schema' in path:
            if http_method == 'GET':
                # GET /tenants/{tenant_id}/schema
                return get_tenant_schema(tenant_id)
            elif http_method == 'POST':
                # POST /tenants/{tenant_id}/schema
                body = json.loads(event.get('body', '{}'))
                body['tenant_id'] = tenant_id  # Inject tenant_id from path
                return create_schema(body)
        
        # Generic schema routes
        if http_method == 'GET' and not schema_id:
            # GET /schemas - List all
            filter_tenant_id = query_params.get('tenant_id')
            return list_schemas(filter_tenant_id)
        elif http_method == 'GET' and schema_id:
            # GET /schemas/{schema_id} - Get one
            return get_schema(schema_id)
        elif http_method == 'POST':
            # POST /schemas - Create custom schema
            body = json.loads(event.get('body', '{}'))
            return create_schema(body)
        else:
            return error_response(405, f'Method {http_method} not allowed')
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return error_response(500, f'Internal server error: {str(e)}')


def list_schemas(tenant_id=None):
    """List all schemas, optionally filtered by tenant_id"""
    try:
        if tenant_id:
            # Query using GSI for specific tenant
            response = SCHEMAS_TABLE.query(
                IndexName='tenant_id-index',
                KeyConditionExpression='tenant_id = :tid',
                ExpressionAttributeValues={':tid': tenant_id}
            )
        else:
            # Scan all schemas
            response = SCHEMAS_TABLE.scan()
        
        schemas = response.get('Items', [])
        
        # Sort by created_at
        schemas.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        # Separate APPLICATION and CUSTOM schemas
        app_schemas = [s for s in schemas if s.get('schema_type') == 'APPLICATION']
        custom_schemas = [s for s in schemas if s.get('schema_type') == 'CUSTOM']
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Schemas retrieved successfully',
                'count': len(schemas),
                'application_schemas': app_schemas,
                'custom_schemas': custom_schemas
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to list schemas: {str(e)}')


def get_schema(schema_id):
    """Get specific schema details"""
    try:
        response = SCHEMAS_TABLE.get_item(Key={'schema_id': schema_id})
        
        if 'Item' not in response:
            return error_response(404, f'Schema {schema_id} not found')
        
        schema = response['Item']
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Schema retrieved successfully',
                'schema': schema
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to get schema: {str(e)}')


def get_tenant_schema(tenant_id):
    """Get tenant's current schema information"""
    try:
        # Import here to avoid circular dependency
        TENANTS_TABLE = dynamodb.Table('octodb-tenants')
        
        # Get tenant info
        tenant_response = TENANTS_TABLE.get_item(Key={'tenant_id': tenant_id})
        
        if 'Item' not in tenant_response:
            return error_response(404, f'Tenant {tenant_id} not found')
        
        tenant = tenant_response['Item']
        current_schema_version = tenant.get('schema_version', 'v1.0')
        
        # Get the schema details
        schema_response = SCHEMAS_TABLE.get_item(Key={'schema_id': current_schema_version})
        
        schema_info = None
        if 'Item' in schema_response:
            schema_info = schema_response['Item']
        
        # Also get any custom schemas for this tenant
        custom_schemas_response = SCHEMAS_TABLE.query(
            IndexName='tenant_id-index',
            KeyConditionExpression='tenant_id = :tid',
            ExpressionAttributeValues={':tid': tenant_id}
        )
        
        custom_schemas = custom_schemas_response.get('Items', [])
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Tenant schema information retrieved successfully',
                'tenant_id': tenant_id,
                'current_schema_version': current_schema_version,
                'current_schema': schema_info,
                'custom_schemas': custom_schemas
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to get tenant schema: {str(e)}')


def create_schema(body):
    """Create a new custom schema for a tenant"""
    try:
        # Validate required fields
        tenant_id = body.get('tenant_id')
        schema_name = body.get('schema_name')
        schema_sql = body.get('schema_sql')
        parent_schema_id = body.get('parent_schema_id', '')
        
        if not tenant_id or not schema_name or not schema_sql:
            return error_response(400, 'Missing required fields: tenant_id, schema_name, schema_sql')
        
        # Generate unique schema ID
        schema_id = f"custom_{uuid.uuid4().hex[:12]}"
        current_time = datetime.utcnow().isoformat() + 'Z'
        
        # Create schema item
        schema_item = {
            'schema_id': schema_id,
            'tenant_id': tenant_id,
            'schema_name': schema_name,
            'schema_sql': schema_sql,
            'schema_type': 'CUSTOM',
            'version': 'custom',
            'parent_schema_id': parent_schema_id,
            'created_at': current_time,
            'created_by': tenant_id
        }
        
        # Insert into DynamoDB
        SCHEMAS_TABLE.put_item(Item=schema_item)
        
        print(f"Created custom schema: {schema_id} for tenant: {tenant_id}")
        
        return {
            'statusCode': 201,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Custom schema created successfully',
                'schema_id': schema_id,
                'tenant_id': tenant_id,
                'schema_name': schema_name,
                'created_at': current_time
            })
        }
    except Exception as e:
        return error_response(500, f'Failed to create schema: {str(e)}')


def error_response(status_code, message):
    """Helper function for error responses"""
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'error': message})
    }
