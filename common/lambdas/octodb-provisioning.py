import json
import boto3
import sqlite3
import uuid
import secrets
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
standby_s3 = boto3.client('s3', region_name='us-east-2')
dynamodb = boto3.resource('dynamodb')

# Configuration
PRIMARY_S3_BUCKET = 'octodb-tenants-bucket'
TENANTS_TABLE = dynamodb.Table('octodb-tenants')
SCHEMAS_TABLE = dynamodb.Table('octodb-schemas')
READ_ONLY_S3_BUCKET = 'scalable-db-read-only-replica-bucket'
STANDBY_S3_BUCKET = 'octodb-tenants-standby-bucket'
REPLICA_METADATA_TABLE = dynamodb.Table('tenant-metadata')


def lambda_handler(event, context):
    """
    Lambda function to provision new tenants with their own database.

    Expected input (POST /tenants):
    {
        "tenant_name": "Acme Corporation",
        "admin_email": "admin@example.com",
        "template": "slack_v1.0"  // or "crm_v1.0" or "ecommerce_v1.0"
    }
    """

    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        tenant_name = body.get('tenant_name')
        admin_email = body.get('admin_email')
        template = body.get('template', 'slack_v1.0')  # Default to slack

        # Validate inputs
        if not tenant_name or not admin_email:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Missing required fields: tenant_name and admin_email'
                })
            }

        # Validate template exists
        valid_templates = ['slack_v1.0', 'crm_v1.0', 'ecommerce_v1.0']
        if template not in valid_templates:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': f'Invalid template. Choose from: {", ".join(valid_templates)}'
                })
            }

        # Generate unique IDs
        tenant_id = f"tenant_{uuid.uuid4().hex[:12]}"
        db_uuid = f"db_{uuid.uuid4().hex[:12]}"
        db_path = f"databases/{db_uuid}.db"

        # Generate API key for authentication
        api_key = f"sk_live_{secrets.token_urlsafe(32)}"

        print(f"Creating tenant: {tenant_id} with database: {db_path}, template: {template}")

        # Step 1: Get schema SQL from S3
        schema_s3_path = f"schemas/{template}.sql"

        try:
            schema_obj = s3.get_object(Bucket=PRIMARY_S3_BUCKET, Key=schema_s3_path)
            schema_sql = schema_obj['Body'].read().decode('utf-8')
            print(f"Retrieved schema from s3://{PRIMARY_S3_BUCKET}/{schema_s3_path}")
        except s3.exceptions.NoSuchKey:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': f'Template schema {template} not found in S3. Please upload schemas first.'
                })
            }

        # Step 2: Create SQLite database in /tmp
        local_db_path = f"/tmp/{db_uuid}.db"
        conn = sqlite3.connect(local_db_path)
        cursor = conn.cursor()

        # Execute all CREATE TABLE statements
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        for statement in statements:
            if statement:
                print(f"Executing: {statement[:50]}...")
                cursor.execute(statement)

        conn.commit()
        conn.close()
        print(f"Created SQLite database with {len(statements)} tables")

        # Step 3: Upload database to S3
        s3.upload_file(local_db_path, PRIMARY_S3_BUCKET, db_path)
        print(f"Uploaded database to s3://{PRIMARY_S3_BUCKET}/{db_path}")

        # Upload to standby bucket
        try:
            standby_s3.upload_file(local_db_path, STANDBY_S3_BUCKET, db_path)
            print(f"Uploaded to standby: s3://{STANDBY_S3_BUCKET}/{db_path}")
        except Exception as e:
            print(f"Standby upload failed: {str(e)}")

        # Upload to read only bucket
        try:
            s3.upload_file(local_db_path, READ_ONLY_S3_BUCKET, db_path)
            print(f"Uploaded to read-only: s3://{READ_ONLY_S3_BUCKET}/{db_path}")
        except Exception as e:
            print(f"Read-only upload failed: {str(e)}")

        # Step 4: Insert tenant metadata into DynamoDB
        current_time = datetime.utcnow().isoformat() + 'Z'

        # New attributes for hot cold logic
        storage_tier = 'HOT'              # new tenants start in HOT tier
        last_accessed_at = current_time   # first access time = creation time

        TENANTS_TABLE.put_item(Item={
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'admin_email': admin_email,
            'api_key': api_key,
            'current_db_path': db_path,

            'schema_ref': template,           # Points to template in S3
            'parent_schema_ref': template,    # Original template chosen
            'tenant_type': 'CHILD',           # Follows template

            'created_at': current_time,
            'last_migration_id': '',
            'last_migration_date': '',

            # hot cold attributes
            'storage_tier': storage_tier,
            'last_accessed_at': last_accessed_at
        })
        print(f"Inserted tenant metadata into DynamoDB")

        try:
            REPLICA_METADATA_TABLE.put_item(Item={
                'tenantId': tenant_id,
                'db_path': db_path,
                'api_key': api_key,
                'last_updated_at': current_time,
                'primary_bucket': PRIMARY_S3_BUCKET,
                'standby_bucket': STANDBY_S3_BUCKET,
                'read_only_bucket': READ_ONLY_S3_BUCKET
            })
            print("Inserted replica metadata")
        except Exception as e:
            print(f"Replica metadata failed: {str(e)}")

        # Step 5: Return success response
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            },
            'body': json.dumps({
                'message': 'Tenant created successfully',
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'admin_email': admin_email,
                'api_key': api_key,
                'database_path': f"s3://{PRIMARY_S3_BUCKET}/{db_path}",
                'schema_ref': template,
                'tenant_type': 'CHILD',
                'created_at': current_time,
                'storage_tier': storage_tier,
                'last_accessed_at': last_accessed_at,
                'note': 'Save your API key securely - it will not be shown again'
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            })
        }


# import json
# import boto3
# import sqlite3
# import uuid
# from datetime import datetime

# # Initialize AWS clients
# s3 = boto3.client('s3')
# standby_s3 = boto3.client('s3', region_name='us-east-2')
# dynamodb = boto3.resource('dynamodb')

# # Configuration
# PRIMARY_S3_BUCKET = 'octodb-tenants-bucket'
# READ_ONLY_S3_BUCKET = 'scalable-db-read-only-replica-bucket'
# STANDBY_S3_BUCKET = 'octodb-tenants-standby-bucket'
# TENANTS_TABLE = dynamodb.Table('octodb-tenants')
# SCHEMAS_TABLE = dynamodb.Table('octodb-schemas')
# REPLICA_METADATA_TABLE = dynamodb.Table('tenant-metadata')

# def lambda_handler(event, context):
#     """
#     Lambda function to provision new tenants with their own database.

#     Expected input (POST /tenants):
#     {
#         "tenant_name": "Acme Corporation",
#         "admin_email": "[email protected]"
#     }
#     """

#     try:
#         # Parse request body
#         body = json.loads(event.get('body', '{}'))
#         tenant_name = body.get('tenant_name')
#         admin_email = body.get('admin_email')

#         # Validate inputs
#         if not tenant_name or not admin_email:
#             return {
#                 'statusCode': 400,
#                 'headers': {'Content-Type': 'application/json'},
#                 'body': json.dumps({
#                     'error': 'Missing required fields: tenant_name and admin_email'
#                 })
#             }

#         # Generate unique IDs
#         tenant_id = f"tenant_{uuid.uuid4().hex[:12]}"
#         db_uuid = f"db_{uuid.uuid4().hex[:12]}"
#         db_path = f"databases/{db_uuid}.db"

#         print(f"Creating tenant: {tenant_id} with database: {db_path}")

#         # Step 1: Get v1.0 schema SQL from DynamoDB
#         schema_response = SCHEMAS_TABLE.get_item(Key={'schema_id': 'v1.0'})

#         if 'Item' not in schema_response:
#             return {
#                 'statusCode': 500,
#                 'headers': {'Content-Type': 'application/json'},
#                 'body': json.dumps({
#                     'error': 'v1.0 schema not found in database. Please seed schema first.'
#                 })
#             }

#         schema_sql = schema_response['Item']['schema_sql']
#         print(f"Retrieved v1.0 schema ({len(schema_sql)} characters)")

#         # Step 2: Create SQLite database in /tmp
#         local_db_path = f"/tmp/{db_uuid}.db"
#         conn = sqlite3.connect(local_db_path)
#         cursor = conn.cursor()

#         # Execute all CREATE TABLE statements
#         # Split by semicolon and execute each statement
#         statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
#         for statement in statements:
#             print(f"Executing: {statement[:50]}...")
#             cursor.execute(statement)

#         conn.commit()
#         conn.close()
#         print(f"Created SQLite database with {len(statements)} tables")

#         # Step 3: Upload database to S3
#         s3.upload_file(local_db_path, PRIMARY_S3_BUCKET, db_path)
#         print(f"Uploaded database to s3://{PRIMARY_S3_BUCKET}/{db_path}")
#         # Try to upload to standby bucket
#         try:
#             standby_s3.upload_file(local_db_path, STANDBY_S3_BUCKET, db_path)
#             print(f"Uploaded database to standby bucket: s3://{STANDBY_S3_BUCKET}/{db_path}")
#         except Exception as e:
#             print(f"Failed to upload to standby bucket: {str(e)}")
#         # Try to upload to read-only bucket
#         try:
#             s3.upload_file(local_db_path, READ_ONLY_S3_BUCKET, db_path)
#             print(f"Uploaded database to read-only bucket: s3://{READ_ONLY_S3_BUCKET}/{db_path}")
#         except Exception as e:
#             print(f"Failed to upload to read-only bucket: {str(e)}")


#         # Step 4: Insert tenant metadata into DynamoDB
#         current_time = datetime.utcnow().isoformat() + 'Z'

#         TENANTS_TABLE.put_item(Item={
#             'tenant_id': tenant_id,
#             'tenant_name': tenant_name,
#             'admin_email': admin_email,
#             'current_db_path': db_path,
#             'parent_db_path': '',  # Empty for initial database
#             'schema_version': 'v1.0',
#             'tenant_type': 'CHILD',  # All tenants start as CHILD of v1.0
#             'created_at': current_time,
#             'last_migration_id': '',
#             'last_migration_date': ''
#         })
#         print(f"Inserted tenant metadata into DynamoDB")

#         try:
#             REPLICA_METADATA_TABLE.put_item(Item={
#                 'tenantId': tenant_id,
#                 'db_path': db_path,
#                 'last_updated_at': current_time,
#                 'primary_bucket': PRIMARY_S3_BUCKET,
#                 'standby_bucket': STANDBY_S3_BUCKET,
#                 'read_only_bucket': READ_ONLY_S3_BUCKET
#             })
#             print(f"Inserted replica metadata into DynamoDB")
#         except Exception as e:
#             print(f"Failed to insert replica metadata into DynamoDB: {str(e)}")

#         # Step 5: Return success response
#         return {
#             'statusCode': 201,
#             'headers': {
#             'Content-Type': 'application/json',
#             'Access-Control-Allow-Origin': '*',
#             'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
#             'Access-Control-Allow-Methods': 'GET,PUT,POST,OPTIONS'
#             },
#             'body': json.dumps({
#                 'message': 'Tenant created successfully',
#                 'tenant_id': tenant_id,
#                 'tenant_name': tenant_name,
#                 'admin_email': admin_email,
#                 'database_path': f"s3://{PRIMARY_S3_BUCKET}/{db_path}",
#                 'schema_version': 'v1.0',
#                 'created_at': current_time
#             })
#         }

#     except Exception as e:
#         print(f"Error: {str(e)}")
#         import traceback
#         traceback.print_exc()

#         return {
#             'statusCode': 500,
#             'headers': {
#             'Content-Type': 'application/json',
#             'Access-Control-Allow-Origin': '*',
#             'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
#             'Access-Control-Allow-Methods': 'POST,OPTIONS'
#             },
#             'body': json.dumps({
#                 'error': 'Internal server error',
#                 'details': str(e)
#             })
#         }
