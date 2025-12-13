import os
import json
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TENANT_METADATA_TABLE = os.environ.get('TENANT_METADATA_TABLE', 'octodb-tenants')


def lambda_handler(event, context):
    """
    Expected payload from callers (read handler or write handler):

    {
        "tenant_id": "tenant-123",
        "tenant_name": "Tandon",
        "source_bucket": "octodb-tenants-bucket",
        "db_key": "databases/db_123456789abc.db",
        "target_path": "/mnt/efs/databases/db_123456789abc.db",
        "source_type": "primary"  # or "read_replica"
    }
    """
    print("Rehydration event:", json.dumps(event))

    try:
        tenant_id = event.get('tenant_id')
        tenant_name = event.get('tenant_name')
        source_bucket = event.get('source_bucket')
        db_key = event.get('db_key')
        target_path = event.get('target_path')
        source_type = event.get('source_type', 'primary')

        if not tenant_id or not source_bucket or not db_key or not target_path:
            raise ValueError(
                f'Missing required fields. tenant_id={tenant_id}, '
                f'source_bucket={source_bucket}, db_key={db_key}, target_path={target_path}'
            )

        print(
            f"Starting rehydration for tenant_id={tenant_id}, "
            f"tenant_name={tenant_name}, source_bucket={source_bucket}, "
            f"db_key={db_key}, target_path={target_path}, source_type={source_type}"
        )

        # Ensure directory on EFS exists
        target_dir = os.path.dirname(target_path)
        if target_dir:
            print(f"Ensuring EFS directory exists: {target_dir}")
            os.makedirs(target_dir, exist_ok=True)

        # Download DB file from S3 to EFS
        print(f"Downloading s3://{source_bucket}/{db_key} to {target_path}")
        try:
            s3.download_file(source_bucket, db_key, target_path)
        except ClientError as e:
            print(f"ERROR: Failed to download from S3: {e}")
            raise

        print(f"Download complete. Verifying file exists at {target_path}")
        if not os.path.exists(target_path):
            raise RuntimeError(f"File does not exist at {target_path} after download")

        # Update tenant metadata: storage_tier and last_accessed_at
        tenant_table = dynamodb.Table(TENANT_METADATA_TABLE)

        now = datetime.now(timezone.utc).isoformat()
        update_expr = 'SET storage_tier = :tier, last_accessed_at = :ts'
        expr_vals = {
            ':tier': 'HOT',
            ':ts': now
        }

        # Optionally ensure current_db_path is set
        update_expr += ', current_db_path = if_not_exists(current_db_path, :db_key)'
        expr_vals[':db_key'] = db_key

        try:
            print(f"Updating tenant metadata for tenant_id={tenant_id}")
            tenant_table.update_item(
                Key={'tenant_id': tenant_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_vals
            )
            print(f"Tenant metadata updated for tenant_id={tenant_id}")
        except ClientError as e:
            print(f"WARNING: Failed to update tenant metadata for tenant_id={tenant_id}: {e}")

        # Return a normal 200 so caller does not see FunctionError
        body = {
            'success': True,
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'storage_tier': 'HOT',
            'db_key': db_key,
            'target_path': target_path,
            'source_type': source_type
        }

        print("Rehydration completed successfully:", json.dumps(body))
        return {
            'statusCode': 200,
            'body': json.dumps(body)
        }

    except Exception as e:
        # Log and re-raise so the caller sees FunctionError (for now)
        print(f"ERROR: Rehydration failed: {e}")
        raise
