import os
import json
import boto3
import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TENANT_METADATA_TABLE = os.environ.get('TENANT_METADATA_TABLE', 'octodb-tenants')
REPLICA_METADATA_TABLE = os.environ.get('REPLICA_METADATA_TABLE', 'tenant-metadata')
EFS_MOUNT_DIR = os.environ.get('EFS_MOUNT_DIR', '/mnt/efs')
COLD_THRESHOLD_HOURS = float(os.environ.get('COLD_THRESHOLD_HOURS', '24.0'))


def _parse_ts_utc(value):
    """
    Returns a timezone-aware datetime in UTC, or raises.
    Accepts:
      - Decimal epoch seconds
      - ISO string (with Z or offset, or naive)
    """
    if value is None:
        raise ValueError("timestamp is None")

    # DynamoDB might store as Decimal (epoch seconds)
    if isinstance(value, Decimal):
        return datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)

    s = str(value).strip()

    # Normalize trailing Z -> +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = datetime.datetime.fromisoformat(s)

    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)

    return dt


def lambda_handler(event, context):
    print(f'Starting cold storage manager job with threshold {COLD_THRESHOLD_HOURS} hours')

    tenant_table = dynamodb.Table(TENANT_METADATA_TABLE)
    replica_table = dynamodb.Table(REPLICA_METADATA_TABLE)

    # Use timezone-aware UTC timestamps everywhere
    now = datetime.datetime.now(datetime.timezone.utc)
    idle_cutoff = now - datetime.timedelta(hours=COLD_THRESHOLD_HOURS)

    demoted_tenants = []

    scan_kwargs = {}
    while True:
        resp = tenant_table.scan(**scan_kwargs)
        items = resp.get('Items', [])

        for tenant in items:
            tenant_id = tenant.get('tenant_id')
            tenant_name = tenant.get('tenant_name')
            storage_tier = (tenant.get('storage_tier') or 'COLD').upper()

            if storage_tier != 'HOT':
                continue

            last_accessed_at = tenant.get('last_accessed_at') or tenant.get('created_at')
            if not last_accessed_at:
                print(f'Skipping tenant_id={tenant_id} because last_accessed_at and created_at are missing')
                continue

            try:
                last_ts = _parse_ts_utc(last_accessed_at)
            except Exception:
                print(f'Skipping tenant_id={tenant_id} because last_accessed_at parse failed: {last_accessed_at}')
                continue

            # Now both are UTC-aware, safe to compare
            if last_ts > idle_cutoff:
                continue

            print(f'Tenant {tenant_id} is idle (last access {last_ts.isoformat()}), demoting to COLD')

            db_key = tenant.get('current_db_path')
            primary_bucket = None

            try:
                repl_resp = replica_table.get_item(Key={'tenantId': tenant_id})
                repl_item = repl_resp.get('Item')
            except ClientError as e:
                print(f'WARNING: Failed to fetch replica metadata for tenant_id={tenant_id}: {e}')
                repl_item = None

            if repl_item:
                if not db_key:
                    db_key = repl_item.get('db_path')
                primary_bucket = repl_item.get('primary_bucket')

            if not db_key:
                print(f'WARNING: Could not determine db_key for tenant_id={tenant_id}. Skipping demotion.')
                continue
            if not primary_bucket:
                print(f'WARNING: Could not determine primary_bucket for tenant_id={tenant_id}. Skipping demotion.')
                continue

            efs_path = os.path.join(EFS_MOUNT_DIR, db_key)

            if os.path.exists(efs_path):
                try:
                    print(f'Uploading EFS DB {efs_path} to S3 {primary_bucket}/{db_key} for tenant_id={tenant_id}')
                    s3.upload_file(efs_path, primary_bucket, db_key)
                    print(f'Upload to S3 complete for tenant_id={tenant_id}')
                except Exception as e:
                    print(f'ERROR: Failed to upload EFS DB to S3 for tenant_id={tenant_id}: {e}')
                    continue

                try:
                    os.remove(efs_path)
                    print(f'Removed EFS file {efs_path} for tenant_id={tenant_id}')
                except Exception as e:
                    print(f'WARNING: Failed to delete EFS file {efs_path} for tenant_id={tenant_id}: {e}')
            else:
                print(
                    f'INFO: EFS file {efs_path} does not exist for tenant_id={tenant_id}. Assuming DB is already only in S3.')

            try:
                now_iso = now.isoformat()  # includes +00:00
                tenant_table.update_item(
                    Key={'tenant_id': tenant_id},
                    UpdateExpression='SET storage_tier = :tier, last_demoted_at = :ts',
                    ExpressionAttributeValues={':tier': 'COLD', ':ts': now_iso}
                )
                print(f'Tenant {tenant_id} storage_tier set to COLD')
            except ClientError as e:
                print(f'ERROR: Failed to update tenant storage_tier for tenant_id={tenant_id}: {e}')
                continue

            demoted_tenants.append({
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'last_accessed_at': str(last_accessed_at)
            })

        last_key = resp.get('LastEvaluatedKey')
        if not last_key:
            break
        scan_kwargs['ExclusiveStartKey'] = last_key

    result = {
        'success': True,
        'threshold_hours': COLD_THRESHOLD_HOURS,
        'demoted_count': len(demoted_tenants),
        'demoted_tenants': demoted_tenants
    }

    print('Cold storage manager summary:', json.dumps(result, indent=2))
    return result
