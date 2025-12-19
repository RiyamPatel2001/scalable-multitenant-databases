import json
import uuid
import re
import datetime as dt
import os
import boto3
import sqlite3
from typing import Any, Dict, List, Optional


# Connect to s3
s3 = boto3.client("s3")

# Connect to DynamoDB
dynamodb = boto3.client("dynamodb")
TENANT_TABLE_NAME = os.environ["TENANT_TABLE_NAME"]
SCHEMA_TABLE_NAME = os.environ["SCHEMA_TABLE_NAME"]


# Connect to SQS
sqs = boto3.client("sqs")
QUEUE_URL = os.environ["MIGRATION_QUEUE_URL"]

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qident(name: str) -> str:
    if not SAFE_IDENT.match(name):
        raise ValueError(f"Unsafe identifier: {name}")
    return f'"{name}"'


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,)
    )
    return cur.fetchone() is not None


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({qident(table)})")
    return any(row[1] == column for row in cur.fetchall())  # row[1]=name


def sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    # string
    return "'" + str(v).replace("'", "''") + "'"


def print_schema_debug(new_sql: str, max_lines: int = 100):
    lines = new_sql.splitlines()
    print("========== UPDATED SCHEMA SQL (BEGIN) ==========")
    for line in lines[:max_lines]:
        print(line)
    if len(lines) > max_lines:
        print(f"... ({len(lines) - max_lines} more lines)")
    print("=========== UPDATED SCHEMA SQL (END) ===========")


def apply_ops_to_schema_sql(bucket, schema_s3_key, operations):
    obj = s3.get_object(Bucket=bucket, Key=schema_s3_key)
    old_sql = obj["Body"].read().decode("utf-8")
    conn = sqlite3.connect(":memory:")
    try:
        old_sql = (old_sql or "").strip()
        if old_sql:
            conn.executescript(old_sql + ("" if old_sql.endswith(";") else ";"))

        conn.execute("BEGIN;")
        try:
            for op in operations:
                typ = op["op"]
                print(f"Operation: {typ}")
                if typ == "CREATE_TABLE":
                    sql = op.get("sql")
                    if not sql:
                        raise ValueError("CREATE_TABLE requires op['sql']")
                    # For redundancy: if you also include op['table'], you can skip when exists.
                    conn.executescript(sql.strip() + ("" if sql.strip().endswith(";") else ";"))

                elif typ == "DROP_TABLE":
                    table = op["table"]
                    conn.execute(f"DROP TABLE IF EXISTS {qident(table)};")

                elif typ == "RENAME_TABLE":
                    old_name = op["table"]
                    new_name = op.get("new_name") or op.get("name")
                    if not new_name:
                        raise ValueError("RENAME_TABLE requires 'new_name'")
                    if not table_exists(conn, old_name):
                        raise ValueError(f"RENAME_TABLE: table does not exist: {old_name}")
                    if table_exists(conn, new_name):
                        # already renamed or conflict → for your project, treat as redundant
                        continue
                    conn.execute(f"ALTER TABLE {qident(old_name)} RENAME TO {qident(new_name)};")

                elif typ == "ADD_COLUMN":
                    table = op["table"]
                    col = op["column"]
                    col_name = col["name"]
                    col_type = col.get("type", "")
                    nullable = bool(col.get("nullable", True))
                    default = col.get("default", None)

                    if not table_exists(conn, table):
                        raise ValueError(f"ADD_COLUMN: table does not exist: {table}")
                    if column_exists(conn, table, col_name):
                        continue  # redundant

                    sql = f"ALTER TABLE {qident(table)} ADD COLUMN {qident(col_name)} {col_type}".rstrip()
                    if nullable is False:
                        # SQLite: NOT NULL add should have DEFAULT (practical rule)
                        if default is None:
                            raise ValueError("ADD_COLUMN nullable=false requires default in SQLite")
                        sql += " NOT NULL"
                    if default is not None:
                        sql += f" DEFAULT {sql_literal(default)}"
                    conn.execute(sql + ";")

                else:
                    raise ValueError(f"Unsupported op: {typ}")

            if (operations[-1]["op"] != "CREATE_TABLE"):
                conn.commit()

        except Exception:
            conn.rollback()
            raise

        # Dump updated schema.
        new_sql = "\n".join(conn.iterdump()) + "\n"

        # Push new schema to s3
        s3.put_object(Bucket=bucket, Key=schema_s3_key, Body=new_sql.encode("utf-8"))

        # Debug
        # print_schema_debug(new_sql)

    finally:
        conn.close()


def send_tenant_migration_job_to_sqs(
    id,
    bucket,
    schema_s3_key,
    tenant_s3_key,
    operations,
    tenant_id=None,          # NEW
    tenant_name=None,        # NEW
    dry_run=False
):
    migration_id = f"mig_{uuid.uuid4().hex[:12]}"
    requested_at = dt.datetime.now(dt.timezone.utc).isoformat()

    message_body = {
        "migrationId": migration_id,
        "requestedAt": requested_at,
        "bucket": bucket,
        "schemaS3Key": schema_s3_key,
        "tenantS3Key": tenant_s3_key,
        "operations": operations,
        "tenantId": tenant_id,                 # ok if None in body
        "tenantName": tenant_name,
        "refreshHotCache": True
    }

    msg_attrs = {
        "migrationId": {"DataType": "String", "StringValue": migration_id},
        "id": {"DataType": "String", "StringValue": str(id)},
    }

    # Only add if non-empty, otherwise SQS throws InvalidParameterValue
    if tenant_id and str(tenant_id).strip():
        msg_attrs["tenantId"] = {"DataType": "String", "StringValue": str(tenant_id)}

    dedup_id = f"{id}:{migration_id}"

    resp = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message_body),
        MessageGroupId=str(id),
        MessageDeduplicationId=dedup_id,
        MessageAttributes=msg_attrs
    )
    print(resp)

    return {"migrationId": migration_id, "sqsMessageId": resp["MessageId"]}


def lambda_handler(event, context):

    scope = event["scope"]
    operations = event["operations"]

    if (scope == "TENANT"):
        tenantName = event["tenantName"]

        response_tenant = dynamodb.query(
            TableName=TENANT_TABLE_NAME,
            IndexName="Tenant_Name_Index",
            KeyConditionExpression="tenant_name = :tname",
            ExpressionAttributeValues={
                ":tname": {"S": tenantName}
            }
        )

        tenant_id = response_tenant["Items"][0]["tenant_id"]["S"]

        response = dynamodb.get_item(TableName=TENANT_TABLE_NAME, Key={"tenant_id": {"S": tenant_id}})

        # Validate request
        try:
            if (event["apiKey"] != response["Item"]["api_key"]["S"]):
                return {
                    "statusCode": 401,
                    "body": json.dumps({
                        "message": "Unauthorized Access"
                    }),
                }
        except:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Tenant not found"
                }),
            }

        tenant_s3_path = response["Item"]["current_db_path"]['S']

        parent_schema = response["Item"]["parent_schema_ref"]['S']

        # Copy the schema and rename the new schema
        bucket = "octodb-tenants-bucket"
        read_only_replica_bucket = "scalable-db-read-only-replica-bucket"
        standby_replica_bucket = "octodb-tenants-standby-bucket"

        source_key = f"schemas/{parent_schema}.sql"

        dest_key = f"schemas/{tenant_id}.sql"
        copy_source = {
            "Bucket": bucket,
            "Key": source_key
        }
        print(copy_source)  # TODO: Redoing the migration again messes it up
        print(dest_key)

        if (parent_schema != "NULL"):
            s3.copy_object(Bucket=bucket, CopySource=copy_source, Key=dest_key)

        # Apply migration to the copied schema
        apply_ops_to_schema_sql(bucket, dest_key, operations)

        # Copy the schema to standby replica bucket
        standby_copy_source = {
            "Bucket": bucket,
            "Key": dest_key
        }
        s3.copy_object(Bucket=standby_replica_bucket, CopySource=standby_copy_source, Key=dest_key)

        # Send tenant to migration SQS job
        out = send_tenant_migration_job_to_sqs(
            id=tenant_id,
            bucket=bucket,
            schema_s3_key=dest_key,
            tenant_s3_key=tenant_s3_path,
            operations=operations,
            tenant_id=tenant_id,           # ADD THIS
            tenant_name=tenantName         # optional
        )

        # Send read-only replica to migration SQS job
        out_read_only_replica = send_tenant_migration_job_to_sqs(
            id=f"{tenant_id}-read-only-replica",
            bucket=read_only_replica_bucket,
            schema_s3_key=dest_key,
            tenant_s3_key=tenant_s3_path,
            operations=operations,
            tenant_id=tenant_id,           # ADD THIS
            tenant_name=tenantName         # optional
        )

        # Send standby replica to migration SQS job
        out_standby_replica = send_tenant_migration_job_to_sqs(
            id=f"{tenant_id}-standby-replica",
            bucket=standby_replica_bucket,
            schema_s3_key=dest_key,
            tenant_s3_key=tenant_s3_path,
            operations=operations,
            tenant_id=tenant_id,           # ADD THIS
            tenant_name=tenantName         # optional
        )

        # Add new schema to Dynamo
        dynamodb.put_item(
            TableName=SCHEMA_TABLE_NAME,
            Item={
                "schema_id": {"S": tenant_id},
                "created_at": {"S": dt.datetime.now(dt.timezone.utc).isoformat()},
                "created_by": {"S": "admin"},
                "s3_path": {"S": dest_key},
                "schema_name": {"S": tenant_id},
                "schema_type": {"S": "TEMPLATE"},
                "tenant_id": {"S": tenant_id}
            }
        )

        # Update the parent_schema_ref
        dynamodb.update_item(
            TableName=TENANT_TABLE_NAME,
            Key={"tenant_id": {"S": tenant_id}},
            UpdateExpression="SET parent_schema_ref = :s",
            ExpressionAttributeValues={":s": {"S": "NULL"}}
        )

    elif (scope == "TEMPLATE"):

        schemaName = event["schemaName"]
        response_schema = dynamodb.query(
            TableName=SCHEMA_TABLE_NAME,
            IndexName="schema_name_index",
            KeyConditionExpression="schema_name = :sid",
            ExpressionAttributeValues={
                ":sid": {"S": schemaName}
            }
        )

        schema_id = response_schema["Items"][0]["schema_id"]["S"]
        response_schema = dynamodb.get_item(TableName=SCHEMA_TABLE_NAME, Key={"schema_id": {"S": schema_id}})

        # #Validate request
        # try:
        #     if (event["apiKey"] != response_schema["Item"]["api_key"]["S"]):
        #         return {
        #             "statusCode": 401,
        #             "body": json.dumps({
        #                 "message": "Unauthorized Access"
        #             }),
        #         }

        try:
            schema_s3_path = response_schema["Item"]["s3_path"]['S']
        except:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Schema not found"
                }),
            }

        # Check the tenant table for tenants with this schema id
        response_tenants = dynamodb.query(
            TableName=TENANT_TABLE_NAME,
            IndexName="parent_schema_index",
            KeyConditionExpression="parent_schema_ref = :sid",
            ExpressionAttributeValues={
                ":sid": {"S": schema_id}
            }
        )

        tenant_ids_with_given_schema = [item["tenant_id"]["S"] for item in response_tenants["Items"]]

        # Apply migration to the schema
        bucket = "octodb-tenants-bucket"
        read_only_replica_bucket = "scalable-db-read-only-replica-bucket"
        standby_replica_bucket = "octodb-tenants-standby-bucket"

        apply_ops_to_schema_sql(bucket, schema_s3_path, operations)

        # Copy updated schema to standby replica bucket
        standby_copy_source = {
            "Bucket": bucket,
            "Key": schema_s3_path
        }
        s3.copy_object(Bucket=standby_replica_bucket, CopySource=standby_copy_source, Key=schema_s3_path)

        for tenant_id in tenant_ids_with_given_schema:

            response_tenant = dynamodb.get_item(TableName=TENANT_TABLE_NAME, Key={"tenant_id": {"S": tenant_id}})
            tenant_s3_path = response_tenant["Item"]["current_db_path"]["S"]

            out_tenant = send_tenant_migration_job_to_sqs(
                id=f"{schema_id}:{tenant_id}",
                bucket=bucket,
                schema_s3_key=schema_s3_path,
                tenant_s3_key=tenant_s3_path,
                operations=operations,
                tenant_id=tenant_id,
            )

            out_read_only_replica = send_tenant_migration_job_to_sqs(
                id=f"{tenant_id}-read-only-replica",
                bucket=read_only_replica_bucket,
                schema_s3_key=dest_key,
                tenant_s3_key=tenant_s3_path,
                operations=operations,
                tenant_id=tenant_id,           # ADD THIS
                tenant_name=tenantName,         # optional
            )

            out_standby_replica = send_tenant_migration_job_to_sqs(
                id=f"{tenant_id}-standby-replica",
                bucket=standby_replica_bucket,
                schema_s3_key=dest_key,
                tenant_s3_key=tenant_s3_path,
                operations=operations,
                tenant_id=tenant_id,           # ADD THIS
                tenant_name=tenantName         # optional
            )

    return {
        'statusCode': 202,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'POST,OPTIONS'
        },
        'body': json.dumps("Processed Migration request")
    }
