import json
import uuid
import re
import datetime as dt
import os
import boto3
import sqlite3
from typing import Any, Dict, List, Optional


#Connect to s3
s3 = boto3.client("s3")

#Connect to DynamoDB
dynamodb = boto3.client("dynamodb")
TENANT_TABLE_NAME = os.environ["TENANT_TABLE_NAME"]
SCHEMA_TABLE_NAME = os.environ["SCHEMA_TABLE_NAME"]


#Connect to SQS
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

                if typ == "CREATE_TABLE":
                    sql = op.get("sql")
                    if not sql:
                        raise ValueError("CREATE_TABLE requires op['sql']")
                    # For redundancy: if you also include op['table'], you can skip when exists.
                    conn.executescript(sql.strip() + ("" if sql.strip().endswith(";") else ";"))

                elif typ == "DROP_TABLE":
                    table = op["table"]
                    conn.execute(f"DROP TABLE IF EXISTS {qident(table)};")
                    conn.execute("COMMIT;")

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
                    conn.execute("COMMIT;")

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
                    conn.execute("COMMIT;")

                else:
                    raise ValueError(f"Unsupported op: {typ}")

            
        except Exception:
            raise

        # Dump updated schema.
        new_sql = "\n".join(conn.iterdump()) + "\n"

        #Push new schema to s3
        s3.put_object(Bucket=bucket, Key=schema_s3_key, Body=new_sql.encode("utf-8"))

        #Debug
        print_schema_debug(new_sql)

    finally:
        conn.close()


def send_tenant_migration_job_to_sqs(id, schema_s3_key, tenant_s3_key, operations, dry_run=False):
    migration_id = f"mig_{uuid.uuid4().hex[:12]}"
    requested_at = dt.datetime.now(dt.timezone.utc).isoformat()

    message_body = {
        "migrationId": migration_id,
        "requestedAt": requested_at,
        "schemaS3Key": schema_s3_key,
        "tenantS3Key": tenant_s3_key,
        "operations": operations
    }

    dedup_id = f"{id}:{migration_id}"

    resp = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message_body),
        MessageGroupId=str(id), 
        MessageDeduplicationId=dedup_id,
        MessageAttributes={
            "migrationId": {"DataType": "String", "StringValue": migration_id},
            "id": {"DataType": "String", "StringValue": str(id)},
        },
    )
    print(resp)

    return {
        "migrationId": migration_id,
        "sqsMessageId": resp["MessageId"],
    }


def lambda_handler(event, context):


    scope = event["scope"]
    operations  = event["operations"]

    if(scope == "TENANT"):
        tenant_id = event["tenantId"]
        response = dynamodb.get_item(TableName=TENANT_TABLE_NAME, Key={"tenant_id": {"S": tenant_id}})
        
        tenant_s3_path = response["Item"]["current_db_path"]
        parent_schema = response["Item"]["parent_schema_ref"]

        #Copy the schema and rename the new schema
        bucket = "octodb-tenants-bucket"
        source_key = f"schemas/{parent_schema}"

        dest_key = f"schemas/{tenant_id}"
        copy_source = {
            "Bucket": bucket,
            "Key": source_key
        }

        s3.copy_object(Bucket=bucket, CopySource=copy_source, Key=dest_key)

        #Apply migration to the copied schema
        apply_ops_to_schema_sql(bucket, dest_key, operations)



        out = send_tenant_migration_job_to_sqs(
            id=tenant_id,
            schema_s3_key=dest_key,
            tenant_s3_key=tenant_s3_path,
            operations=operations,
        )

        #Update the parent_schema_ref
        dynamodb.update_item(
            TableName=TENANT_TABLE_NAME,
            Key={"tenant_id": {"S": tenant_id}},
            UpdateExpression="SET parent_schema_ref = :s",
            ExpressionAttributeValues={":s": {"S": "NULL"}}
        )


    elif(scope == "TEMPLATE"):
        schema_id = event["schemaId"]
        response_schema = dynamodb.get_item(TableName=SCHEMA_TABLE_NAME, Key={"schema_id": {"S": schema_id}})
        schema_s3_path = response_schema["Item"]["s3_path"]['S']
        #Check the tenant table for tenants with this schema id
        response_tenants = dynamodb.query(
            TableName=TENANT_TABLE_NAME,
            IndexName="parent_schema_index",
            KeyConditionExpression="parent_schema_ref = :sid",
            ExpressionAttributeValues={
                ":sid": {"S": schema_id}
            }
        )
        tenant_ids_with_given_schema = [item["tenant_id"]["S"] for item in response_tenants["Items"]]
        
        #Apply migration to the schema
        bucket = "octodb-tenants-bucket"
        apply_ops_to_schema_sql(bucket, schema_s3_path, operations)


        for tenant_id in tenant_ids_with_given_schema:

            response_tenant = dynamodb.get_item(TableName=TENANT_TABLE_NAME, Key={"tenant_id": {"S": tenant_id}})
            tenant_s3_path = response_tenant["Item"]["current_db_path"]

            out_tenant = send_tenant_migration_job_to_sqs(
                id = f"{schema_id}:{tenant_id}",
                schema_s3_key = schema_s3_path,
                tenant_s3_key = tenant_s3_path,
                operations = operations,
            )


    return {
        'statusCode': 202,
        "headers": {"Content-Type": "application/json"},
        'body': json.dumps("Processed Migration request")
    }
