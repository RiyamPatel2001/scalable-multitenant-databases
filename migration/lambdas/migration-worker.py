import json
import os
import re
import sqlite3
import boto3

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")
dynamodb = boto3.resource("dynamodb")

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TENANTS_TABLE = os.environ.get("TENANTS_TABLE", "octodb-tenants")
PRIMARY_BUCKET = os.environ.get("PRIMARY_BUCKET", "octodb-tenants-bucket")
REHYDRATION_FUNCTION = os.environ.get("REHYDRATION_FUNCTION", "rehydration-handler")
EFS_MOUNT_ROOT = os.environ.get("EFS_MOUNT_ROOT", "/mnt/efs")


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


def apply_ops_to_tenant_db(conn, operations):

    try:
        conn.execute("PRAGMA foreign_keys = ON;")
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
                    # conn.execute("COMMIT;")

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
                    # conn.execute("COMMIT;")

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
                    # conn.execute("COMMIT;")

                else:
                    raise ValueError(f"Unsupported op: {typ}")
            if (operations[-1]["op"] != "CREATE_TABLE"):
                conn.commit()

        except Exception:
            conn.rollback()
            raise

        # Dump updated schema.
        # new_sql = "\n".join(conn.iterdump()) + "\n"

        # Push new schema to s3
        # s3.put_object(Bucket=bucket, Key=tenant_s3_key, Body=new_sql.encode("utf-8"))

        # Debug
        # print_schema_debug(new_sql)

    finally:
        conn.close()


def get_tenant_storage_tier(tenant_id: str) -> str:
    table = dynamodb.Table(TENANTS_TABLE)
    resp = table.get_item(Key={"tenant_id": tenant_id})
    item = resp.get("Item") or {}
    return item.get("storage_tier", "COLD")


def invoke_rehydration(tenant_id: str, tenant_name: str, bucket: str, db_key: str):
    target_path = f"{EFS_MOUNT_ROOT}/{db_key}"  # keeps same key structure under /mnt/efs
    payload = {
        "tenant_id": tenant_id,
        "tenant_name": tenant_name,
        "source_bucket": bucket,
        "db_key": db_key,
        "target_path": target_path,
        "source_type": "primary",
        "reason": "MIGRATION_CACHE_REFRESH"
    }

    print(f"[cache-refresh] Invoking {REHYDRATION_FUNCTION} with: {json.dumps(payload)}")

    resp = lambda_client.invoke(
        FunctionName=REHYDRATION_FUNCTION,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    out = json.loads(resp["Payload"].read().decode("utf-8") or "{}")
    if isinstance(out, dict) and out.get("statusCode", 200) >= 400:
        raise RuntimeError(f"Rehydration failed: {out}")
    return out


def handler_one_message(migration_id, bucket, schema_key, tenant_key, operations, tenant_id=None):

    # Use /tmp (Lambda writable space)
    local_in = f"/tmp/in_{migration_id}.sqlite"

    # Download DB
    s3.download_file(bucket, tenant_key, local_in)

    # Apply migration
    conn = sqlite3.connect(local_in)

    apply_ops_to_tenant_db(conn, operations)

    # Upload back (overwrite same key). You may prefer versioned keys for safety.
    s3.upload_file(local_in, bucket, tenant_key)

    # if bucket == PRIMARY_S3_BUCKET and CACHE_REFRESH_FUNCTION and tenant_id:
    #     payload = {
    #         "tenant_id": tenant_id,
    #         "migration_id": migration_id,
    #         "bucket": bucket,
    #         "db_key": tenant_key
    #     }
    #     lambda_client.invoke(
    #         FunctionName=CACHE_REFRESH_FUNCTION,
    #         InvocationType="Event",  # async
    #         Payload=json.dumps(payload).encode("utf-8")
    #     )


def lambda_handler(event, context):
    # bucket = "octodb-tenants-bucket"

    # SQS batch: event["Records"]
    print(event.get("Records", []))
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        # print(body)
        migration_id = body["migrationId"]

        bucket = body["bucket"]
        schema_key = body["schemaS3Key"]
        tenant_key = body["tenantS3Key"]
        operations = body.get("operations", [])
        tenant_id = body.get("tenantId")
        tenant_name = body.get("tenantName", "")

        handler_one_message(migration_id, bucket, schema_key, tenant_key, operations)

        # Refresh EFS hot cache ONLY for the primary tenant DB
        if bucket == PRIMARY_BUCKET and tenant_id and body.get("refreshHotCache", True):
            tier = get_tenant_storage_tier(tenant_id)
            print(f"[cache-refresh] tenant_id={tenant_id} storage_tier={tier}")

            # If you want: refresh only if HOT (recommended)
            if tier == "HOT":
                try:
                    invoke_rehydration(tenant_id, tenant_name, bucket, tenant_key)
                    print("[cache-refresh] Rehydration completed")
                except Exception as e:
                    print(f"[cache-refresh] WARNING: failed: {str(e)}")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'POST,OPTIONS'
        },
        'body': json.dumps('Migration Done')
    }
