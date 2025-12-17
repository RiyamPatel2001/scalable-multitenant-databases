import json
import os
import re
import sqlite3
import boto3

s3 = boto3.client("s3")

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_ident(name: str) -> str:
    # SQLite identifier quoting; validate to avoid injection via table/column names
    if not SAFE_IDENT.match(name):
        raise ValueError(f"Unsafe identifier: {name}")
    return f'"{name}"'

def apply_operations(conn: sqlite3.Connection, operations: list[dict]):
    # Make DDL transactional where possible
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("BEGIN;")
    try:
        for op in operations:
            op_type = op.get("op")
            if op_type == "ADD_COLUMN":
                table = quote_ident(op["table"])
                col = op["column"]
                col_name = quote_ident(col["name"])
                col_type = col["type"]  # assume trusted enum from your API layer

                nullable = bool(col.get("nullable", True))
                default = col.get("default")

                # SQLite supports: ALTER TABLE ... ADD COLUMN ...
                # NOTE: SQLite has constraints/limitations; NOT NULL often requires DEFAULT.
                ddl = f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                if not nullable:
                    if default is None:
                        raise ValueError("ADD_COLUMN with nullable=false requires a default for SQLite")
                    ddl += " NOT NULL"
                if default is not None:
                    # default can be numeric or quoted string; safest is to pass literals carefully.
                    # Here we handle basic types; expand if you need more.
                    if isinstance(default, (int, float)):
                        ddl += f" DEFAULT {default}"
                    else:
                        ddl += " DEFAULT ?"
                        conn.execute(ddl, (str(default),))
                        continue

                conn.execute(ddl)

            elif op_type == "CREATE_TABLE":
                # Expect op["sql"] to contain full CREATE TABLE statement from trusted source
                sql = op["sql"]
                conn.executescript(sql)

            elif op_type == "DROP_TABLE":
                table = quote_ident(op["table"])
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            
            elif op_type == "RENAME_TABLE":
                print(op)
                table = quote_ident(op["table"])
                new_name = quote_ident(op["new_name"])
                conn.execute(f"ALTER TABLE {table} RENAME TO {new_name}")

            else:
                raise ValueError(f"Unsupported op: {op_type}")

        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise

def handler_one_message(migration_id, bucket, key, operations):


    # Use /tmp (Lambda writable space)
    local_in = f"/tmp/in_{migration_id}.sqlite"

    # Download DB
    s3.download_file(bucket, key, local_in)

    # Apply migration
    conn = sqlite3.connect(local_in)
    try:
        apply_operations(conn, operations)
    finally:
        conn.close()

    # Upload back (overwrite same key). You may prefer versioned keys for safety.
    s3.upload_file(local_in, bucket, key)

def lambda_handler(event, context):
    bucket = "octodb-tenants-bucket"

    # SQS batch: event["Records"]
    for record in event.get("Records", []):
        body = json.loads(record["body"])

        migration_id = body["migrationId"]
        key = body["s3Path"]["S"]
        operations = body.get("operations", [])


        handler_one_message(migration_id, bucket, key, operations)

    return {
        'statusCode': 200,
        'body': json.dumps('Migration Done')
    }

