# Migration

## Tenant schema migrations (API Body)

Send a JSON payload like the following in an API body to apply a migration to a tenant SQLite database:

```json
{
  "scope": "TENANT",
  "tenantName": "Tandon",
  "apiKey": "sk_live_PAC_x0LUbWVUZLzUeBitoTsDbt83bQSNmqf3uaTx-Yo",
  "operations": [
    {
      "op": "CREATE_TABLE",
      "table": "Payments",
      "sql": "CREATE TABLE IF NOT EXISTS Payments (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, amount REAL NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);"
    },
    {
      "op": "ADD_COLUMN",
      "table": "Users",
      "column": { "name": "phone", "type": "TEXT", "nullable": true }
    },
    {
      "op": "RENAME_TABLE",
      "table": "Cart",
      "name": "ShoppingCart"
    },
    {
      "op": "DROP_TABLE",
      "table": "OrderItems"
    }
  ]
}
```

## Template schema migrations (API Body)
Send a JSON payload like the following in an API body to apply a migration to the template SQLite database:

```json
{
  "scope": "TEMPLATE",
  "schemaName": "E-commerce Platform",
  "apiKey": "sk_live_PAC_x0LUbWVUZLzUeBitoTsDbt83bQSNmqf3uaTx-Yo",
  "operations": [
    {
      "op": "CREATE_TABLE",
      "table": "Payments",
      "sql": "CREATE TABLE IF NOT EXISTS Payments (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, amount REAL NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);"
    },
    {
      "op": "ADD_COLUMN",
      "table": "Users",
      "column": { "name": "phone", "type": "TEXT", "nullable": true }
    },
    {
      "op": "RENAME_TABLE",
      "table": "Cart",
      "name": "ShoppingCart"
    },
    {
      "op": "DROP_TABLE",
      "table": "OrderItems"
    }
  ]
}
```
