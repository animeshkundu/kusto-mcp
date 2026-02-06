## Kusto MCP Server

A MCP server that provides access to Azure Data Explorer (ADX) clusters. Supports zero-config startup with automatic headless-friendly authentication.

### Tools

The following tools are provided by the server. All tools accept `cluster` and `database` as required parameters.

- **list tables:**
    - `list_internal_tables`: list all internal tables in the database
    - `list_external_tables`: list all external tables in the database
    - `list_materialized_views`: list all materialized views in the database
- **execute query:**
    - `execute_query_internal_table`: execute a KQL query on an internal table or materialized view
    - `execute_query_external_table`: execute a KQL query on an external table
- **get table schema:**
    - `retrieve_internal_table_schema`: get the schema of an internal table or materialized view
    - `retrieve_external_table_schema`: get the schema of an external table

### Authentication

The server supports automatic authentication with no configuration required.

#### Default (recommended)

Just start the server with no arguments. Authentication happens automatically on the first tool call:

1. **Azure CLI** — if you have run `az login`, authentication is silent
2. **Device code flow** — if Azure CLI is not available, the server returns a device code and URL. The LLM will ask you to open a browser and enter the code to authenticate.

Tokens are cached persistently, so you only need to authenticate once across server restarts.

#### Single-tenant hint

If you need to target a specific Azure tenant:

```bash
kusto-mcp --tenant-id YOUR_TENANT_ID
```

### Claude Code

```bash
claude mcp add kusto -- uvx --from git+https://github.com/animeshkundu/kusto-mcp kusto-mcp
```

### Claude Desktop configuration

```json
{
  "mcpServers": {
    "kusto": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/animeshkundu/kusto-mcp",
        "kusto-mcp"
      ]
    }
  }
}
```

When using Azure Data Explorer emulator locally, no authentication is needed — just provide an `http://` cluster URL in your tool calls.

### Features

- **Zero-config startup** — no CLI arguments required
- **Multi-cluster** — the LLM provides the cluster URL with each tool call; connections are cached per cluster
- **Headless-friendly auth** — device code flow works on remote/SSH machines
- **Persistent token cache** — no re-authentication across server restarts
- **Schema hints on errors** — when a query fails, the error response includes the table schema to help the LLM self-correct
- **Structured JSON responses** — query results returned as structured JSON for better LLM comprehension
- **Query safety** — 3-minute timeouts and deferred partial query failures
