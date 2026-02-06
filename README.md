# kusto-mcp

[![CI](https://github.com/animeshkundu/kusto-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/animeshkundu/kusto-mcp/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server for [Azure Data Explorer](https://learn.microsoft.com/en-us/azure/data-explorer/) (Kusto). Zero-config startup with automatic headless-friendly authentication.

## Install

```bash
pip install kusto-mcp
```

Or run directly with `uvx`:

```bash
uvx kusto-mcp
```

## Quick Start

### Claude Code

```bash
claude mcp add kusto -- uvx kusto-mcp
```

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kusto": {
      "command": "uvx",
      "args": ["kusto-mcp"]
    }
  }
}
```

That's it. No cluster URL, credentials, or tenant ID needed at startup — the LLM provides the cluster and database with each tool call, and authentication is handled automatically.

## Authentication

Authentication happens automatically on the first tool call:

1. **Azure CLI** — silent if you've run `az login`
2. **Device code flow** — if Azure CLI is unavailable, the server returns a device code and URL through the LLM, which asks you to authenticate in any browser

Tokens are cached persistently across server restarts.

For single-tenant scenarios, you can optionally pass a tenant hint:

```bash
kusto-mcp --tenant-id YOUR_TENANT_ID
```

Local ADX emulator clusters (`http://` URLs) require no authentication.

## Tools

All tools accept `cluster` and `database` as required parameters.

| Tool | Description |
|------|-------------|
| `list_internal_tables` | List all internal tables in the database |
| `list_external_tables` | List all external tables in the database |
| `list_materialized_views` | List all materialized views in the database |
| `execute_query_internal_table` | Execute a KQL query on an internal table or materialized view |
| `execute_query_external_table` | Execute a KQL query on an external table |
| `retrieve_internal_table_schema` | Get the schema of an internal table or materialized view |
| `retrieve_external_table_schema` | Get the schema of an external table |

## Features

- **Zero-config startup** — no CLI arguments required
- **Multi-cluster** — connections cached per cluster URL
- **Headless-friendly auth** — Azure CLI → device code flow chain with persistent token cache
- **Schema hints on errors** — query failures include the table schema so the LLM can self-correct
- **Structured JSON responses** — `row.to_dict()` output for LLM comprehension
- **Query safety** — 3-minute timeouts and deferred partial query failures

## Development

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest tests/ -v

# Run the server locally
uv run kusto-mcp
```

## License

[MIT](LICENSE)
