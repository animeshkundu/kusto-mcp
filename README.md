<div align="center">

# kusto-mcp

**Query Azure Data Explorer from Claude and other LLMs**

[![CI](https://github.com/animeshkundu/kusto-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/animeshkundu/kusto-mcp/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/animeshkundu/kusto-mcp)](https://github.com/animeshkundu/kusto-mcp/releases/latest)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://animeshkundu.github.io/kusto-mcp/)

A [Model Context Protocol](https://modelcontextprotocol.io/) server for [Azure Data Explorer](https://learn.microsoft.com/en-us/azure/data-explorer/) (Kusto).
<br>Zero config. Headless auth. Multi-cluster.

</div>

---

## Get Started

> **One command. No config files, no credentials, no cluster URLs at startup.**

<table>
<tr><td><b>Claude Code</b></td>
<td>

```bash
claude mcp add kusto -- uvx --from git+https://github.com/animeshkundu/kusto-mcp kusto-mcp
```

</td></tr>
<tr><td><b>Claude Desktop</b></td>
<td>

Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "kusto": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/animeshkundu/kusto-mcp", "kusto-mcp"]
    }
  }
}
```

</td></tr>
<tr><td><b>pip</b></td>
<td>

```bash
pip install git+https://github.com/animeshkundu/kusto-mcp
```

</td></tr>
</table>

## How It Works

```
You: "Show me the top 5 APIs by request count in the last hour"

Claude ──► kusto-mcp ──► Azure Data Explorer
                │
                ├─ Authenticates automatically (Azure CLI or device code)
                ├─ Caches connections per cluster
                ├─ Returns structured JSON
                └─ Includes schema hints if query fails
```

The LLM provides `cluster`, `database`, and `query` with each tool call. The server handles everything else — auth, connections, formatting, error recovery.

## Authentication

Auth is automatic on the first tool call. No setup required.

| Method | When | How |
|--------|------|-----|
| **Azure CLI** | You've run `az login` | Silent, no interaction |
| **Device code** | No CLI session | Server returns a code → LLM asks you to open a browser → done |
| **Cached token** | After first auth | Persistent across restarts, valid ~90 days |

For single-tenant scenarios: `kusto-mcp --tenant-id YOUR_TENANT_ID`
<br>Local ADX emulator (`http://` URLs): no auth needed.

## Tools

All tools require `cluster` and `database` parameters. The LLM provides these automatically.

Table kinds follow Kusto semantics: internal tables are ingested into the cluster, while external tables reference data stored outside the cluster and are queried via `external_table()` with their own `.show external tables` metadata commands. Materialized views are queried like internal tables.

KQL identifiers with spaces or special characters must be referenced using bracket quoting such as `['table-name']` or `["table name"]`. For external tables, use bracket-quoted identifiers (for example, `['table-name']`), and the server will wrap them in `external_table("table-name")`.

When `table_kind='external'`, the server rewrites the leading table reference and any direct `join`/`union` table tokens. For subqueries or let bindings, use `external_table()` explicitly.

| Tool | Description |
|------|-------------|
| `list_tables` | List tables by kind (`internal`, `external`, `materialized_view`, or `all`) |
| `execute_query` | Run KQL; set `table_kind='external'` for external tables |
| `retrieve_table_schema` | Get table schema; set `table_kind='external'` for external tables |

## KQL Coverage Plan

We are growing coverage of KQL query shapes based on the official KQL reference. The current plan is:

1. Table reference forms (plain identifiers, bracket-quoted identifiers, and `external_table()`).
2. Common query operators (`where`, `project`, `summarize`, `join`, `union`).
3. Function and multi-statement forms (`let`, `datatable`, `database()`/`cluster()` references). Today, the rewrite only applies to leading table references, so `let` bindings are not rewritten.
4. Failure coverage (management command rejection, unknown identifiers, schema hints).

Each phase adds data-driven tests that validate the exact query text the MCP sends to Kusto.

## Why kusto-mcp?

| Feature | Detail |
|---------|--------|
| **Zero config** | No CLI args. No env vars. Just start it. |
| **Multi-cluster** | Query different clusters in the same session. Connections cached. |
| **Headless auth** | Works on remote machines, SSH sessions, containers — anywhere without a browser. |
| **Self-correcting** | Query errors include the table schema so the LLM fixes itself. |
| **Structured output** | JSON with column names and typed values, not opaque strings. |
| **Safe defaults** | 3-min timeouts, management command blocking, deferred partial failures. |

## Releases

Releases are **automatic**. When the `version` in `pyproject.toml` is bumped and pushed to `main`, CI creates a GitHub release with the built wheel and sdist attached.

To release a new version:
1. Bump `version` in `pyproject.toml`
2. Push to `main`
3. Done — CI handles the rest

## Development

```bash
git clone https://github.com/animeshkundu/kusto-mcp && cd kusto-mcp
uv sync --dev        # install deps
uv run pytest -v     # run tests
uv run kusto-mcp     # start server
```

## License

[MIT](LICENSE)
