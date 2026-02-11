import json
import logging
import os
import re
from datetime import timedelta
from typing import Any, List

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data import ClientRequestProperties

from mcp_server_kusto.auth import build_credential, build_kcsb, get_pending_device_code

# init logger
if not os.path.exists("logs"):
    os.makedirs("logs")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="logs/mcp_kusto_server.log",
)
logger = logging.getLogger("mcp_kusto_server")

logger.info("Starting MCP Kusto Server")


def _make_request_properties() -> ClientRequestProperties:
    """Build default ClientRequestProperties for safe query execution."""
    properties = ClientRequestProperties()
    properties.set_option(
        properties.request_timeout_option_name, timedelta(minutes=3)
    )
    properties.set_option(
        properties.results_defer_partial_query_failures_option_name, True
    )
    return properties


def _format_results(result_table) -> str:
    """Format query results as structured JSON for LLM consumption."""
    columns = [col.column_name for col in result_table.columns]
    rows = [row.to_dict() for row in result_table]
    return json.dumps(
        {"columns": columns, "row_count": len(rows), "data": rows}, default=str
    )


_TABLE_KIND_ALIASES = {
    "internal": "internal",
    "external": "external",
    "materialized": "materialized_view",
    "materialized_view": "materialized_view",
    "view": "materialized_view",
    "views": "materialized_view",
    "all": "all",
}
_LIST_TABLE_KINDS = {"internal", "external", "materialized_view", "all"}
_QUERY_TABLE_KINDS = {"internal", "external", "materialized_view"}


def _normalize_table_kind(
    table_kind: str | None, *, default: str, allowed: set[str]
) -> str:
    if not table_kind:
        normalized = default
    else:
        key = table_kind.strip().lower().replace(" ", "_").replace("-", "_")
        if key not in _TABLE_KIND_ALIASES:
            raise ValueError(
                "Unknown table_kind. Use internal, external, materialized_view, or all."
            )
        normalized = _TABLE_KIND_ALIASES[key]
    if normalized not in allowed:
        raise ValueError(
            f"table_kind '{normalized}' is not valid here. "
            f"Allowed values: {', '.join(sorted(allowed))}."
        )
    return normalized


class KustoDatabase:
    def __init__(self, credential):
        """
        Connection manager that caches KustoClient instances per cluster.
        :param credential: A ChainedTokenCredential for authentication.
        """
        self._credential = credential
        self._clients: dict[str, KustoClient] = {}

    def _get_client(self, cluster: str) -> KustoClient:
        if cluster not in self._clients:
            kcsb = build_kcsb(cluster, self._credential)
            self._clients[cluster] = KustoClient(kcsb)
        return self._clients[cluster]

    def _try_get_schema_hint(self, cluster: str, database: str, query: str) -> str:
        """Try to fetch the schema of the table referenced in the query."""
        try:
            table_name = query.split("|")[0].strip()
            if not table_name or table_name.startswith("."):
                return ""
            client = self._get_client(cluster)
            response = client.execute(database, f"{table_name} | getschema")
            rows = [row.to_dict() for row in response.primary_results[0]]
            schema_lines = [
                f"  {r.get('ColumnName', '')}: {r.get('ColumnType', '')}"
                for r in rows
            ]
            return f"Schema for '{table_name}':\n" + "\n".join(schema_lines)
        except Exception as schema_err:
            logger.debug(f"Could not fetch schema hint: {schema_err}")
            return ""

    def _parse_external_table_name(self, query: str) -> str:
        prefix = "external_table("
        lower_query = query.lower()
        if not lower_query.startswith(prefix):
            return ""
        rest = query[len(prefix) :].lstrip()
        if not rest:
            return ""
        quote = rest[0]
        if quote not in {"'", '"'}:
            return ""
        name_chars: list[str] = []
        escape_char = "\\"
        escaped = False
        for ch in rest[1:]:
            if escaped:
                name_chars.append(ch)
                escaped = False
                continue
            if ch == escape_char:
                escaped = True
                continue
            if ch == quote:
                return "".join(name_chars)
            name_chars.append(ch)
        return ""

    def _escape_external_table_name(self, table_name: str) -> str:
        return (
            table_name.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        )

    def _extract_table_name(self, query: str) -> str:
        """Extract the leading table name or external_table("...") argument."""
        query = query.strip()
        if not query or query.startswith("."):
            return ""
        external_name = self._parse_external_table_name(query)
        if external_name:
            return external_name
        if query.startswith("["):
            end = query.find("]")
            if end != -1:
                content = query[1:end].strip()
                if (
                    len(content) >= 2
                    and content[0] in {"'", '"'}
                    and content[0] == content[-1]
                ):
                    return content[1:-1]
                return content
        return query.split("|")[0].strip()

    def _get_table_names(
        self,
        cluster: str,
        database: str,
        list_method,
    ) -> list[str]:
        try:
            return json.loads(list_method(cluster, database))
        except Exception as err:
            logger.debug(f"Could not list tables for hint: {err}")
            return []

    def _table_kind_hint(
        self,
        cluster: str,
        database: str,
        reference: str,
        expected_kind: str,
        *,
        is_table_name: bool = False,
    ) -> str:
        table_name = reference if is_table_name else self._extract_table_name(reference)
        if not table_name:
            return ""
        if expected_kind == "internal":
            external_tables = self._get_table_names(
                cluster, database, self.list_external_tables
            )
            if table_name in external_tables:
                return (
                    f"Hint: '{table_name}' is an external table. "
                    "Use table_kind='external'."
                )
        if expected_kind == "external":
            internal_tables = self._get_table_names(
                cluster, database, self.list_internal_tables
            )
            materialized_views = self._get_table_names(
                cluster, database, self.list_materialized_views
            )
            if table_name in internal_tables or table_name in materialized_views:
                return (
                    f"Hint: '{table_name}' is an internal table or materialized view. "
                    "Use table_kind='internal'."
                )
        return ""

    def list_tables(
        self, cluster: str, database: str, table_kind: str | None = None
    ) -> str:
        resolved_kind = _normalize_table_kind(
            table_kind, default="all", allowed=_LIST_TABLE_KINDS
        )
        if resolved_kind == "all":
            internal_tables = json.loads(self.list_internal_tables(cluster, database))
            external_tables = json.loads(self.list_external_tables(cluster, database))
            materialized_views = json.loads(
                self.list_materialized_views(cluster, database)
            )
            return json.dumps(
                {
                    "internal_tables": internal_tables,
                    "external_tables": external_tables,
                    "materialized_views": materialized_views,
                }
            )
        if resolved_kind == "internal":
            return self.list_internal_tables(cluster, database)
        if resolved_kind == "external":
            return self.list_external_tables(cluster, database)
        return self.list_materialized_views(cluster, database)

    def list_internal_tables(self, cluster: str, database: str) -> str:
        client = self._get_client(cluster)
        response = client.execute(database, ".show tables")
        tables = [row[0] for row in response.primary_results[0]]
        return json.dumps(tables)

    def list_external_tables(self, cluster: str, database: str) -> str:
        client = self._get_client(cluster)
        response = client.execute(database, ".show external tables")
        tables = [row[0] for row in response.primary_results[0]]
        return json.dumps(tables)

    def list_materialized_views(self, cluster: str, database: str) -> str:
        client = self._get_client(cluster)
        response = client.execute(database, ".show materialized-views")
        tables = [row[0] for row in response.primary_results[0]]
        return json.dumps(tables)

    def execute_query_internal_table(
        self, cluster: str, database: str, query: str
    ) -> str:
        logger.debug(f"Executing query: {query}")
        if query.startswith("."):
            raise ValueError("Should not use management commands")
        try:
            client = self._get_client(cluster)
            properties = _make_request_properties()
            response = client.execute(database, query, properties)
            return _format_results(response.primary_results[0])
        except KustoServiceError as e:
            logger.error(f"Query error: {e}")
            hint = self._try_get_schema_hint(cluster, database, query)
            table_hint = ""
            if not hint:
                table_hint = self._table_kind_hint(
                    cluster, database, query, expected_kind="internal"
                )
            msg = f"Query failed: {e}"
            if hint:
                msg += f"\n\n{hint}\n\nPlease fix the query and retry."
            if table_hint:
                msg += f"\n\n{table_hint}"
            return msg

    def execute_query_external_table(
        self, cluster: str, database: str, query: str
    ) -> str:
        logger.debug(f"Executing query: {query}")
        if query.startswith("."):
            raise ValueError("Should not use management commands")
        try:
            client = self._get_client(cluster)
            table_name = self._extract_table_name(query)
            stripped_query = query.lstrip()
            prefix_segment = stripped_query.split("|")[0].strip()
            if (
                table_name
                and prefix_segment
                and not stripped_query.lower().startswith("external_table(")
                and (
                    " " not in prefix_segment
                    or (prefix_segment.startswith("[") and prefix_segment.endswith("]"))
                )
            ):
                leading_whitespace = query[: len(query) - len(stripped_query)]
                escaped_table_name = self._escape_external_table_name(table_name)
                rewritten = re.sub(
                    rf"^{re.escape(prefix_segment)}",
                    f'external_table("{escaped_table_name}")',
                    stripped_query,
                    count=1,
                )
                query = f"{leading_whitespace}{rewritten}"
            properties = _make_request_properties()
            response = client.execute(database, query, properties)
            return _format_results(response.primary_results[0])
        except KustoServiceError as e:
            logger.error(f"Query error: {e}")
            table_hint = self._table_kind_hint(
                cluster, database, query, expected_kind="external"
            )
            msg = f"Query failed: {e}"
            if table_hint:
                msg += f"\n\n{table_hint}"
            return msg

    def retrieve_internal_table_schema(
        self, cluster: str, database: str, table: str
    ) -> str:
        client = self._get_client(cluster)
        response = client.execute(database, f"{table} | getschema")
        return _format_results(response.primary_results[0])

    def retrieve_external_table_schema(
        self, cluster: str, database: str, table: str
    ) -> str:
        client = self._get_client(cluster)
        response = client.execute(
            database, f'external_table("{table}") | getschema'
        )
        return _format_results(response.primary_results[0])

    def execute_query(
        self,
        cluster: str,
        database: str,
        query: str,
        table_kind: str | None = None,
    ) -> str:
        """Execute a query; materialized views follow the internal-table path."""
        resolved_kind = _normalize_table_kind(
            table_kind, default="internal", allowed=_QUERY_TABLE_KINDS
        )
        if resolved_kind == "external":
            return self.execute_query_external_table(cluster, database, query)
        if resolved_kind in {"internal", "materialized_view"}:
            return self.execute_query_internal_table(cluster, database, query)
        raise ValueError(
            f"table_kind '{resolved_kind}' is not valid here. "
            f"Allowed values: {', '.join(sorted(_QUERY_TABLE_KINDS))}."
        )

    def retrieve_table_schema(
        self,
        cluster: str,
        database: str,
        table: str,
        table_kind: str | None = None,
    ) -> str:
        resolved_kind = _normalize_table_kind(
            table_kind, default="internal", allowed=_QUERY_TABLE_KINDS
        )
        try:
            if resolved_kind == "external":
                return self.retrieve_external_table_schema(cluster, database, table)
            if resolved_kind in {"internal", "materialized_view"}:
                return self.retrieve_internal_table_schema(cluster, database, table)
            raise ValueError(
                f"table_kind '{resolved_kind}' is not valid here. "
                f"Allowed values: {', '.join(sorted(_QUERY_TABLE_KINDS))}."
            )
        except KustoServiceError as e:
            table_hint = self._table_kind_hint(
                cluster,
                database,
                table,
                expected_kind=resolved_kind,
                is_table_name=True,
            )
            msg = f"Schema lookup failed: {e}"
            if table_hint:
                msg += f"\n\n{table_hint}"
            return msg


async def main(tenant_id: str = None):
    server = Server("kusto-manager")
    credential = build_credential(tenant_id=tenant_id)
    kusto_database = KustoDatabase(credential)

    cluster_prop = {
        "type": "string",
        "description": "Azure Data Explorer cluster URL (e.g. https://mycluster.eastus.kusto.windows.net)",
    }
    database_prop = {"type": "string", "description": "Database name"}
    list_table_kind_prop = {
        "type": "string",
        "enum": ["internal", "external", "materialized_view", "all"],
        "description": "Optional filter for table kind. Defaults to 'all'.",
    }
    query_table_kind_prop = {
        "type": "string",
        "enum": ["internal", "external", "materialized_view"],
        "description": "Optional table kind. Defaults to 'internal'. Use 'external' for external tables.",
    }

    tool_list = [
        types.Tool(
            name="list_tables",
            description=(
                "List tables in the database. Use table_kind to filter "
                "(internal, external, materialized_view, or all)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "table_kind": list_table_kind_prop,
                },
                "required": ["cluster", "database"],
            },
        ),
        types.Tool(
            name="execute_query",
            description=(
                "Execute a KQL query. Defaults to internal tables/materialized views; "
                "set table_kind='external' for external tables. Always use '| project' "
                "to select only the columns you need — tables can have many columns and "
                "returning all of them wastes context."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "query": {
                        "type": "string",
                        "description": "KQL query to execute. Use '| project col1, col2' to limit columns returned.",
                    },
                    "table_kind": query_table_kind_prop,
                },
                "required": ["cluster", "database", "query"],
            },
        ),
        types.Tool(
            name="retrieve_table_schema",
            description=(
                "Get the schema of a table or materialized view. Defaults to internal; "
                "set table_kind='external' for external tables."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "table": {"type": "string", "description": "Table name"},
                    "table_kind": query_table_kind_prop,
                },
                "required": ["cluster", "database", "table"],
            },
        ),
    ]
    legacy_tool_aliases = {
        "list_internal_tables": ("list_tables", {"table_kind": "internal"}),
        "list_external_tables": ("list_tables", {"table_kind": "external"}),
        "list_materialized_views": ("list_tables", {"table_kind": "materialized_view"}),
        "execute_query_internal_table": ("execute_query", {"table_kind": "internal"}),
        "execute_query_external_table": ("execute_query", {"table_kind": "external"}),
        "retrieve_internal_table_schema": (
            "retrieve_table_schema",
            {"table_kind": "internal"},
        ),
        "retrieve_external_table_schema": (
            "retrieve_table_schema",
            {"table_kind": "external"},
        ),
    }
    tool_name_list = [tool.name for tool in tool_list] + list(
        legacy_tool_aliases.keys()
    )

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        return tool_list

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        alias = legacy_tool_aliases.get(name)
        if alias:
            name, alias_arguments = alias
            if arguments:
                arguments = {**alias_arguments, **arguments}
            else:
                arguments = dict(alias_arguments)
        if name not in tool_name_list:
            raise ValueError(f"Unknown tool: {name}")

        if not arguments or "cluster" not in arguments or "database" not in arguments:
            raise ValueError("Missing cluster or database argument")
        cluster = arguments["cluster"]
        database = arguments["database"]

        try:
            if name == "list_tables":
                results = kusto_database.list_tables(
                    cluster, database, arguments.get("table_kind")
                )
                return [types.TextContent(type="text", text=results)]
            elif name == "execute_query":
                if "query" not in arguments:
                    raise ValueError("Missing query argument")
                results = kusto_database.execute_query(
                    cluster,
                    database,
                    arguments["query"],
                    arguments.get("table_kind"),
                )
                return [types.TextContent(type="text", text=results)]
            elif name == "retrieve_table_schema":
                if "table" not in arguments:
                    raise ValueError("Missing table argument")
                results = kusto_database.retrieve_table_schema(
                    cluster,
                    database,
                    arguments["table"],
                    arguments.get("table_kind"),
                )
                return [types.TextContent(type="text", text=results)]
        except Exception as e:
            # Check if this is an auth error and surface device code to LLM
            device_info = get_pending_device_code()
            if device_info:
                return [
                    types.TextContent(
                        type="text",
                        text=(
                            f"Authentication required for cluster {cluster}. "
                            f"Please ask the user to open {device_info['verification_uri']} "
                            f"in a browser and enter code: {device_info['user_code']}. "
                            f"Then retry this request."
                        ),
                    )
                ]
            logger.error(f"Error in tool {name}: {e}")
            return [types.TextContent(type="text", text=f"Error: {e}")]

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="kusto",
                server_version="0.2.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
