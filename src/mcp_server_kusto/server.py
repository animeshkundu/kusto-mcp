import json
import logging
import os
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
            msg = f"Query failed: {e}"
            if hint:
                msg += f"\n\n{hint}\n\nPlease fix the query and retry."
            return msg

    def execute_query_external_table(
        self, cluster: str, database: str, query: str
    ) -> str:
        logger.debug(f"Executing query: {query}")
        if query.startswith("."):
            raise ValueError("Should not use management commands")
        try:
            client = self._get_client(cluster)
            table_name = query.split("|")[0].strip()
            query = query.replace(table_name, f'external_table("{table_name}")')
            properties = _make_request_properties()
            response = client.execute(database, query, properties)
            return _format_results(response.primary_results[0])
        except KustoServiceError as e:
            logger.error(f"Query error: {e}")
            return f"Query failed: {e}"

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


async def main(tenant_id: str = None):
    server = Server("kusto-manager")
    credential = build_credential(tenant_id=tenant_id)
    kusto_database = KustoDatabase(credential)

    cluster_prop = {
        "type": "string",
        "description": "Azure Data Explorer cluster URL (e.g. https://mycluster.eastus.kusto.windows.net)",
    }
    database_prop = {"type": "string", "description": "Database name"}

    tool_list = [
        types.Tool(
            name="list_internal_tables",
            description="List all internal tables in the database",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                },
                "required": ["cluster", "database"],
            },
        ),
        types.Tool(
            name="list_external_tables",
            description="List all external tables in the database",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                },
                "required": ["cluster", "database"],
            },
        ),
        types.Tool(
            name="list_materialized_views",
            description="List all materialized views in the database",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                },
                "required": ["cluster", "database"],
            },
        ),
        types.Tool(
            name="execute_query_internal_table",
            description="Execute a KQL query on an internal table or materialized view",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "query": {"type": "string", "description": "KQL query to execute"},
                },
                "required": ["cluster", "database", "query"],
            },
        ),
        types.Tool(
            name="execute_query_external_table",
            description="Execute a KQL query on an external table",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "query": {"type": "string", "description": "KQL query to execute"},
                },
                "required": ["cluster", "database", "query"],
            },
        ),
        types.Tool(
            name="retrieve_internal_table_schema",
            description="Get the schema of an internal table or materialized view",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "table": {"type": "string", "description": "Table name"},
                },
                "required": ["cluster", "database", "table"],
            },
        ),
        types.Tool(
            name="retrieve_external_table_schema",
            description="Get the schema of an external table",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster": cluster_prop,
                    "database": database_prop,
                    "table": {"type": "string", "description": "Table name"},
                },
                "required": ["cluster", "database", "table"],
            },
        ),
    ]
    tool_name_list = [tool.name for tool in tool_list]

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        return tool_list

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        if name not in tool_name_list:
            raise ValueError(f"Unknown tool: {name}")

        if not arguments or "cluster" not in arguments or "database" not in arguments:
            raise ValueError("Missing cluster or database argument")
        cluster = arguments["cluster"]
        database = arguments["database"]

        try:
            if name == "list_internal_tables":
                results = kusto_database.list_internal_tables(cluster, database)
                return [types.TextContent(type="text", text=results)]
            elif name == "list_external_tables":
                results = kusto_database.list_external_tables(cluster, database)
                return [types.TextContent(type="text", text=results)]
            elif name == "list_materialized_views":
                results = kusto_database.list_materialized_views(cluster, database)
                return [types.TextContent(type="text", text=results)]
            elif name == "execute_query_internal_table":
                if "query" not in arguments:
                    raise ValueError("Missing query argument")
                results = kusto_database.execute_query_internal_table(
                    cluster, database, arguments["query"]
                )
                return [types.TextContent(type="text", text=results)]
            elif name == "execute_query_external_table":
                if "query" not in arguments:
                    raise ValueError("Missing query argument")
                results = kusto_database.execute_query_external_table(
                    cluster, database, arguments["query"]
                )
                return [types.TextContent(type="text", text=results)]
            elif name == "retrieve_internal_table_schema":
                if "table" not in arguments:
                    raise ValueError("Missing table argument")
                results = kusto_database.retrieve_internal_table_schema(
                    cluster, database, arguments["table"]
                )
                return [types.TextContent(type="text", text=results)]
            elif name == "retrieve_external_table_schema":
                if "table" not in arguments:
                    raise ValueError("Missing table argument")
                results = kusto_database.retrieve_external_table_schema(
                    cluster, database, arguments["table"]
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
