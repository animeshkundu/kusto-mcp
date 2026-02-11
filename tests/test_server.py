"""Tests for mcp_server_kusto.server module."""
import json
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

from mcp_server_kusto.server import (
    KustoDatabase,
    _format_results,
    _make_request_properties,
)


class TestMakeRequestProperties:
    def test_returns_client_request_properties(self):
        props = _make_request_properties()
        assert type(props).__name__ == "ClientRequestProperties"


class TestFormatResults:
    def test_formats_empty_table(self):
        table = MagicMock()
        table.columns = []
        table.__iter__ = MagicMock(return_value=iter([]))
        result = _format_results(table)
        parsed = json.loads(result)
        assert parsed == {"columns": [], "row_count": 0, "data": []}

    def test_formats_rows_as_dicts(self):
        col1 = MagicMock()
        col1.column_name = "Name"
        col2 = MagicMock()
        col2.column_name = "Age"

        row1 = MagicMock()
        row1.to_dict.return_value = {"Name": "Alice", "Age": 30}
        row2 = MagicMock()
        row2.to_dict.return_value = {"Name": "Bob", "Age": 25}

        table = MagicMock()
        table.columns = [col1, col2]
        table.__iter__ = MagicMock(return_value=iter([row1, row2]))

        result = _format_results(table)
        parsed = json.loads(result)
        assert parsed["columns"] == ["Name", "Age"]
        assert parsed["row_count"] == 2
        assert parsed["data"][0] == {"Name": "Alice", "Age": 30}
        assert parsed["data"][1] == {"Name": "Bob", "Age": 25}


class TestKustoDatabase:
    def test_init_creates_empty_cache(self):
        cred = MagicMock()
        db = KustoDatabase(cred)
        assert db._clients == {}
        assert db._credential is cred

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_get_client_caches_per_cluster(self, mock_build, mock_client_cls):
        mock_kcsb = MagicMock()
        mock_build.return_value = mock_kcsb
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)

        # First call creates client
        client1 = db._get_client("https://cluster1.kusto.windows.net")
        assert client1 is mock_client
        assert mock_client_cls.call_count == 1

        # Second call to same cluster reuses cached client
        client2 = db._get_client("https://cluster1.kusto.windows.net")
        assert client2 is client1
        assert mock_client_cls.call_count == 1  # not called again

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_get_client_different_clusters(self, mock_build, mock_client_cls):
        cred = MagicMock()
        db = KustoDatabase(cred)

        db._get_client("https://cluster1.kusto.windows.net")
        db._get_client("https://cluster2.kusto.windows.net")
        assert mock_client_cls.call_count == 2

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_list_internal_tables(self, mock_build, mock_client_cls):
        mock_row1 = MagicMock()
        mock_row1.__getitem__ = lambda self, i: "Table1"
        mock_row2 = MagicMock()
        mock_row2.__getitem__ = lambda self, i: "Table2"

        mock_response = MagicMock()
        mock_response.primary_results = [[mock_row1, mock_row2]]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.list_internal_tables("https://cluster.kusto.windows.net", "mydb")

        parsed = json.loads(result)
        assert parsed == ["Table1", "Table2"]
        mock_client.execute.assert_called_once_with("mydb", ".show tables")

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_list_external_tables(self, mock_build, mock_client_cls):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: "ExtTable"

        mock_response = MagicMock()
        mock_response.primary_results = [[mock_row]]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.list_external_tables("https://cluster.kusto.windows.net", "mydb")

        parsed = json.loads(result)
        assert parsed == ["ExtTable"]
        mock_client.execute.assert_called_once_with("mydb", ".show external tables")

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_list_materialized_views(self, mock_build, mock_client_cls):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: "View1"

        mock_response = MagicMock()
        mock_response.primary_results = [[mock_row]]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.list_materialized_views("https://cluster.kusto.windows.net", "mydb")

        parsed = json.loads(result)
        assert parsed == ["View1"]

    def test_list_tables_all(self):
        cred = MagicMock()
        db = KustoDatabase(cred)
        with patch.object(
            db, "list_internal_tables", return_value=json.dumps(["Table1"])
        ), patch.object(
            db, "list_external_tables", return_value=json.dumps(["ExtTable"])
        ), patch.object(
            db, "list_materialized_views", return_value=json.dumps(["View1"])
        ):
            result = db.list_tables("https://cluster.kusto.windows.net", "mydb")

        parsed = json.loads(result)
        assert parsed == {
            "internal_tables": ["Table1"],
            "external_tables": ["ExtTable"],
            "materialized_views": ["View1"],
        }

    def test_list_tables_all_empty(self):
        cred = MagicMock()
        db = KustoDatabase(cred)
        with patch.object(
            db, "list_internal_tables", return_value=json.dumps([])
        ), patch.object(
            db, "list_external_tables", return_value=json.dumps([])
        ), patch.object(
            db, "list_materialized_views", return_value=json.dumps([])
        ):
            result = db.list_tables("https://cluster.kusto.windows.net", "mydb")

        parsed = json.loads(result)
        assert parsed == {
            "internal_tables": [],
            "external_tables": [],
            "materialized_views": [],
        }

    def test_execute_query_rejects_management_commands(self):
        cred = MagicMock()
        db = KustoDatabase(cred)
        with pytest.raises(ValueError, match="management commands"):
            db.execute_query_internal_table(
                "https://cluster.kusto.windows.net", "mydb", ".show tables"
            )

    def test_execute_external_query_rejects_management_commands(self):
        cred = MagicMock()
        db = KustoDatabase(cred)
        with pytest.raises(ValueError, match="management commands"):
            db.execute_query_external_table(
                "https://cluster.kusto.windows.net", "mydb", ".drop table Foo"
            )

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_execute_query_internal_table(self, mock_build, mock_client_cls):
        col = MagicMock()
        col.column_name = "Count"
        row = MagicMock()
        row.to_dict.return_value = {"Count": 42}

        result_table = MagicMock()
        result_table.columns = [col]
        result_table.__iter__ = MagicMock(return_value=iter([row]))

        mock_response = MagicMock()
        mock_response.primary_results = [result_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.execute_query_internal_table(
            "https://cluster.kusto.windows.net", "mydb", "MyTable | count"
        )

        parsed = json.loads(result)
        assert parsed["columns"] == ["Count"]
        assert parsed["row_count"] == 1
        assert parsed["data"] == [{"Count": 42}]

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_execute_query_internal_table_error_with_schema_hint(
        self, mock_build, mock_client_cls
    ):
        from azure.kusto.data.exceptions import KustoServiceError

        # First call: query fails
        # Second call: schema fetch succeeds
        schema_col_name = MagicMock()
        schema_col_name.column_name = "ColumnName"
        schema_col_type = MagicMock()
        schema_col_type.column_name = "ColumnType"
        schema_row = MagicMock()
        schema_row.to_dict.return_value = {
            "ColumnName": "RealColumn",
            "ColumnType": "string",
        }
        schema_table = MagicMock()
        schema_table.columns = [schema_col_name, schema_col_type]
        schema_table.__iter__ = MagicMock(return_value=iter([schema_row]))
        schema_response = MagicMock()
        schema_response.primary_results = [schema_table]

        mock_client = MagicMock()
        mock_client.execute.side_effect = [
            KustoServiceError("BadColumn not found", mock_client),
            schema_response,
        ]
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.execute_query_internal_table(
            "https://cluster.kusto.windows.net",
            "mydb",
            "MyTable | where BadColumn > 5",
        )

        assert "Query failed" in result
        assert "Schema for 'MyTable'" in result
        assert "RealColumn" in result

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_execute_query_internal_table_hint_for_external_table(
        self, mock_build, mock_client_cls
    ):
        from azure.kusto.data.exceptions import KustoServiceError

        mock_client = MagicMock()
        mock_client.execute.side_effect = KustoServiceError(
            "Table not found", mock_client
        )
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)

        with patch.object(db, "_try_get_schema_hint", return_value=""), patch.object(
            db,
            "list_external_tables",
            return_value=json.dumps(["ExtTable"]),
        ):
            result = db.execute_query_internal_table(
                "https://cluster.kusto.windows.net", "mydb", "ExtTable | count"
            )

        assert "table_kind='external'" in result

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_execute_query_external_table_wraps_name(
        self, mock_build, mock_client_cls
    ):
        col = MagicMock()
        col.column_name = "Val"
        row = MagicMock()
        row.to_dict.return_value = {"Val": 1}

        result_table = MagicMock()
        result_table.columns = [col]
        result_table.__iter__ = MagicMock(return_value=iter([row]))

        mock_response = MagicMock()
        mock_response.primary_results = [result_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        db.execute_query_external_table(
            "https://cluster.kusto.windows.net",
            "mydb",
            "ExtTable | take 10",
        )

        call_args = mock_client.execute.call_args
        query_sent = call_args[0][1]
        assert 'external_table("ExtTable")' in query_sent

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_execute_query_external_table_preserves_existing_call(
        self, mock_build, mock_client_cls
    ):
        result_table = MagicMock()
        result_table.columns = []
        result_table.__iter__ = MagicMock(return_value=iter([]))

        mock_response = MagicMock()
        mock_response.primary_results = [result_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        db.execute_query_external_table(
            "https://cluster.kusto.windows.net",
            "mydb",
            'external_table("ExtTable") | take 1',
        )

        call_args = mock_client.execute.call_args
        query_sent = call_args[0][1]
        assert query_sent.startswith('external_table("ExtTable")')

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_execute_query_external_table_bracketed_name(
        self, mock_build, mock_client_cls
    ):
        result_table = MagicMock()
        result_table.columns = []
        result_table.__iter__ = MagicMock(return_value=iter([]))

        mock_response = MagicMock()
        mock_response.primary_results = [result_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        db.execute_query_external_table(
            "https://cluster.kusto.windows.net",
            "mydb",
            '["Ext Table"] | take 1',
        )

        call_args = mock_client.execute.call_args
        query_sent = call_args[0][1]
        assert 'external_table("Ext Table")' in query_sent

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_retrieve_internal_table_schema(self, mock_build, mock_client_cls):
        col = MagicMock()
        col.column_name = "ColumnName"
        row = MagicMock()
        row.to_dict.return_value = {"ColumnName": "Id", "ColumnType": "long"}

        result_table = MagicMock()
        result_table.columns = [col]
        result_table.__iter__ = MagicMock(return_value=iter([row]))

        mock_response = MagicMock()
        mock_response.primary_results = [result_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.retrieve_internal_table_schema(
            "https://cluster.kusto.windows.net", "mydb", "MyTable"
        )

        mock_client.execute.assert_called_once_with("mydb", "MyTable | getschema")
        parsed = json.loads(result)
        assert parsed["data"][0]["ColumnName"] == "Id"

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_retrieve_external_table_schema(self, mock_build, mock_client_cls):
        col = MagicMock()
        col.column_name = "ColumnName"
        row = MagicMock()
        row.to_dict.return_value = {"ColumnName": "Id", "ColumnType": "string"}

        result_table = MagicMock()
        result_table.columns = [col]
        result_table.__iter__ = MagicMock(return_value=iter([row]))

        mock_response = MagicMock()
        mock_response.primary_results = [result_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = mock_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        result = db.retrieve_external_table_schema(
            "https://cluster.kusto.windows.net", "mydb", "ExtTable"
        )

        mock_client.execute.assert_called_once_with(
            "mydb", 'external_table("ExtTable") | getschema'
        )
        parsed = json.loads(result)
        assert parsed["data"][0]["ColumnName"] == "Id"


class TestSchemaHint:
    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_schema_hint_for_simple_query(self, mock_build, mock_client_cls):
        schema_row = MagicMock()
        schema_row.to_dict.return_value = {
            "ColumnName": "EventTime",
            "ColumnType": "datetime",
        }
        schema_table = MagicMock()
        schema_table.__iter__ = MagicMock(return_value=iter([schema_row]))
        schema_response = MagicMock()
        schema_response.primary_results = [schema_table]

        mock_client = MagicMock()
        mock_client.execute.return_value = schema_response
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        hint = db._try_get_schema_hint(
            "https://cluster.kusto.windows.net",
            "mydb",
            "Events | where foo > 1",
        )

        assert "Schema for 'Events'" in hint
        assert "EventTime" in hint
        assert "datetime" in hint

    @patch("mcp_server_kusto.server.KustoClient")
    @patch("mcp_server_kusto.server.build_kcsb")
    def test_schema_hint_returns_empty_on_failure(self, mock_build, mock_client_cls):
        mock_client = MagicMock()
        mock_client.execute.side_effect = Exception("not found")
        mock_client_cls.return_value = mock_client

        cred = MagicMock()
        db = KustoDatabase(cred)
        hint = db._try_get_schema_hint(
            "https://cluster.kusto.windows.net", "mydb", "BadTable | count"
        )
        assert hint == ""

    def test_schema_hint_skips_management_commands(self):
        cred = MagicMock()
        db = KustoDatabase(cred)
        hint = db._try_get_schema_hint(
            "https://cluster.kusto.windows.net", "mydb", ".show tables"
        )
        assert hint == ""
