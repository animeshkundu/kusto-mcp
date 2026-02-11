"""Spec-oriented KQL coverage for query rewrite behavior."""
from unittest.mock import MagicMock, patch

import pytest

from mcp_server_kusto.server import KustoDatabase


@pytest.mark.parametrize(
    ("query", "expected_query"),
    [
        ("ExtTable | take 5", 'external_table("ExtTable") | take 5'),
        ("Ext_Table | take 1", 'external_table("Ext_Table") | take 1'),
        ("Table.Name | take 1", 'external_table("Table.Name") | take 1'),
        ('["Ext Table"] | take 1', 'external_table("Ext Table") | take 1'),
        ("['Ext-Table'] | take 1", 'external_table("Ext-Table") | take 1'),
        ("['1day'] | take 1", 'external_table("1day") | take 1'),
        ('["where"] | take 1', 'external_table("where") | take 1'),
        ('external_table("ExtTable") | take 1', 'external_table("ExtTable") | take 1'),
        (
            "ExtTable | join ExtTable2 on Key",
            'external_table("ExtTable") | join external_table("ExtTable2") on Key',
        ),
        (
            "ExtTable | join kind=inner (ExtTable2) on Key",
            'external_table("ExtTable") | join kind=inner (external_table("ExtTable2")) on Key',
        ),
        (
            "ExtTable | union ExtTable2",
            'external_table("ExtTable") | union external_table("ExtTable2")',
        ),
        (
            "union ['Ext Table'], ExtTable2 | take 1",
            'union external_table("Ext Table"), external_table("ExtTable2") | take 1',
        ),
        (
            "ExtTable | join ['Ext-Table2'] on Key",
            'external_table("ExtTable") | join external_table("Ext-Table2") on Key',
        ),
        ("database('db').Table | take 1", "database('db').Table | take 1"),
        # Let statements are not rewritten; the tool only wraps leading table refs.
        ("let T = ExtTable; T | take 1", "let T = ExtTable; T | take 1"),
    ],
)
@patch("mcp_server_kusto.server.KustoClient")
@patch("mcp_server_kusto.server.build_kcsb")
def test_external_table_rewrite_cases(
    mock_build, mock_client_cls, query, expected_query
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
        "https://cluster.kusto.windows.net", "mydb", query
    )

    call_args = mock_client.execute.call_args
    query_sent = call_args[0][1]
    assert query_sent == expected_query


@pytest.mark.parametrize(
    "query",
    [
        "cluster('c').database('db').Table | take 1",
        "table('ExtTable') | take 1",
        "materialized_view('View1') | take 1",
        "// comment\nExtTable | take 1",
        "/* comment */ ExtTable | take 1",
        "['ExtTable] | take 1",
        "[ExtTable | take 1",
        "123Table | take 1",
        "(ExtTable) | take 1",
        "EXTERNAL_TABLE('ExtTable') | take 1",
    ],
)
@patch("mcp_server_kusto.server.KustoClient")
@patch("mcp_server_kusto.server.build_kcsb")
def test_external_table_rewrite_skips_invalid_inputs(
    mock_build, mock_client_cls, query
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
        "https://cluster.kusto.windows.net", "mydb", query
    )

    call_args = mock_client.execute.call_args
    query_sent = call_args[0][1]
    assert query_sent == query


@patch("mcp_server_kusto.server.logger.warning")
@patch("mcp_server_kusto.server.KustoClient")
@patch("mcp_server_kusto.server.build_kcsb")
def test_external_table_rewrite_warns_on_unbalanced(
    mock_build, mock_client_cls, mock_warning
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
    query = "ExtTable | join (ExtTable2 on Key"
    db.execute_query_external_table(
        "https://cluster.kusto.windows.net", "mydb", query
    )

    call_args = mock_client.execute.call_args
    query_sent = call_args[0][1]
    assert query_sent == query
    mock_warning.assert_called_once_with(
        "Skipping external table rewrite due to unbalanced brackets or parentheses."
    )
