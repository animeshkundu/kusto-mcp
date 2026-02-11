"""Spec-oriented KQL coverage for query rewrite behavior."""
from unittest.mock import MagicMock, patch

import pytest

from mcp_server_kusto.server import KustoDatabase


@pytest.mark.parametrize(
    ("query", "expected_query"),
    [
        ("ExtTable | take 5", 'external_table("ExtTable") | take 5'),
        ('["Ext Table"] | take 1', 'external_table("Ext Table") | take 1'),
        ("['Ext-Table'] | take 1", 'external_table("Ext-Table") | take 1'),
        ('external_table("ExtTable") | take 1', 'external_table("ExtTable") | take 1'),
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
