"""Tests for mcp_server_kusto.auth module."""
import sys
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from mcp_server_kusto.auth import (
    build_credential,
    build_kcsb,
    get_pending_device_code,
    _stderr_device_code_prompt,
    _get_kusto_scopes,
    CACHE_NAME,
    KUSTO_CLIENT_APP_ID,
)


class TestConstants:
    def test_cache_name(self):
        assert CACHE_NAME == "kusto-mcp"

    def test_kusto_client_app_id(self):
        assert KUSTO_CLIENT_APP_ID == "db662dc1-0cfe-4e1c-a843-19a68e65be58"


class TestBuildCredential:
    def test_returns_chained_credential(self):
        cred = build_credential()
        assert type(cred).__name__ == "ChainedTokenCredential"

    def test_accepts_tenant_id(self):
        cred = build_credential(tenant_id="test-tenant")
        assert type(cred).__name__ == "ChainedTokenCredential"

    def test_accepts_none_tenant_id(self):
        cred = build_credential(tenant_id=None)
        assert cred is not None


class TestBuildKcsb:
    def test_http_cluster_uses_no_auth(self):
        cred = MagicMock()
        kcsb = build_kcsb("http://localhost:8082", cred)
        # No auth — credential should never be called
        cred.get_token.assert_not_called()
        assert kcsb is not None

    def test_https_cluster_uses_token_provider(self):
        mock_token = MagicMock()
        mock_token.token = "fake-token"
        cred = MagicMock()
        cred.get_token.return_value = mock_token

        kcsb = build_kcsb("https://mycluster.eastus.kusto.windows.net", cred)
        assert kcsb is not None
        # The token provider should be set (will be called by KustoClient later)
        assert kcsb._token_provider is not None

    def test_https_token_provider_calls_credential(self):
        mock_token = MagicMock()
        mock_token.token = "test-token-123"
        cred = MagicMock()
        cred.get_token.return_value = mock_token

        kcsb = build_kcsb("https://mycluster.eastus.kusto.windows.net", cred)
        result = kcsb._token_provider()
        assert result == "test-token-123"
        cred.get_token.assert_called_once()


class TestDeviceCodePrompt:
    def test_stores_device_code_info(self):
        # Clear any pending state
        get_pending_device_code()

        expires = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        _stderr_device_code_prompt(
            "https://microsoft.com/devicelogin", "TESTCODE", expires
        )

        info = get_pending_device_code()
        assert info is not None
        assert info["verification_uri"] == "https://microsoft.com/devicelogin"
        assert info["user_code"] == "TESTCODE"
        assert info["expires_on"] == expires

    def test_get_pending_clears_after_read(self):
        expires = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        _stderr_device_code_prompt("https://example.com", "CODE1", expires)

        first = get_pending_device_code()
        assert first is not None
        second = get_pending_device_code()
        assert second is None

    def test_prints_to_stderr(self, capsys):
        expires = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        _stderr_device_code_prompt(
            "https://microsoft.com/devicelogin", "ABC123", expires
        )
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "ABC123" in captured.err
        assert "https://microsoft.com/devicelogin" in captured.err
        # Clean up
        get_pending_device_code()

    def test_returns_none_when_no_pending(self):
        # Ensure cleared
        get_pending_device_code()
        assert get_pending_device_code() is None


class TestGetKustoScopes:
    @patch("mcp_server_kusto.auth.CloudSettings")
    def test_returns_scope_from_cloud_settings(self, mock_cloud):
        mock_info = MagicMock()
        mock_info.kusto_service_resource_id = "https://kusto.kusto.windows.net"
        mock_cloud.get_cloud_info_for_cluster.return_value = mock_info

        scopes = _get_kusto_scopes("https://mycluster.kusto.windows.net")
        assert scopes == ["https://kusto.kusto.windows.net/.default"]

    @patch("mcp_server_kusto.auth.CloudSettings")
    def test_falls_back_on_error(self, mock_cloud):
        mock_cloud.get_cloud_info_for_cluster.side_effect = Exception("network error")

        scopes = _get_kusto_scopes("https://mycluster.kusto.windows.net")
        assert scopes == ["https://kusto.kusto.windows.net/.default"]
