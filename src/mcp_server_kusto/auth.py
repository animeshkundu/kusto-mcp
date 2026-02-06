import sys
import logging
import threading
from datetime import datetime
from typing import Optional

from azure.identity import (
    AzureCliCredential,
    DeviceCodeCredential,
    ChainedTokenCredential,
    TokenCachePersistenceOptions,
)
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data._cloud_settings import CloudSettings

logger = logging.getLogger("mcp_kusto_server")

CACHE_NAME = "kusto-mcp"

# Well-known Kusto client app ID used by the Kusto SDK for user-delegated auth
KUSTO_CLIENT_APP_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

# Stores the most recent device code prompt info so tool calls can surface it to the LLM
_pending_device_code: Optional[dict] = None
_device_code_lock = threading.Lock()


def _stderr_device_code_prompt(
    verification_uri: str,
    user_code: str,
    expires_on: datetime,
) -> None:
    """Callback for DeviceCodeCredential that stores the prompt info and prints to stderr."""
    global _pending_device_code
    with _device_code_lock:
        _pending_device_code = {
            "verification_uri": verification_uri,
            "user_code": user_code,
            "expires_on": expires_on,
        }
    # Also print to stderr as backup (stdout is MCP JSON-RPC)
    message = (
        f"\nTo authenticate with Azure Data Explorer, open a browser to:\n"
        f"  {verification_uri}\n"
        f"and enter the code: {user_code}\n"
    )
    print(message, file=sys.stderr, flush=True)


def get_pending_device_code() -> Optional[dict]:
    """Return and clear the most recent device code prompt info."""
    global _pending_device_code
    with _device_code_lock:
        info = _pending_device_code
        _pending_device_code = None
        return info


def _get_kusto_scopes(cluster: str) -> list[str]:
    """Resolve the OAuth scope for the given Kusto cluster."""
    try:
        cloud_info = CloudSettings.get_cloud_info_for_cluster(cluster)
        resource_id = cloud_info.kusto_service_resource_id
    except Exception:
        resource_id = "https://kusto.kusto.windows.net"
    return [resource_id + "/.default"]


def build_credential(tenant_id: Optional[str] = None) -> ChainedTokenCredential:
    """Build a ChainedTokenCredential for headless-friendly auth.

    Order:
      1. AzureCliCredential — silent if ``az login`` was run
      2. DeviceCodeCredential — prints code to stderr for headless use

    Both use persistent token caching so subsequent server restarts
    do not require re-authentication.
    """
    cache_options = TokenCachePersistenceOptions(
        name=CACHE_NAME,
        allow_unencrypted_storage=True,
    )

    cli_kwargs = {}
    if tenant_id:
        cli_kwargs["tenant_id"] = tenant_id
    cli_credential = AzureCliCredential(**cli_kwargs)

    device_kwargs = {
        "client_id": KUSTO_CLIENT_APP_ID,
        "prompt_callback": _stderr_device_code_prompt,
        "cache_persistence_options": cache_options,
    }
    if tenant_id:
        device_kwargs["tenant_id"] = tenant_id
    device_credential = DeviceCodeCredential(**device_kwargs)

    return ChainedTokenCredential(cli_credential, device_credential)


def build_kcsb(
    cluster: str,
    credential: ChainedTokenCredential,
) -> KustoConnectionStringBuilder:
    """Build a KustoConnectionStringBuilder for the given cluster.

    - ``http://`` clusters use no authentication (local emulator).
    - ``https://`` clusters use the shared credential via token provider.
    """
    if cluster.startswith("http://"):
        logger.info("Using no-auth for local emulator cluster: %s", cluster)
        return KustoConnectionStringBuilder.with_no_authentication(cluster)

    scopes = _get_kusto_scopes(cluster)

    def token_provider() -> str:
        token = credential.get_token(*scopes)
        return token.token

    logger.info("Using default auth (AzureCLI -> DeviceCode) for cluster: %s", cluster)
    return KustoConnectionStringBuilder.with_token_provider(cluster, token_provider)
