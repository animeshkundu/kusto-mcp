from . import server
import asyncio
import argparse


def main():
    parser = argparse.ArgumentParser(description="Kusto MCP Server")
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        help="Azure tenant ID (optional, for single-tenant scenarios)",
        required=False,
    )
    args = parser.parse_args()

    asyncio.run(server.main(tenant_id=args.tenant_id))


__all__ = ["main", "server"]
