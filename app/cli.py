"""Admin CLI.

    python -m app.cli create-api-key --name dashboard
    python -m app.cli list-api-keys
    python -m app.cli revoke-api-key --prefix fg_AbCdEfGh
"""

from __future__ import annotations

import argparse
import sys

from app.db.session import get_sessionmaker
from app.services import auth


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.cli", description="FlowGrid admin commands")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-api-key", help="Create an API key (shown once)")
    create.add_argument("--name", required=True, help="Who/what the key is for")
    sub.add_parser("list-api-keys", help="List API keys (prefixes only)")
    revoke = sub.add_parser("revoke-api-key", help="Revoke a key by its prefix")
    revoke.add_argument("--prefix", required=True)

    args = parser.parse_args(argv)

    with get_sessionmaker()() as db:
        if args.command == "create-api-key":
            api_key, raw = auth.create_api_key(db, args.name)
            print(f"Created API key '{api_key.name}' ({api_key.prefix}…). Store it now; it will not be shown again:")
            print(raw)
        elif args.command == "list-api-keys":
            for k in auth.list_api_keys(db):
                state = f"revoked {k.revoked_at:%Y-%m-%d}" if k.revoked_at else "active"
                print(f"{k.prefix}…  {k.name:<24} created {k.created_at:%Y-%m-%d %H:%M}  {state}")
        elif args.command == "revoke-api-key":
            count = auth.revoke_api_key(db, args.prefix)
            print(f"Revoked {count} key(s)")
            return 0 if count else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
