from __future__ import annotations

import argparse
import json

from .app import create_app
from .database import Database
from .service import AssignmentService
from .settings import Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bitrix Taxi Router CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Initialize sqlite schema")
    subparsers.add_parser("reassign-once", help="Run timed reassignment worker once")

    serve = subparsers.add_parser("serve", help="Run HTTP API")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = Settings.load()
    settings.ensure_runtime_dirs()
    database = Database(settings.db_path)

    if args.command == "init-db":
        database.init_schema()
        print(f"Initialized database at {settings.db_path}")
        return

    if args.command == "reassign-once":
        database.init_schema()
        service = AssignmentService(database, settings)
        print(json.dumps(service.process_due_reassignments(), ensure_ascii=False, indent=2))
        return

    if args.command == "serve":
        import uvicorn

        app = create_app(settings)
        uvicorn.run(
            app,
            host=args.host or settings.app_host,
            port=args.port or settings.app_port,
        )
        return

    parser.error(f"Unsupported command: {args.command}")
