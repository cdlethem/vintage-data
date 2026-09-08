"""Administrative bot-dashboard CLI argument parsing and dispatch only."""
from __future__ import annotations

import argparse

from .legacy_import import import_legacy, inspect_run, purge


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bot-dashboard")
    commands = parser.add_subparsers(dest="command", required=True)

    inspect_parser = commands.add_parser("inspect-run")
    inspect_parser.add_argument("--dag-id", required=True)
    inspect_parser.add_argument("--run-id", required=True)
    inspect_parser.add_argument("--task-id", required=True)
    inspect_parser.add_argument("--try-number", type=int)

    import_parser = commands.add_parser("import-legacy")
    import_parser.add_argument("--runs-root", required=True)
    import_parser.add_argument("--reviews-root", required=True)
    import_parser.add_argument("--check", action="store_true")

    purge_parser = commands.add_parser("purge")
    purge_parser.add_argument("--before", required=True)
    purge_parser.add_argument("--apply", action="store_true")

    reset_parser = commands.add_parser("reset-queue", help="Archive pending actions while retaining their audit history")
    reset_parser.add_argument("--actor", required=True)
    reset_parser.add_argument("--reason", required=True)
    reset_parser.add_argument("--apply", action="store_true", help="Apply the reviewed reset; default is a preview")

    args = parser.parse_args(argv)
    if args.command == "reset-queue":
        import json
        from airflow.utils.session import create_session
        from .service import reset_queue
        with create_session() as session:
            result = reset_queue(session, actor_id=args.actor, reason=args.reason, apply=args.apply)
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "inspect-run":
        return inspect_run(args.dag_id, args.run_id, args.task_id, args.try_number)
    if args.command == "import-legacy":
        return import_legacy(args.runs_root, args.reviews_root, check=args.check)
    return purge(args.before, apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
