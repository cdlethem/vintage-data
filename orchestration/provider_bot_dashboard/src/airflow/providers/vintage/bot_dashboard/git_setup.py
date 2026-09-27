"""Operator-only provisioning; credential values never enter argv or output."""
from __future__ import annotations

import json
import os
import stat
import sys

from airflow.models.connection import Connection
from airflow.utils.session import create_session
from sqlalchemy import select

from .git_provider import GitProviderError, get_provider, repository_config_from_connection


def add_parser(commands):
    parser = commands.add_parser("provision-git", help="Validate Git publication configuration, then optionally save it encrypted")
    parser.add_argument("--connection-id", default="bot_dashboard_git")
    parser.add_argument("--provider", choices=["github", "gitlab"], required=True)
    for name in ("project", "api-base-url", "clone-url", "base-branch", "service-account-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--allow-path", action="append", required=True)
    parser.add_argument("--deny-path", action="append", default=[])
    parser.add_argument("--max-changed-files", type=int, default=40)
    parser.add_argument("--max-diff-bytes", type=int, default=500000)
    token = parser.add_mutually_exclusive_group(required=True)
    token.add_argument("--token-env", help="Name of the environment variable holding the token")
    token.add_argument("--token-file", help="Private regular file containing the token; - reads stdin")
    parser.add_argument("--apply", action="store_true", help="Save after validation; default only previews")


def read_token(args):
    if args.token_env:
        value = os.environ.get(args.token_env, "")
    elif args.token_file == "-":
        value = sys.stdin.read(65537)
    else:
        fd = os.open(args.token_file, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd) as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise GitProviderError("token file must be an owner-only regular file owned by this user")
            value = stream.read(65537)
    value = value.strip()
    if not value or len(value) > 65536 or "\n" in value or "\r" in value:
        raise GitProviderError("token source must contain one nonempty bounded token")
    return value


def validate_identity(config):
    provider = get_provider(config)
    identity = provider._request("GET", "user")
    if str(identity.get("id")) != config.service_account_id:
        raise GitProviderError("authenticated Git identity does not match service_account_id")
    if config.provider == "github":
        repository = provider._request("GET", f"repos/{config.project}")
        can_push = repository.get("permissions", {}).get("push") is True
    else:
        repository = provider._request("GET", f"projects/{provider.project_path}")
        permissions = repository.get("permissions", {})
        can_push = max((permissions.get(key) or {}).get("access_level", 0) for key in ("project_access", "group_access")) >= 30
    if not can_push:
        raise GitProviderError("authenticated Git identity lacks repository push permission")


def provision(args):
    extra = {name: getattr(args, name) for name in ("provider", "project", "api_base_url", "clone_url", "base_branch", "service_account_id", "max_changed_files", "max_diff_bytes")}
    extra.update(allowed_path_globs=args.allow_path, denied_path_globs=args.deny_path)
    candidate = Connection(conn_id=args.connection_id, conn_type="generic", password=read_token(args), extra=json.dumps(extra))
    if not candidate.is_encrypted:
        raise GitProviderError("configure Airflow Fernet encryption before provisioning Git credentials")
    config = repository_config_from_connection(candidate)
    validate_identity(config)
    if args.apply:
        with create_session() as session:
            existing = session.scalar(select(Connection).where(Connection.conn_id == args.connection_id).with_for_update())
            if existing is None:
                session.add(candidate)
            else:
                existing.password = candidate.password
                existing.extra = candidate.extra
                existing.conn_type = "generic"
    return {"status": "saved" if args.apply else "validated", "connection_id": args.connection_id, "provider": config.provider, "project": config.project, "base_branch": config.base_branch, "service_account_id": config.service_account_id, "encrypted": True}
