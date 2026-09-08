"""Add provider-agnostic token and spend accounting.

Revision ID: 0004_usage_accounting
Revises: 0003_workflow_hardening
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_usage_accounting"
down_revision = "0003_workflow_hardening"
branch_labels = None
depends_on = None


def _columns(table: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}


def _add_missing(table: str, columns: list[sa.Column]) -> None:
    present = _columns(table)
    for column in columns:
        if column.name not in present:
            op.add_column(table, column)


def _has_spend_index() -> bool:
    table = "bot_dashboard_run_report"
    wanted = ["bot_name", "created_at"]
    for index in sa.inspect(op.get_bind()).get_indexes(table):
        if list(index.get("column_names") or []) == wanted:
            return True
    return False


def upgrade() -> None:
    # Nullable columns preserve all existing rows.  Defaults make direct SQL
    # inserts safe while callers migrate to explicit usage accounting.
    _add_missing(
        "bot_dashboard_run_report",
        [
            sa.Column("cached_input_tokens", sa.Integer(), nullable=True, server_default="0"),
            sa.Column("cache_write_tokens", sa.Integer(), nullable=True, server_default="0"),
            sa.Column("reasoning_tokens", sa.Integer(), nullable=True, server_default="0"),
            sa.Column("model_requests", sa.Integer(), nullable=True, server_default="0"),
            sa.Column("cost_micro_usd", sa.Integer(), nullable=True),
            sa.Column("cost_source", sa.String(32), nullable=True, server_default="unavailable"),
            sa.Column("pricing_id", sa.String(200), nullable=True),
        ],
    )
    if not _has_spend_index():
        op.create_index(
            "ix_bot_dashboard_report_bot_created",
            "bot_dashboard_run_report",
            ["bot_name", "created_at"],
        )


def downgrade() -> None:
    # Forward-only: usage history is retained.
    pass
