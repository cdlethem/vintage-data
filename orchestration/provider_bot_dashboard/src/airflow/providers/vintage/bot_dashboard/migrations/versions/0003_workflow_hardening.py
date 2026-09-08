"""Add typed run, budget, specialist, and artifact lifecycle state.

Revision ID: 0003_workflow_hardening
Revises: 0002_history_guards
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.elements import conv

from airflow.providers.vintage.bot_dashboard.models import Artifact, LegacyImport, RunBudgetClaim

revision = "0003_workflow_hardening"
down_revision = "0002_history_guards"
branch_labels = None
depends_on = None


def _columns(table: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}


def _add_missing(table: str, columns: list[sa.Column]) -> None:
    present = _columns(table)
    bind = op.get_bind()
    for column in columns:
        if column.name in present:
            continue
        if bind.dialect.name == "sqlite" and (
            column.foreign_keys
            or (
                column.server_default is not None
                and "CURRENT_TIMESTAMP" in str(column.server_default.arg)
            )
        ):
            column = sa.Column(
                column.name,
                column.type,
                nullable=True,
                server_default=None,
            )
        op.add_column(table, column)

def _replace_source_constraint() -> None:
    bind = op.get_bind()
    checks = sa.inspect(bind).get_check_constraints("bot_dashboard_task")
    source = next((item for item in checks if item.get("name") == "ck_bot_dashboard_task_source"), None)
    if source is None or "specialist" in source.get("sqltext", ""):
        return
    # conv() keeps these literal: the metadata naming convention would otherwise
    # re-expand an already-final constraint name.
    name = conv("ck_bot_dashboard_task_source")
    definition = "source in ('manager','manual','specialist')"
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("bot_dashboard_task") as batch:
            batch.drop_constraint(name, type_="check")
            batch.create_check_constraint(name, definition)
    else:
        op.drop_constraint(name, "bot_dashboard_task", type_="check")
        op.create_check_constraint(name, "bot_dashboard_task", definition)


def _replace_report_identity() -> None:
    table = "bot_dashboard_run_report"
    uniques = sa.inspect(op.get_bind()).get_unique_constraints(table)
    wanted = {"dag_id", "run_id", "task_id", "map_index", "try_number"}
    if any(set(item.get("column_names") or []) == wanted for item in uniques):
        return
    old = next(
        (
            item
            for item in uniques
            if set(item.get("column_names") or [])
            == {"dag_id", "run_id", "task_id", "map_index"}
        ),
        None,
    )
    with op.batch_alter_table(table) as batch:
        if old and old.get("name"):
            batch.drop_constraint(conv(old["name"]), type_="unique")
        batch.create_unique_constraint(
            conv("uq_bd_report_task_try"),
            ["dag_id", "run_id", "task_id", "map_index", "try_number"],
        )


def _normalize_json_nulls() -> None:
    """A JSON `null` is not an absent document; presence checks use IS NULL."""
    bind = op.get_bind()
    literal = "'null'" if bind.dialect.name == "sqlite" else "'null'::jsonb"
    cast = "" if bind.dialect.name == "sqlite" else "::jsonb"
    for table, column in (
        ("bot_dashboard_run_report", "body_json"),
        ("bot_dashboard_execution", "verification_manifest"),
        ("bot_dashboard_execution", "provider_state"),
    ):
        op.execute(
            f"UPDATE {table} SET {column} = NULL "
            f"WHERE {column} IS NOT NULL AND {column}{cast} = {literal}"
        )


def upgrade() -> None:
    bind = op.get_bind()
    RunBudgetClaim.__table__.create(bind=bind, checkfirst=True)
    Artifact.__table__.create(bind=bind, checkfirst=True)
    LegacyImport.__table__.create(bind=bind, checkfirst=True)

    _add_missing(
        "bot_dashboard_task",
        [sa.Column("source_bot", sa.String(100), nullable=True)],
    )
    _replace_source_constraint()

    empty_json = sa.text("'[]'")
    _add_missing(
        "bot_dashboard_revision",
        [
            sa.Column("evidence", sa.JSON(), nullable=False, server_default=empty_json),
            sa.Column("verification_commands", sa.JSON(), nullable=False, server_default=empty_json),
            sa.Column("allowed_path_globs", sa.JSON(), nullable=False, server_default=empty_json),
            sa.Column("resource_keys", sa.JSON(), nullable=False, server_default=empty_json),
            sa.Column("follow_up_bots", sa.JSON(), nullable=False, server_default=empty_json),
        ],
    )

    _add_missing(
        "bot_dashboard_execution",
        [
            sa.Column("executor_deadline_at", sa.DateTime(timezone=True)),
            sa.Column("review_deadline_at", sa.DateTime(timezone=True)),
            sa.Column("base_sha", sa.String(64)),
            sa.Column("source_artifact_sha256", sa.String(64)),
            sa.Column("patch_sha256", sa.String(64)),
            sa.Column("patch_byte_count", sa.Integer()),
            sa.Column("verification_manifest", sa.JSON()),
            sa.Column("executor_report_sha256", sa.String(64)),
            sa.Column("review_report_sha256", sa.String(64)),
            sa.Column("review_verdict", sa.String(32)),
            sa.Column("review_comment_fingerprint", sa.String(64)),
            sa.Column("review_commented_at", sa.DateTime(timezone=True)),
            sa.Column("published_at", sa.DateTime(timezone=True)),
            sa.Column("merged_at", sa.DateTime(timezone=True)),
            sa.Column("terminal_reason_code", sa.String(100)),
            sa.Column("terminal_failure_class", sa.String(100)),
            sa.Column("terminal_detail", sa.Text()),
        ],
    )

    _add_missing(
        "bot_dashboard_artifact",
        [
            sa.Column(
                "media_type",
                sa.String(128),
                nullable=False,
                server_default="application/octet-stream",
            ),
        ],
    )
    op.execute(
        """
        UPDATE bot_dashboard_artifact
        SET media_type = CASE kind
            WHEN 'source' THEN 'application/x-tar'
            WHEN 'patch' THEN 'text/x-diff'
            WHEN 'manifest' THEN 'application/json'
            WHEN 'executor_report' THEN 'application/json'
            WHEN 'review_report' THEN 'application/json'
            ELSE 'application/octet-stream'
        END
        WHERE media_type IS NULL OR media_type = 'application/octet-stream'
        """
    )

    _add_missing(
        "bot_dashboard_run_report",
        [
            sa.Column(
                "budget_claim_id",
                sa.Uuid(),
                sa.ForeignKey("bot_dashboard_run_budget_claim.id"),
            ),
        ],
    )
    now = sa.text("CURRENT_TIMESTAMP")
    _add_missing(
        "bot_dashboard_run_report",
        [
            sa.Column("try_number", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("outcome", sa.String(32), nullable=False, server_default="failed"),
            sa.Column("retry_class", sa.String(16), nullable=False, server_default="terminal"),
            sa.Column("reason_code", sa.String(100), nullable=False, server_default="legacy_import"),
            sa.Column("failure_class", sa.String(100)),
            sa.Column("failure_code", sa.String(100)),
            sa.Column("failure_fingerprint", sa.String(64)),
            sa.Column("failure_detail", sa.Text()),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
            sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
            sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("context_sha256", sa.String(64), nullable=False, server_default=""),
            sa.Column("context_byte_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("context_build_ms", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("attempts_json", sa.JSON(), nullable=False, server_default=empty_json),
            sa.Column("input_tokens", sa.Integer()),
            sa.Column("output_tokens", sa.Integer()),
            sa.Column("total_tokens", sa.Integer()),
            sa.Column("provider_duration_ms", sa.Integer()),
            sa.Column("deadline_consumed_ms", sa.Integer()),
        ],
    )
    op.execute(
        """
        UPDATE bot_dashboard_run_report
        SET outcome = CASE status
            WHEN 'ok' THEN 'succeeded'
            WHEN 'skipped' THEN 'skipped'
            WHEN 'busy' THEN 'capacity_unavailable'
            ELSE 'failed' END,
            retry_class = CASE WHEN status IN ('ok','skipped','busy') THEN 'none' ELSE 'terminal' END,
            reason_code = CASE status
            WHEN 'ok' THEN 'legacy_succeeded'
            WHEN 'skipped' THEN 'legacy_skipped'
            WHEN 'busy' THEN 'legacy_capacity'
            ELSE 'legacy_failed' END,
            started_at = created_at,
            finished_at = created_at,
            deadline_at = created_at
        WHERE status IN ('ok', 'skipped', 'busy') OR reason_code = 'legacy_import'
        """
    )
    _replace_report_identity()
    _normalize_json_nulls()


def downgrade() -> None:
    # Forward-only: workflow history and content-addressed artifacts are retained.
    pass
