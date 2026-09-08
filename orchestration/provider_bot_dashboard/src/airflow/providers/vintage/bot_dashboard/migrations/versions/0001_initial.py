"""Initial bot dashboard domain schema.

Revision ID: 0001_bot_dashboard
Revises:
Create Date: 2026-09-07
"""
from alembic import op
from sqlalchemy import select

from airflow.providers.vintage.bot_dashboard.models import Policy, metadata

revision = "0001_bot_dashboard"
down_revision = None
branch_labels = None
depends_on = None


def _immutability_triggers(dialect: str) -> None:
    if dialect == "postgresql":
        op.execute("""
        CREATE OR REPLACE FUNCTION bot_dashboard_reject_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable bot dashboard history'; END $$
        """)
        for table in ("bot_dashboard_revision", "bot_dashboard_event"):
            op.execute(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION bot_dashboard_reject_mutation()")
    elif dialect == "sqlite":
        for table in ("bot_dashboard_revision", "bot_dashboard_event"):
            op.execute(f"CREATE TRIGGER {table}_immutable_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT, 'immutable bot dashboard history'); END")
            op.execute(f"CREATE TRIGGER {table}_immutable_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT, 'immutable bot dashboard history'); END")


def upgrade() -> None:
    bind = op.get_bind()
    metadata.create_all(bind=bind)
    _immutability_triggers(bind.dialect.name)
    if bind.execute(select(Policy).where(Policy.category == "*")).scalar_one_or_none() is None:
        bind.execute(Policy.__table__.insert().values(category="*", mode="manual", reviewer_required=True))


def downgrade() -> None:
    # Forward-only. Disabling/uninstalling the provider preserves the work queue.
    pass
