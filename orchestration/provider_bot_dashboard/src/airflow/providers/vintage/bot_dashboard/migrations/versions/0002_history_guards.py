"""Install immutable-history guards and default policy for ORM-stamped databases.

Revision ID: 0002_history_guards
Revises: 0001_bot_dashboard
"""
from alembic import op
from sqlalchemy import text

revision = "0002_history_guards"
down_revision = "0001_bot_dashboard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("""
        CREATE OR REPLACE FUNCTION bot_dashboard_reject_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable bot dashboard history'; END $$
        """)
        for table in ("bot_dashboard_revision", "bot_dashboard_event"):
            op.execute(f"DROP TRIGGER IF EXISTS {table}_immutable ON {table}")
            op.execute(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION bot_dashboard_reject_mutation()")
    elif bind.dialect.name == "sqlite":
        for table in ("bot_dashboard_revision", "bot_dashboard_event"):
            for operation in ("update", "delete"):
                op.execute(f"DROP TRIGGER IF EXISTS {table}_immutable_{operation}")
                op.execute(f"CREATE TRIGGER {table}_immutable_{operation} BEFORE {operation.upper()} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable bot dashboard history'); END")
    op.execute("""
        INSERT INTO bot_dashboard_policy
            (category, mode, reviewer_required, updated_at)
        SELECT '*', 'manual', true, CURRENT_TIMESTAMP
        WHERE NOT EXISTS (
            SELECT 1 FROM bot_dashboard_policy WHERE category = '*'
        )
    """)


def downgrade() -> None:
    pass
