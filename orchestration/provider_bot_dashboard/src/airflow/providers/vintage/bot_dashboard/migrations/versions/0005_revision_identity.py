"""Allow one specialist report to propose revisions for multiple tickets.

Revision ID: 0005_revision_identity
Revises: 0004_usage_accounting
"""
import sqlalchemy as sa
from alembic import op

revision = "0005_revision_identity"
down_revision = "0004_usage_accounting"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "bot_dashboard_revision"
    name = "uq_bd_revision_source_identity"
    columns = ["task_id", "source_dag_id", "source_run_id", "source_task_id", "source_map_index"]
    existing = next((item for item in sa.inspect(op.get_bind()).get_unique_constraints(table)
                     if item["name"] == name), None)
    if existing and existing["column_names"] == columns:
        return
    with op.batch_alter_table(table) as batch:
        if existing:
            batch.drop_constraint(name, type_="unique")
        batch.create_unique_constraint(name, columns)
    # SQLite rebuilds the table to change a constraint; restore its history guards.
    if op.get_bind().dialect.name == "sqlite":
        for operation in ("update", "delete"):
            op.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_immutable_{operation} BEFORE {operation.upper()} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable bot dashboard history'); END")


def downgrade() -> None:
    # Forward-only: restoring the old constraint would reject valid history.
    pass
