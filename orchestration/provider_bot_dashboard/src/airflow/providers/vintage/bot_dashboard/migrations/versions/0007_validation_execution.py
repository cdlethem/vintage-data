"""Add bounded validation execution leases and recipe parameters."""
from alembic import op
import sqlalchemy as sa

revision = "0007_validation_execution"
down_revision = "0006_validation_gates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "bot_dashboard_validation_gate"
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}
    additions = [
        sa.Column("recipe_args", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("lease_id", sa.String(64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_run_id", sa.String(250), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
    ]
    for column in additions:
        if column.name not in columns:
            op.add_column(table, column)
    for constraint in sa.inspect(op.get_bind()).get_check_constraints(table):
        if "'pending'" in constraint["sqltext"] and "'passed'" in constraint["sqltext"] and "'leased'" not in constraint["sqltext"]:
            with op.batch_alter_table(table) as batch:
                batch.drop_constraint(op.f(constraint["name"]), type_="check")
                batch.create_check_constraint(
                    op.f(constraint["name"]),
                    "status in ('pending','leased','running','passed','failed')",
                )
    indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table)}
    if "ix_bot_dashboard_validation_gate_lease" not in indexes:
        op.create_index("ix_bot_dashboard_validation_gate_lease", table, ["status", "lease_expires_at"])


def downgrade() -> None:
    # Forward-only: preserve validation execution evidence.
    pass
