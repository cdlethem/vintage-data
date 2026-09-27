"""Admit bounded public reconciliation and trusted workflow validation recipes."""
from alembic import op
import sqlalchemy as sa

revision = "0008_public_reconciliation"
down_revision = "0007_validation_execution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "bot_dashboard_validation_gate"
    constraints = sa.inspect(op.get_bind()).get_check_constraints(table)
    for constraint in constraints:
        text = constraint["sqltext"]
        if "public_source_smoke" in text and "disposable_schema_migration" in text and (
                "public_source_reconciliation" not in text or "trusted_workflow_check" not in text):
            with op.batch_alter_table(table) as batch:
                batch.drop_constraint(op.f(constraint["name"]), type_="check")
                batch.create_check_constraint(
                    op.f(constraint["name"]),
                    "recipe in ('public_source_smoke','public_source_reconciliation','trusted_workflow_check',"
                    "'disposable_schema_migration','warehouse_check','dag_inspection','lightdash_preview','manual')",
                )


def downgrade() -> None:
    # Forward-only: preserve reconciliation gates and their evidence.
    pass
