"""Add typed, stage-specific validation gates.

Revision ID: 0006_validation_gates
Revises: 0005_revision_identity
"""
from alembic import op

from airflow.providers.vintage.bot_dashboard.models import ValidationGate

revision = "0006_validation_gates"
down_revision = "0005_revision_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Revision 0001 intentionally creates the current ORM metadata for fresh
    # installations. checkfirst keeps a fresh 0001 -> head replay idempotent,
    # while existing pre-0006 databases still receive the new table here.
    ValidationGate.__table__.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    # Forward-only: validation evidence is audit history.
    pass
