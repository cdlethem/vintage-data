"""Airflow external DB manager for provider-owned tables."""
from pathlib import Path

from airflow.utils.db_manager import BaseDBManager, command

from .models import metadata


class BotDashboardDBManager(BaseDBManager):
    metadata = metadata
    _root = Path(__file__).resolve().parent
    migration_dir = str(_root / "migrations")
    alembic_file = str(_root / "alembic.ini")
    version_table_name = "bot_dashboard_schema_version"
    supports_table_dropping = False

    def create_db_from_orm(self):
        """Always run migrations so immutable-history triggers and seeds exist."""
        self.log.info("Creating %s tables from migrations", self.__class__.__name__)
        command.upgrade(self.get_alembic_config(), "head")
