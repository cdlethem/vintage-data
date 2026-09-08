from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from airflow.providers.vintage.bot_dashboard.models import metadata

config = context.config
target_metadata = metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"), target_metadata=target_metadata,
        literal_binds=True, dialect_opts={"paramstyle": "named"},
        version_table="bot_dashboard_schema_version",
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = config.attributes.get("connection")
    owned = connectable is None
    if owned:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section, {}), prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )
    if hasattr(connectable, "connect"):
        connection = connectable.connect()
    else:
        connection = connectable
    try:
        context.configure(
            connection=connection, target_metadata=target_metadata,
            version_table="bot_dashboard_schema_version",
        )
        with context.begin_transaction():
            context.run_migrations()
    finally:
        if owned:
            connection.close()
            connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
