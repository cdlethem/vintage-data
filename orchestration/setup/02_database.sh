#!/usr/bin/env bash
# Create the airflow role + database, and generate airflow.secrets.env once.
# Idempotent: existing role/db/secrets are left alone on rerun.
set -euo pipefail
cd "$(dirname "$0")/.."

SECRETS=airflow.secrets.env

if [[ -f "$SECRETS" ]]; then
    echo "$SECRETS already exists; leaving it untouched"
    PGPASS=$(grep -oP '(?<=airflow:)[^@]+' "$SECRETS")
else
    PGPASS=$(openssl rand -hex 16)
    cat > "$SECRETS" <<EOF
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${PGPASS}@localhost:5432/airflow
AIRFLOW__API_AUTH__JWT_SECRET=$(openssl rand -hex 32)
EOF
    chmod 600 "$SECRETS"
    echo "wrote $SECRETS"
fi

if sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='airflow'" | grep -q 1; then
    echo "role airflow exists"
else
    sudo -u postgres psql -c "CREATE USER airflow PASSWORD '${PGPASS}'"
fi

if sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1; then
    echo "database airflow exists"
else
    sudo -u postgres psql -c "CREATE DATABASE airflow OWNER airflow"
fi
