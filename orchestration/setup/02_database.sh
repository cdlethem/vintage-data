#!/usr/bin/env bash
# Create the Airflow metadata role + database, and generate airflow.secrets.env
# once. Idempotent: an existing role/db/secrets file is left alone on rerun.
#
# Role, database, host and port come from config.env (POSTGRES_*). To use a
# managed Postgres instead, create the role and database there, then write
# airflow.secrets.env by hand from airflow.secrets.env.example and skip this
# step — nothing else in setup reads POSTGRES_*.
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[[ -f config.env ]] && { set -a; source config.env; set +a; }
: "${POSTGRES_HOST:=localhost}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=airflow}"
: "${POSTGRES_ROLE:=airflow}"

SECRETS=airflow.secrets.env

if [[ -f "$SECRETS" ]]; then
    echo "$SECRETS already exists; leaving it untouched"
    PGPASS=$(sed -n "s|^AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=.*://${POSTGRES_ROLE}:\([^@]*\)@.*|\1|p" "$SECRETS")
    [[ -n "$PGPASS" ]] || { echo "cannot read the $POSTGRES_ROLE password out of $SECRETS" >&2; exit 1; }
else
    PGPASS=$(openssl rand -hex 16)
    cat > "$SECRETS" <<EOF
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${POSTGRES_ROLE}:${PGPASS}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
AIRFLOW__API_AUTH__JWT_SECRET=$(openssl rand -hex 32)
EOF
    chmod 600 "$SECRETS"
    echo "wrote $SECRETS"
fi

if [[ "$POSTGRES_HOST" != localhost && "$POSTGRES_HOST" != 127.0.0.1 ]]; then
    echo "POSTGRES_HOST=$POSTGRES_HOST is not local; create the role and database there yourself"
    exit 0
fi

if sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${POSTGRES_ROLE}'" | grep -q 1; then
    echo "role ${POSTGRES_ROLE} exists"
else
    sudo -u postgres psql -c "CREATE USER ${POSTGRES_ROLE} PASSWORD '${PGPASS}'"
fi

if sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
    echo "database ${POSTGRES_DB} exists"
else
    sudo -u postgres psql -c "CREATE DATABASE ${POSTGRES_DB} OWNER ${POSTGRES_ROLE}"
fi
