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

: "${BOT_DASHBOARD_API_USERNAME:=bot-worker}"
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

if ! grep -q '^BOT_DASHBOARD_API_PASSWORD=' "$SECRETS"; then
    printf 'BOT_DASHBOARD_API_PASSWORD=%s\n' "$(openssl rand -hex 32)" >> "$SECRETS"
    chmod 600 "$SECRETS"
fi
BOT_DASHBOARD_API_PASSWORD=$(
    sed -n 's/^BOT_DASHBOARD_API_PASSWORD=//p' "$SECRETS"
)
[[ -n "$BOT_DASHBOARD_API_PASSWORD" ]] || {
    echo "cannot read bot dashboard API password from $SECRETS" >&2
    exit 1
}

PASSWORD_FILE=airflow_home/simple_auth_manager_passwords.json.generated
mkdir -p airflow_home
BOT_DASHBOARD_API_USERNAME="$BOT_DASHBOARD_API_USERNAME" \
BOT_DASHBOARD_API_PASSWORD="$BOT_DASHBOARD_API_PASSWORD" \
PASSWORD_FILE="$PASSWORD_FILE" python3 - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ["PASSWORD_FILE"])
try:
    passwords = json.loads(path.read_text()) if path.is_file() else {}
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"cannot merge {path}: {exc}") from exc
if not isinstance(passwords, dict):
    raise SystemExit(f"cannot merge {path}: root is not an object")
passwords[os.environ["BOT_DASHBOARD_API_USERNAME"]] = os.environ[
    "BOT_DASHBOARD_API_PASSWORD"
]
temporary = path.with_suffix(path.suffix + ".tmp")
temporary.write_text(json.dumps(passwords, sort_keys=True) + "\n")
temporary.chmod(0o600)
temporary.replace(path)
PY

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
