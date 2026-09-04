#!/usr/bin/env bash
# Render every path- and host-bound file from orchestration/config.env.
#
#   setup/render_config.sh          # write airflow.env + generated/systemd/
#   setup/render_config.sh --check  # fail if anything would change (CI/no-op check)
#
# Inputs : orchestration/config.env            (gitignored, machine-local)
# Outputs: orchestration/airflow.env           (gitignored, generated)
#          orchestration/generated/systemd/*   (gitignored, generated)
#
# Templates carry @TOKEN@ placeholders; every token must be defined by
# config.env or computed here, and a leftover token is a hard error rather
# than a half-rendered unit file.
set -euo pipefail
cd "$(dirname "$0")/.."

CHECK=0
[[ "${1:-}" == "--check" ]] && CHECK=1

if [[ ! -f config.env ]]; then
    echo "orchestration/config.env is missing. Create it once with:" >&2
    echo "    cp orchestration/config.example.env orchestration/config.env" >&2
    exit 1
fi

# PROJECT_ROOT is derived, never configured: the checkout location is a fact.
PROJECT_ROOT="$(cd .. && pwd)"
export PROJECT_ROOT

set -a
# shellcheck disable=SC1091
source config.env
set +a

# Defaults for anything config.example.env gained after a config.env was written,
# so an older config.env keeps rendering instead of failing on a new token.
: "${SERVICE_USER:=$(id -un)}"
: "${SERVICE_GROUP:=$(id -gn)}"
: "${SERVICE_PATH:=$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin}"
: "${EXTRACT_SINK:=local}"
: "${EXTRACT_USER_AGENT:=vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)}"
: "${EXTRACT_DATA_ROOT:=$HOME/.local/share/vintage-data/extract}"
: "${EXTRACT_WAREHOUSE:=$HOME/.local/share/vintage-data/warehouse/extract.duckdb}"
: "${EXTRACT_LOAD_QUEUE:=$EXTRACT_DATA_ROOT/_load_queue}"
: "${EXTRACT_WAREHOUSE_THREADS:=4}"
: "${EXTRACT_WAREHOUSE_MEMORY_LIMIT:=4GB}"
: "${AIRFLOW_API_HOST:=127.0.0.1}"
: "${AIRFLOW_API_PORT:=8082}"
: "${AIRFLOW_API_BASE_URL:=http://localhost:$AIRFLOW_API_PORT}"
: "${AIRFLOW_ADMIN_USERS:=admin:admin}"
: "${AIRFLOW_WORKERS:=2}"
: "${AIRFLOW_WORKER_CONCURRENCY:=4}"
: "${CELERY_BROKER_URL:=redis://localhost:6379/0}"
: "${BOTS_MODELS_CONFIG:=$PROJECT_ROOT/bots/models.yml}"

TOKENS=(
    PROJECT_ROOT SERVICE_USER SERVICE_GROUP SERVICE_PATH
    EXTRACT_SINK EXTRACT_USER_AGENT
    EXTRACT_DATA_ROOT EXTRACT_WAREHOUSE EXTRACT_LOAD_QUEUE
    EXTRACT_WAREHOUSE_THREADS EXTRACT_WAREHOUSE_MEMORY_LIMIT
    AIRFLOW_API_HOST AIRFLOW_API_PORT AIRFLOW_API_BASE_URL
    AIRFLOW_ADMIN_USERS AIRFLOW_WORKER_CONCURRENCY CELERY_BROKER_URL
    BOTS_MODELS_CONFIG
)

SED_ARGS=()
for token in "${TOKENS[@]}"; do
    value="${!token-}"
    case "$value" in
        # '|' breaks the sed expressions below; '"' breaks the quoting that
        # lets airflow.env carry values containing spaces.
        *"|"*) echo "config value for $token may not contain '|': $value" >&2; exit 1 ;;
        *'"'*) echo "config value for $token may not contain a double quote: $value" >&2; exit 1 ;;
    esac
    SED_ARGS+=(-e "s|@${token}@|${value}|g")
done

changed=0

render() {
    local src=$1 dst=$2
    local tmp
    tmp="$(mktemp)"
    sed "${SED_ARGS[@]}" "$src" > "$tmp"
    if grep -o '@[A-Z][A-Z0-9_]*@' "$tmp" | sort -u | grep .; then
        rm -f "$tmp"
        echo "^ unresolved template tokens in $src" >&2
        exit 1
    fi
    if [[ -f "$dst" ]] && cmp -s "$tmp" "$dst"; then
        rm -f "$tmp"
        return 0
    fi
    changed=1
    if ((CHECK)); then
        rm -f "$tmp"
        echo "would change: $dst" >&2
        return 0
    fi
    mkdir -p "$(dirname "$dst")"
    install -m 0644 "$tmp" "$dst"
    rm -f "$tmp"
    echo "rendered $dst"
}

render airflow.env.template airflow.env

for template in systemd/*.template; do
    render "$template" "generated/systemd/$(basename "$template" .template)"
done

if ((CHECK)); then
    ((changed)) && exit 1
    echo "generated files are up to date"
fi
