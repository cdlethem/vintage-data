#!/bin/sh
set -eu
minio server /data &
server=$!
trap 'kill "$server"; wait "$server"' TERM INT
attempt=0
until mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 60 ] || exit 1
    sleep 1
done
mc mb --ignore-existing local/lightdash
wait "$server"
