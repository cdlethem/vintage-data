#!/usr/bin/env bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=app_password="$LIGHTDASH_APP_PASSWORD" \
  --set=publisher_password="$LIGHTDASH_PUBLISHER_PASSWORD" \
  --set=reader_password="$LIGHTDASH_READER_PASSWORD" \
  --set=serving_db="${LIGHTDASH_SERVING_DB:-vintage_serving}" <<'SQL'
CREATE ROLE lightdash_app LOGIN PASSWORD :'app_password';
CREATE ROLE mart_publisher LOGIN PASSWORD :'publisher_password';
CREATE ROLE mart_reader LOGIN PASSWORD :'reader_password';
CREATE DATABASE lightdash OWNER lightdash_app;
CREATE DATABASE :"serving_db" OWNER mart_publisher;
REVOKE ALL ON DATABASE lightdash FROM PUBLIC;
REVOKE ALL ON DATABASE :"serving_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"serving_db" TO mart_reader;
\connect :serving_db
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA transform_marts AUTHORIZATION mart_publisher;
CREATE SCHEMA _publish AUTHORIZATION mart_publisher;
GRANT USAGE ON SCHEMA transform_marts, _publish TO mart_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE mart_publisher IN SCHEMA transform_marts GRANT SELECT ON TABLES TO mart_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE mart_publisher IN SCHEMA _publish GRANT SELECT ON TABLES TO mart_reader;
ALTER ROLE mart_reader SET default_transaction_read_only = on;
ALTER ROLE mart_reader SET statement_timeout = '60s';
SQL
