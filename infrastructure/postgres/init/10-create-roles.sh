#!/usr/bin/env bash
# First-initialization script (runs only when the data directory is empty).
#
# Creates least-privilege roles and the application schema:
#   TRADING_OWNER_ROLE  owns the schema; used for schema migrations (DDL)
#   TRADING_APP_ROLE    application runtime role; data access only (no DDL)
# Tables are created later by schema migrations run as the owner role; default privileges
# grant the application role DML on them automatically.
set -euo pipefail

read_secret() {
  local file="/run/secrets/$1"
  if [[ ! -s "$file" ]]; then
    echo "init: required secret file $file is missing or empty" >&2
    exit 1
  fi
  tr -d '\r\n' < "$file"
}

owner_role="${TRADING_OWNER_ROLE:?TRADING_OWNER_ROLE is required}"
app_role="${TRADING_APP_ROLE:?TRADING_APP_ROLE is required}"
schema="${TRADING_SCHEMA:?TRADING_SCHEMA is required}"

psql --no-psqlrc -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v db="$POSTGRES_DB" \
  -v schema="$schema" \
  -v owner_role="$owner_role" \
  -v app_role="$app_role" \
  -v owner_pw="$(read_secret trading_owner_password)" \
  -v app_pw="$(read_secret trading_app_password)" <<'SQL'
CREATE ROLE :"owner_role" LOGIN PASSWORD :'owner_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE :"app_role" LOGIN PASSWORD :'app_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

-- Nobody gets implicit access to the database or the public schema.
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO :"owner_role", :"app_role";
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- Application schema owned by the migration role.
CREATE SCHEMA :"schema" AUTHORIZATION :"owner_role";
REVOKE ALL ON SCHEMA :"schema" FROM PUBLIC;
GRANT USAGE ON SCHEMA :"schema" TO :"app_role";

-- Objects the owner creates later are automatically usable (DML only) by the application role.
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA :"schema"
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"app_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA :"schema"
  GRANT USAGE, SELECT ON SEQUENCES TO :"app_role";

ALTER ROLE :"owner_role" SET search_path = :"schema";
ALTER ROLE :"app_role" SET search_path = :"schema";
SQL

echo "init: created roles '$owner_role' (schema owner) and '$app_role' (DML only) and schema '$schema'"
