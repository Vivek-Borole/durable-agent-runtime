#!/bin/sh
set -eu
: "${POSTGRES_URL:?POSTGRES_URL is required}"
attempt=0
until psql "$POSTGRES_URL" -Atc 'select 1' >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 200 ]; then
    echo "PostgreSQL did not become ready within 10 minutes" >&2
    exit 1
  fi
  sleep 3
done
psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -c 'create table if not exists schema_migrations (name text primary key, applied_at timestamptz not null default now())'
for file in /migrations/*.sql; do
  name=$(basename "$file")
  applied=$(psql "$POSTGRES_URL" -Atc "select 1 from schema_migrations where name = '$name'")
  if [ "$applied" != "1" ]; then
    psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -f "$file"
    psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -c "insert into schema_migrations (name) values ('$name')"
  fi
done
