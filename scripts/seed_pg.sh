#!/usr/bin/env bash
# Run ddl.sql + dml.sql against Postgres manually.
# Only needed if the dimension was not auto-seeded on container init
# (e.g. the postgres volume already existed on first start).
set -euo pipefail

PG_DB="${PG_DB:-agentsdb}"
PG_USER="${PG_USER:-postgres}"

docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" < sql/ddl.sql
docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" < sql/dml.sql

echo "--- agent_types ---"
docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -c "SELECT * FROM agent_types ORDER BY id;"
